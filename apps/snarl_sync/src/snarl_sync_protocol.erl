-module(snarl_sync_protocol).

-behaviour(ranch_protocol).

-export([start_link/4, init/4]).

%-ignore_xref([start_link/4]).

-record(state, {socket, transport,
                %% Timeout for a connection is 30s w/o a message
                timeout = 30000,
                tree_fun :: merklet:serial_fun() | undefined}).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, false}, {packet, 4}]),
    loop(#state{socket = Socket, transport = Transport}).

loop(State = #state{transport = Transport, socket = Socket,
                    timeout = Timeout, tree_fun = undefined}) ->
    case Transport:recv(Socket, 0, Timeout) of
        {ok, BinData} ->
            State1 = case binary_to_term(BinData) of
                         ping ->
                             Transport:send(Socket, term_to_binary(pong)),
                             State;
                         get_tree ->
                             snarl_sync:remote_sync_started(),
                             {ok, Tree} = snarl_sync_tree:get_tree(),
                             Fun = merklet:access_unserialize(Tree),
                             State#state{tree_fun = Fun};
                         {raw, System, Realm, UUID} ->
                             Data = snarl_sync_element:raw(System, Realm, UUID),
                             Transport:send(Socket, term_to_binary(Data)),
                             State;
                         {repair, System, Realm, UUID, Obj} ->
                             snarl_sync_element:repair(
                               System, Realm, UUID, Obj),
                             State;
                         {delete, System, Realm, UUID} ->
                             snarl_sync_element:delete(System, Realm, UUID),
                             State;
                         {write, Node, VNode, System, Realm, UUID, Op, Val} ->
                             NVS = {{remote, Node}, VNode, System},
                             snarl_entity_write_fsm:write(
                               NVS, {Realm, UUID}, Op, Val),
                             State
                     end,
            loop(State1);
        E ->
            lager:warning("[sync] Closing connection with error: ~p", [E]),
            Transport:close(Socket)
    end;

loop(State = #state{transport = Transport, socket = Socket,
                    timeout = Timeout, tree_fun = Fun}) ->
    case Transport:recv(Socket, 0, Timeout) of
        {ok, BinData} ->
            State1 =
                case binary_to_term(BinData) of
                    done ->
                        State#state{tree_fun = undefined};
                    {merklet, Cmd, Path} ->
                        Data = Fun(Cmd, Path),
                        Transport:send(Socket, term_to_binary(Data)),
                        State
                end,
            loop(State1);
        E ->
            lager:warning("[sync] Closing connection with error: ~p", [E]),
            Transport:close(Socket)
    end.
