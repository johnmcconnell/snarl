-module(snarl_sync_protocol).

-behaviour(ranch_protocol).

-export([start_link/4, init/4]).

%-ignore_xref([start_link/4]).

-record(state, {socket, transport,
                %% Timeout for a connection is 30s w/o a message
                timeout = 30000}).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, false}, {packet, 4}]),
    loop(#state{socket = Socket, transport = Transport}).

loop(State = #state{transport = Transport, socket = Socket,
                    timeout = Timeout}) ->
    case Transport:recv(Socket, 0, Timeout) of
        {ok, BinData} ->
            case binary_to_term(BinData) of
                ping ->
                    Transport:send(Socket, term_to_binary(pong));
                get_tree ->
                    snarl_sync:remote_sync_started(),
                    {ok, Tree} = snarl_sync_tree:get_tree(),
                    Transport:send(Socket, term_to_binary({ok, Tree}));
                {raw, System, Realm, UUID} ->
                    Data = snarl_sync_element:raw(System, Realm, UUID),
                    Transport:send(Socket, term_to_binary(Data));
                {repair, System, Realm, UUID, Obj} ->
                    snarl_sync_element:repair(System, Realm, UUID, Obj);
                {delete, System, Realm, UUID} ->
                    snarl_sync_element:delete(System, Realm, UUID);
                {write, Node, VNode, System, Realm, UUID, Op, Val} ->
                    NVS = {{remote, Node}, VNode, System},
                    snarl_entity_write_fsm:write(NVS, {Realm, UUID}, Op, Val)
            end,
            loop(State);
        E ->
            lager:warning("[sync] Closing connection with error: ~p", [E]),
            Transport:close(Socket)
    end.
