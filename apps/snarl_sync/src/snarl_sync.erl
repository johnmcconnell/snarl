%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  5 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(snarl_sync).

-behaviour(gen_server).

-include_lib("snarl_dtrace/include/snarl_dtrace.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

%% API
-export([start/2, start_link/2, sync_op/7, hash/2, sync/0,
         remote_sync_started/0, enabled/0]).

-ignore_xref([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(CON_OPTS, [binary, {active, false}, {packet, 4}]).

-define(SYNC_IVAL, 1000*60*15).

-record(state, {ip, port, socket, timeout, interval, timer,
                reconnect_timer}).

%%%===================================================================
%%% API
%%%===================================================================

enabled() ->
    case application:get_env(snarl, sync) of
        {ok, on} ->
            true;
        _ ->
            false
    end.
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------

start(IP, Port) ->
    snarl_sync_worker_sup:start_child(IP, Port).

start_link(IP, Port) ->
    L = io_lib:format("snarl_sync@~s:~p", [IP, Port]),
    N = list_to_atom(lists:flatten(L)),
    gen_server:start_link({local, N}, ?MODULE, [IP, Port], []).

cast_all(Msg) ->
    case whereis(snarl_sync_worker_sup) of
        undefined ->
            [];
        Sup ->
            Cs = supervisor:which_children(Sup),
            [gen_server:cast(P, Msg) || {_, P, _, _} <- Cs]
    end.

sync_op(Node, VNode, System, Bucket, User, Op, Val) ->
    cast_all({write, Node, VNode, System, Bucket, User, Op, Val}).

remote_sync_started() ->
    cast_all(remote_sync).

sync() ->
    cast_all(sync).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([IP, Port]) ->
    Timeout = application:get_env(snarl, sync_recv_timeout, 1500),
    IVal = application:get_env(snarl, sync_interval, ?SYNC_IVAL),
    State = #state{ip=IP, port=Port, timeout=Timeout, interval = IVal},
    timer:send_interval(1000, ping),
    case connect(State)  of
        {ok, Socket} ->
            lager:info("[sync] Connected to: ~s:~p.", [IP, Port]),
            {ok, next_tick(State#state{socket=Socket}), 0};
        E ->
            lager:error("[sync] Initialization failed: ~p.", [E]),
            {ok, reconnect(next_tick(State)), 0}
    end.

connect(#state{ip=IP, port=Port, timeout = Timeout}) ->
    gen_tcp:connect(IP, Port, [{send_timeout, Timeout} | ?CON_OPTS], Timeout).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(sync, State) ->
    self() ! sync,
    {noreply, State};
handle_cast(remote_sync, State) ->
    cancle_timer(State),
    lager:warning("[sync] Remote sync started, skipping this sync tick"),
    {noreply, next_tick(State)};
handle_cast({write, _Node, _VNode, _System, _Bucket, _User, _Op, _Val} = Act,
            State = #state{ip = IP, socket = undefined}) ->
    lager:warning("[sync:~s] Skipping ~p", [IP, Act]),
    {noreply, State};

handle_cast({write, Node, VNode, System, Bucket, ID, Op, Val},
            State = #state{socket=Socket}) ->
    SystemS = atom_to_list(System),
    OpS = atom_to_list(Op),
    dyntrace:p(?DT_SYNC_SEND, ?DT_ENTRY, SystemS, ID, OpS),
    Command = {write, Node, VNode, System, Bucket, ID, Op, Val},
    State0 = case gen_tcp:send(Socket, term_to_binary(Command)) of
                 ok ->
                     State;
                 E ->
                     lager:error("[sync] Error: ~p", [E]),
                     dyntrace:p(?DT_SYNC, ?DT_RETURN, ?DT_FAIL, SystemS, ID,
                                OpS),
                     reconnect(State#state{socket=undefined})
             end,
    {noreply, State0};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_info(ping, State = #state{socket = undefined}) ->
    {noreply, State};

handle_info(ping, State = #state{socket = Socket, timeout = Timeout}) ->
    State0 = case gen_tcp:send(Socket, term_to_binary(ping)) of
                 ok ->
                     case gen_tcp:recv(Socket, 0, Timeout) of
                         {error, E} ->
                             lager:error("[sync] Error: ~p", [E]),
                             reconnect(State#state{socket=undefined});
                         {ok, _Pong} ->
                             State
                     end;
                 E ->
                     lager:error("[sync] Error: ~p", [E]),
                     reconnect(State#state{socket=undefined})
             end,
    {noreply, State0};

handle_info(sync, State = #state{socket = undefined}) ->
    cancle_timer(State),
    lager:warning("[sync] Can't syncing not connected."),
    {noreply, next_tick(State)};

handle_info(sync, State) ->
    cancle_timer(State),
    State0 = do_sync(State),
    {noreply, next_tick(State0)};

handle_info(reconnect, State = #state{socket = Old, ip = IP, port = Port}) ->
    maybe_close(Old),
    State1 = State#state{reconnect_timer = undefined},
    case connect(State)  of
        {ok, Socket} ->
            {noreply, State1#state{socket = Socket}};
        E ->
            lager:error("[sync(~p:~p)] Initialization failed: ~p.",
                        [IP, Port, E]),
            {noreply, reconnect(State1#state{socket = undefined})}
    end;


handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_sync(State = #state{socket = Socket}) ->
    ok = gen_tcp:send(Socket, term_to_binary(get_tree)),
    {ok, Tree} = snarl_sync_tree:get_tree(),
    walk_tree(Tree, State).

-spec sync_fun(gen_tcp:socket(), pos_integer()) -> merklet:serial_fun().
sync_fun(Socket, Timeout) ->
    fun(Cmd, Path) ->
            ok = gen_tcp:send(Socket, term_to_binary({merklet, Cmd, Path})),
            {ok, Reply} = gen_tcp:recv(Socket, 0, Timeout),
            Reply
    end.
walk_tree(Tree, State = #state{ip = IP, port = Port,
                               timeout = Timeout, socket = Socket}) ->
    lager:debug("[merkelt:~p] Starting remote tree walk.", [IP]),
    Fun = merklet:access_unserialize(sync_fun(Socket, Timeout)),
    Diff = merklet:dist_diff(Tree, Fun),
    lager:debug("[merkelt:~p] Tree walk finished.", [IP]),
    ok = gen_tcp:send(Socket, term_to_binary(done)),
    Diff1 = [binary_to_term(D) || D <- Diff],
    lager:debug("[sync] We need to diff: ~p", [Diff1]),
    snarl_sync_exchange_fsm:start(IP, Port, Diff1),
    State.


maybe_close(undefined) ->
    ok;

maybe_close(Old) ->
    gen_tcp:close(Old).

hash(BKey, Obj) ->
    Data = case ft_obj:is_a(Obj) of
               true ->
                   lists:sort(ft_obj:vclock(Obj));
               _ ->
                   Obj
           end,
    integer_to_binary(erlang:phash2({BKey, Data})).

next_tick(State = #state{interval = IVal}) ->
    Wait = rand:uniform(IVal) + IVal,
    T1 = erlang:send_after(Wait, self(), sync),
    State#state{timer = T1}.

cancle_timer(#state{timer = undefiend}) ->
    ok;
cancle_timer(#state{timer = T}) ->
    erlang:cancel_timer(T).

reconnect(State = #state{reconnect_timer = undefined}) ->
    Wait = rand:uniform(2000) + 500,
    lager:warning("[sync] Reconnecting in ~pms", [Wait]),
    T1 = erlang:send_after(Wait, self(), reconnect),
    State#state{reconnect_timer = T1};
reconnect(State) ->
    State.

