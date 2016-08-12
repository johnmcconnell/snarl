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
-export([start/2, start_link/2, sync_op/7, hash/2, split_trees/2, sync/0,
         remote_sync_started/0]).

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
    gen_server:start_link({local, ?SERVER}, ?MODULE, [IP, Port], []).

sync_op(Node, VNode, System, Bucket, User, Op, Val) ->
    gen_server:abcast(?SERVER,
                      {write, Node, VNode, System, Bucket, User, Op, Val}).

remote_sync_started() ->
    gen_server:abcast(?SERVER, remote_sync).

sync() ->
    gen_server:abcast(?SERVER, sync).

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
    case gen_tcp:connect(IP, Port, [{send_timeout, State#state.timeout} |
                                    ?CON_OPTS], Timeout) of
        {ok, Socket} ->
            lager:info("[sync] Connected to: ~s:~p.", [IP, Port]),
            {ok, next_tick(State#state{socket=Socket}), 0};
        E ->
            lager:error("[sync] Initialization failed: ~p.", [E]),
            {ok, reconnect(next_tick(State)), 0}
    end.

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
            State = #state{socket=undefined}) ->
    lager:debug("[sync] ~p", [Act]),
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

handle_info(sync, State = #state{socket = Socket, timeout = Timeout}) ->
    cancle_timer(State),
    State0 = case gen_tcp:send(Socket, term_to_binary(get_tree)) of
                 ok ->
                     case gen_tcp:recv(Socket, 0, Timeout) of
                         {error, E} ->
                             lager:error("[sync] Error: ~p", [E]),
                             reconnect(State#state{socket=undefined});
                         {ok, Bin} ->
                             {ok, LTree} = snarl_sync_tree:get_tree(),
                             {ok, RTree} = binary_to_term(Bin),
                             sync_trees(LTree, RTree, State)
                     end;
                 E ->
                     lager:error("[sync] Error: ~p", [E]),
                     reconnect(State#state{socket=undefined})
             end,
    {noreply, next_tick(State0)};

handle_info(reconnect, State = #state{socket = Old, ip=IP, port=Port,
                                      timeout = Timeout}) ->
    maybe_close(Old),
    State1 = State#state{reconnect_timer = undefined},
    case gen_tcp:connect(IP, Port, [{send_timeout, State#state.timeout} |
                                    ?CON_OPTS], Timeout) of
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

sync_trees(LTree, RTree, State = #state{ip=IP, port=Port}) ->
    Diff = split_trees(LTree, RTree),
    lager:debug("[sync] We need to diff: ~p", [Diff]),
    snarl_sync_exchange_fsm:start(IP, Port, Diff),
    State.

split_trees(L, R) ->
    split_trees(lists:sort(L), lists:sort(R), []).

split_trees([K | LR], [K | RR], Diff) ->
    split_trees(LR, RR, Diff);

split_trees([{K, _} | LR], [{K, _} | RR], Diff) ->
    split_trees(LR, RR, [K | Diff]);

split_trees([{K, _} = L | LR], [_Kr | _] = R, Diff) when L < _Kr ->
    split_trees(LR, R, [K | Diff]);

split_trees(L, [{K, _} | RR], Diff) ->
    split_trees(L, RR, [K | Diff]);

split_trees(L, [], Diff) ->
    Diff ++ [K || {K, _} <- L];

split_trees([], R, Diff) ->
    Diff ++ [K || {K, _} <- R].

next_tick(State = #state{interval = IVal}) ->
    Wait = random:uniform(IVal) + IVal,
    T1 = erlang:send_after(Wait, self(), sync),
    State#state{timer = T1}.

cancle_timer(#state{timer = undefiend}) ->
    ok;
cancle_timer(#state{timer = T}) ->
    erlang:cancel_timer(T).

reconnect(State = #state{reconnect_timer = undefined}) ->
    Wait = random:uniform(2000) + 500,
    lager:warning("[sync] Reconnecting in ~pms", [Wait]),
    T1 = erlang:send_after(Wait, self(), reconnect),
    State#state{reconnect_timer = T1};
reconnect(State) ->
    State.
