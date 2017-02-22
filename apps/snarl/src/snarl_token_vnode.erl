%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2015, Heinz Nikolaus Gies
%%% @doc
%%% The vnode keeps two databases, one for API tokesn one for the rest
%%% the reasonb is that those two have very differnt characteristics.
%%% While API tokens are long lived (forever until revoked) OAuth
%%% tokens often exist only for a very short time and thus might never
%%% get removd from a database unless the datbase has a timeout.
%%% So unless there are two seperated databases either API tokens WILL
%%% timeout after the database timeout OR we run into the risk of
%%% (already invalid) OAuth tokens keeping to gather in the database
%%% growing it forever.
%%%
%%% An alternative solution to two databases would be manually
%%% expiering keys from the disk DB but that sounds rather silly
%%% since the it would mean periodically reading the entire DB from
%%% disks and causing tons of IO.
%%% @end
%%% Created : 18 Sep 2015 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(snarl_token_vnode).
-behaviour(riak_core_vnode).
-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         repair/4,
         get/3,
         delete/3,
         add/4
        ]).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_overload_command/3,
         handle_overload_info/2,
         handle_exit/3]).

-ignore_xref([
              start_vnode/1,
              repair/4,
              get/3,
              delete/3,
              add/4
             ]).

-define(TIMEOUT, 43200).

-define(TIMEOUT_LIMIT, 1000).

-define(TIMEOUT_CYCLE, 100).

-record(state, {
          db,
          tokens,
          api_db,
          api_tokens,
          partition,
          node,
          cnt = 0,
          access_cnt = 0,
          timeout_limit,
          timeout_cycle
         }).
-define(MASTER, snarl_token_vnode_master).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Token, VClock, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Token, VClock, Obj},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, Token) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Token},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

add(Preflist, ReqID, Token, {default, User}) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, Token,
                                    {expiery(?TIMEOUT), User}},
                                   {fsm, undefined, self()},
                                   ?MASTER);
add(Preflist, ReqID, Token, {infinity, User}) ->
    riak_core_vnode_master:command(Preflist,
                                   {add_api, ReqID, Token, User},
                                   {fsm, undefined, self()},
                                   ?MASTER);
add(Preflist, ReqID, Token, {Timeout, User}) ->
    riak_core_vnode_master:command(Preflist,
                                   {add, ReqID, Token,
                                    {expiery(Timeout), User}},
                                   {fsm, undefined, self()},
                                   ?MASTER);

add(Preflist, ReqID, Token, User) ->
    add(Preflist, ReqID, Token, {?TIMEOUT, User}).

delete(Preflist, ReqID, Token) ->
    riak_core_vnode_master:command(Preflist,
                                   {delete, ReqID, Token},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================

init([Partition]) ->
    process_flag(trap_exit, true),
    TimeoutLimit = dflt_env(token_timeout_limit, ?TIMEOUT_LIMIT),
    TimeoutCycle = dflt_env(token_timeout_cycle, ?TIMEOUT_CYCLE),
    {ok, init_db(#state{
                    tokens = #{},
                    api_tokens = #{},
                    partition = Partition,
                    node = node(),
                    timeout_limit = TimeoutLimit,
                    timeout_cycle = TimeoutCycle})}.

handle_overload_command(_Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}).

handle_overload_info(_, _Idx) ->
    ok.

handle_command({repair, {Realm, Token}, _, Obj}, _Sender, State) ->
    lager:warning("[repair:~p/~p] performed ~p~n", [Realm, Token, Obj]),
    T0 = erlang:system_time(seconds),
    Key = term_to_binary({Realm, Token}),
    case ft_obj:val(Obj) of
        {infinit, _} ->
            bitcask:put(State#state.api_db, Key, term_to_binary(Obj)),
            Tokens1 = maps:put(Key, Obj, State#state.api_tokens),
            {noreply, State#state{api_tokens=Tokens1}};
        {Exp, _V} when Exp > T0->
            bitcask:put(State#state.db, Key, term_to_binary(Obj)),
            Tokens1 = maps:put(Key, Obj, State#state.tokens),
            {noreply, State#state{tokens=Tokens1}};
        _ ->
            {noreply, State}
    end;

handle_command({get, ReqID, {Realm, Token}}, _Sender, State) ->
    NodeIdx = {State#state.partition, State#state.node},
    Key = term_to_binary({Realm, Token}),
    case get_key(Key, State) of
        {ok, Res, State1} ->
            {reply, {ok, ReqID, NodeIdx, Res}, State1};
        {not_found, State1} ->
            {reply, {ok, ReqID, NodeIdx, not_found}, State1}
    end;

handle_command({delete, {ReqID, _Coordinator}, {Realm, Token}}, _Sender,
               State) ->
    Key = term_to_binary({Realm, Token}),
    bitcask:delete(State#state.db, Key),
    bitcask:delete(State#state.api_db, Key),
    {reply, {ok, ReqID},
     State#state{
       tokens = maps:remove(Key, State#state.tokens),
       api_tokens = maps:remove(Key, State#state.api_tokens)
      }};

handle_command({add, {ReqID, Coordinator}, {Realm, Token}, {Exp, Value}},
               _Sender, State) ->
    TObject = ft_obj:new({Exp, Value}, Coordinator),
    State1 = expire(State),

    Key = term_to_binary({Realm, Token}),

    Ts0 = maps:put(Key, TObject, State1#state.tokens),
    bitcask:put(State1#state.db, Key, term_to_binary(TObject)),
    {reply, {ok, ReqID, Token}, State1#state{
                                  tokens = Ts0,
                                  access_cnt = State1#state.access_cnt + 1,
                                  cnt = State1#state.cnt + 1
                                 }};

handle_command({add_api, {ReqID, Coordinator}, {Realm, Token}, Value},
               _Sender, State) ->
    TObject = ft_obj:new({infinity, Value}, Coordinator),
    Key = term_to_binary({Realm, Token}),
    Ts0 = maps:put(Key, TObject, State#state.api_tokens),
    bitcask:put(State#state.api_db, Key, term_to_binary(TObject)),
    {reply, {ok, ReqID, Token}, State#state{api_tokens = Ts0}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    FoldFn = fun(K, V, A) ->
                     Fun(K, binary_to_term(V), A)
             end,
    Acc1 = bitcask:fold(State#state.db, FoldFn, Acc0),
    Acc = bitcask:fold(State#state.api_db, FoldFn, Acc1),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    T0 = erlang:system_time(seconds),
    {Key, Obj} = binary_to_term(Data),
    case ft_obj:val(Obj) of
        {infinit, _} ->
            bitcask:put(State#state.api_db, Key, term_to_binary(Obj)),
            Tokens1 = maps:put(Key, Obj, State#state.api_tokens),
            {reply, ok, State#state{api_tokens=Tokens1}};
        {Exp, _V} when Exp > T0 ->
            bitcask:put(State#state.db, Key, term_to_binary(Obj)),
            Tokens1 = maps:put(Key, Obj, State#state.tokens),
            {reply, ok, State#state{tokens=Tokens1}};
        _ ->
            {reply, ok, State}
    end.

encode_handoff_item(Token, Data) ->
    term_to_binary({Token, Data}).

is_empty(State) ->
    Empty =  bitcask:is_empty_estimate(State#state.db) andalso
        bitcask:is_empty_estimate(State#state.api_db),
    {Empty, State}.

delete(State) ->
    bitcask:close(State#state.db),
    bitcask:close(State#state.api_db),
    {DBDir, APIDir} = db_dirs(State),
    del_dir(DBDir),
    del_dir(APIDir),
    State1 = init_db(State),
    {ok, State1#state{tokens = #{},
                      api_tokens = #{}}}.

expire_obj(DB, T0, Realm, RK, V, Acc) ->
    case binary_to_term(RK) of
        {R, K} when R =:= Realm ->
            Obj = binary_to_term(V),
            case ft_obj:val(Obj) of
                {Exp, _V} when Exp > T0 ->
                    [K | Acc];
                _ ->
                    bitcask:delete(DB, RK),
                    Acc
            end;
        _ ->
            Acc
    end.

handle_coverage({list, Realm, ReqID}, _KeySpaces, _Sender, State) ->
    T0 = erlang:system_time(seconds),
    DB = State#state.db,
    %% Get the OAuth tokens first, delete timeouted ones while we are
    %% at it.
    Ks1 = bitcask:fold(State#state.db,
                       fun (RK, V, Acc) ->
                               expire_obj(DB, T0, Realm, RK, V, Acc)
                       end, []),
    %% Add the api keys, it's simpler since we don't need to do
    %% timeouts here.
    Ks = bitcask:fold_keys(State#state.api_db,
                           fun (RK, Acc) ->
                                   case binary_to_term(RK) of
                                       {R, K} when R =:= Realm ->
                                           [K | Acc];
                                       _ ->
                                           Acc
                                   end
                           end, Ks1),
    {reply,
     {ok, ReqID, {State#state.partition, State#state.node}, Ks},
     State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{db=DBRef, api_db = APIDB}) ->
    bitcask:close(DBRef),
    bitcask:close(APIDB),
    ok.


%%%===================================================================
%%% Internal Functions
%%%===================================================================

db_dirs(#state{partition = Partition}) ->
    PString = integer_to_list(Partition),
    {ok, DataDir} = application:get_env(riak_core, platform_data_dir),
    DBDir = DataDir ++ "/token/" ++ PString,
    APIDir = DataDir ++ "/token/api/" ++ PString,
    {DBDir, APIDir}.

init_db(State) ->
    {DBDir, APIDir} = db_dirs(State),
    lager:info("[token] DB Dir: ~s", [DBDir]),
    DBRef = bitcask:open(DBDir, [{expiry_time, max_timeout()}, read_write]),
    APIDBRef = bitcask:open(APIDir, [read_write]),
    State#state{
      db = DBRef,
      api_db = APIDBRef
     }.

dflt_env(N, D) ->
    case application:get_env(N) of
        undefined ->
            D;
        {ok, V} ->
            V
    end.

expire(#state{cnt = Cnt, timeout_limit = Limit} = State)
  when Limit < Cnt ->
    State;

expire(#state{access_cnt = Cnt, timeout_cycle = Cycle} = State)
  when Cycle < Cnt ->
    State;

expire(#state{tokens = Tokens, db = DBRef} = State) ->
    T0 = erlang:system_time(seconds),
    Tokens1 = maps:filter(
                fun(Key, Obj) ->
                        case ft_obj:val(Obj) of
                            {Exp, _V} when Exp > T0  ->
                                true;
                            _->
                                {Realm, Token} = binary_to_term(Key),
                                lager:debug("[token:~p] Expiering: ~p",
                                            [Realm, Token]),
                                bitcask:delete(DBRef, Key),
                                false
                        end
                end, Tokens),
    State#state{
      access_cnt = 0,
      tokens = Tokens1,
      cnt = maps:size(Tokens1)
     };

expire(State) ->
    State.

expiery(T) ->
    erlang:system_time(seconds) + T.

max_timeout() ->
    max_timeout(dflt_env(token_timeout, ?TIMEOUT)).

max_timeout(T) ->
    SubSys = [code_grant, refresh_token, client_credentials,
              password_credentials],
    max_timeout(SubSys, T).

max_timeout([S | R], T) ->
    case oauth2_config:expiry_time(S) of
        T0 when T0 > T ->
            max_timeout(R, T0);
        _ ->
            max_timeout(R, T)
    end;

max_timeout([], T) ->
    T.


del_dir(Dir) ->
    lists:foreach(fun(D) ->
                          ok = file:del_dir(D)
                  end, del_all_files([Dir], [])).

del_all_files([], EmptyDirs) ->
    EmptyDirs;
del_all_files([Dir | T], EmptyDirs) ->
    {ok, FilesInDir} = file:list_dir(Dir),
    {Files, Dirs} = lists:foldl(fun(F, {Fs, Ds}) ->
                                        Path = Dir ++ "/" ++ F,
                                        case filelib:is_dir(Path) of
                                            true ->
                                                {Fs, [Path | Ds]};
                                            false ->
                                                {[Path | Fs], Ds}
                                        end
                                end, {[], []}, FilesInDir),
    lists:foreach(fun(F) ->
                          ok = file:delete(F)
                  end, Files),
    del_all_files(T ++ Dirs, [Dir | EmptyDirs]).


get_key(Key, State) ->
    T0 = erlang:system_time(seconds),
    get_mem(T0, Key, State).

get_mem(T0, Key, State = #state{tokens = Tokens0, db = DBRef}) ->
    case maps:find(Key, Tokens0) of
        error ->
            %% if we don't find a normal memory token we
            %% check for a memory cached aPI token first
            get_api_mem(T0, Key, State);
        {ok, Obj} ->
            case ft_obj:val(Obj) of
                {Exp, _V} when Exp > T0->
                    {ok, Obj, State};
                _ ->
                    bitcask:delete(DBRef, Key),
                    Tokens1 = maps:remove(Key, Tokens0),
                    %% We know this is an expired token,
                    %% we don't need to look further
                    {not_found, State#state{tokens = Tokens1}}
            end
    end.

get_api_mem(T0, Key, State = #state{api_tokens = Tokens0}) ->
    case maps:find(Key, Tokens0) of
        error ->
            %% If we do not have it in the memory cache,
            %% we'll look for a non api token first
            %% since they are more common.
            get_db(T0, Key, State);
        {ok, Obj} ->
            {ok, Obj, State}
    end.

get_db(T0, Key, State = #state{tokens = Tokens0, db = DBRef}) ->
    case bitcask:get(DBRef, Key) of
        {ok, V} ->
            Obj = binary_to_term(V),
            case ft_obj:val(Obj) of
                {Exp, _V} when Exp > T0->
                    Tokens1 = maps:put(Key, Obj, Tokens0),
                    {ok, Obj, State#state{tokens = Tokens1}};
                _ ->
                    bitcask:delete(DBRef, Key),
                    %% We know this is an expired token,
                    %% we don't need to look further
                    {not_found, State}
            end;
        _ ->
            get_api_db(Key, State)
    end.

get_api_db(Key, State = #state{api_tokens = Tokens0, api_db = DBRef}) ->
    case bitcask:get(DBRef, Key) of
        {ok, V} ->
            Obj = binary_to_term(V),
            Tokens1 = maps:put(Key, Obj, Tokens0),
            {ok, Obj, State#state{api_tokens = Tokens1}};
        _ ->
            {not_found, State}
    end.
