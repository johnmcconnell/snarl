-module(snarl_vnode).

-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([init/5,
         mk_pfx/2,
         is_empty/1,
         delete/1,
         delete/2,
         change/6,
         get/3,
         put/4,
         fold/5,
         handle_command/3,
         handle_handoff_data/2,
         handle_coverage/4,
         handle_info/2,
         mkid/0,
         mkid/1,
         hash_object/2,
         handle_overload_command/3,
         handle_overload_info/2,
         mk_reqid/0]).

-ignore_xref([mkid/0, delete/2]).

-define(FM(Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {Mod, Fun},
          Mod, Fun, Args)).


-define(PARTIAL_SIZE, 10).

hash_object(Key, Obj) ->
    snarl_sync:hash(Key, Obj).

mkid() ->
    mkid(node()).

mkid(Actor) ->
    {mk_reqid(), Actor}.

mk_reqid() ->
    erlang:system_time(nano_seconds).

mk_bkt(#vstate{bucket = Bucket})
  when byte_size(Bucket) < 256 ->
    <<0:16,
      (byte_size(Bucket)):8/integer, Bucket/binary>>.

mk_pfx(undefined, #vstate{bucket = B}) ->
    B;

mk_pfx(Realm, State)
  when byte_size(Realm) < 256 ->
    <<(mk_bkt(State))/binary,
      (byte_size(Realm)):8/integer, Realm/binary>>.

%% extract_pfx(<<0:16,
%%               _BS:8/integer, Bucket:_BS/binary,
%%               _RS:8/integer, Realm:_RS/binary, UUID/binary>>) ->
%%     {Bucket, Realm, UUID}.

init(Partition, Bucket, Service, VNode, StateMod) ->
    DB = list_to_atom(integer_to_list(Partition)),
    fifo_db:start(DB),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(Service, Partition, VNode,
                                                    undefined),
    WorkerPoolSize = case application:get_env(snarl, async_workers) of
                         {ok, Val} ->
                             Val;
                         undefined ->
                             5
                     end,
    FoldWorkerPool = {pool, snarl_worker, WorkerPoolSize, []},
    {ok,
     #vstate{db=DB, hashtrees=HT, partition=Partition, node=node(),
             service=Service, bucket=Bucket, state=StateMod, vnode=VNode,
             sync_tree = snarl_sync_tree:get_tree(Service)},
     [FoldWorkerPool]}.

handle_overload_command(_Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}).

handle_overload_info(_, _Idx) ->
    ok.

list_keys(Realm, Sender, State = #vstate{db=DB}) ->
    Bucket = mk_pfx(Realm, State),
    FoldFn = fun (K, L) ->
                     maybe_send_partial([K|L], Sender, State)
             end,
    AsyncWork = fun() ->
                        fold_keys(DB, Bucket, FoldFn)
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State}.

list_keys(Realm, Getter, Requirements, Sender, State=#vstate{state=SM}) ->
    Prefix = mk_pfx(Realm, State),
    ID = mkid(list),
    FoldFn = fun (Key, E, C) ->
                     E1 = load_obj(ID, SM, E),
                     C1 = case rankmatcher:match(E1, Getter, Requirements) of
                              false ->
                                  C;
                              Pts ->
                                  [{Pts, Key} | C]
                          end,
                     maybe_send_partial(C1, Sender, State)
             end,
    fold(Prefix, FoldFn, [], Sender, State).

list(Realm, Getter, Requirements, Sender, State=#vstate{state=SM}) ->
    Prefix = mk_pfx(Realm, State),
    ID = mkid(list),
    FoldFn = fun (Key, E, C) ->
                     E1 = load_obj(ID, SM, E),
                     C1 = case rankmatcher:match(E1, Getter, Requirements) of
                         false ->
                             C;
                         Pts ->
                             [{Pts, {Key, E1}} | C]
                     end,
                     maybe_send_partial(C1, Sender, State)
             end,
    fold(Prefix, FoldFn, [], Sender, State).

fold(Prefix, Fun, Acc0, Sender, State=#vstate{db=DB}) ->
    AsyncWork = fun() ->
                        ?FM(fifo_db, fold, [DB, Prefix, Fun, Acc0])
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State}.

put(Realm, Key, Obj, State) when is_binary(Realm) ->
    Bucket = mk_pfx(Realm, State),
    ?FM(fifo_db, put, [State#vstate.db, Bucket, Key, Obj]),
    SKey = snarl_sync_element:sync_key(snarl_accounting, Key),
    snarl_sync_tree:update(State#vstate.sync_tree, State#vstate.service,
                           {Realm, SKey}, Obj),
    riak_core_aae_vnode:update_hashtree(
      Realm, Key, vc_bin(ft_obj:vclock(Obj)), State#vstate.hashtrees).

change(Realm, UUID, Action, Vals, {ReqID, Coordinator} = ID,
       State=#vstate{state=Mod}) ->
    case snarl_vnode:get(Realm, UUID, State) of
        {ok, O} ->
            H0 = ft_obj:val(O),
            H1 = Mod:load(ID, H0),
            H2 = case Vals of
                     [Val] ->
                         Mod:Action(ID, Val, H1);
                     [Val1, Val2] ->
                         Mod:Action(ID, Val1, Val2, H1);
                     Vals when is_list(Vals) ->
                         Args = [ID | Vals] ++ [H1],
                         erlang:apply(Mod, Action, Args)
                 end,
            Obj = ft_obj:update(H2, Coordinator, O),
            snarl_vnode:put(Realm, UUID, Obj, State),
            {reply, {ok, ReqID}, State};
        R ->
            lager:error("[~s:~s] Tried to write to a non existing element: "
                        "~p => ~p",
                        [State#vstate.bucket, Action, UUID, R]),
            {reply, {ok, ReqID, not_found}, State}
    end.

%%%===================================================================
%%% Callbacks
%%%===================================================================

is_empty(State=#vstate{db=DB}) ->
    FoldFn = fun (_, _) -> {false, State} end,
    ?FM(fifo_db, fold_keys, [DB, mk_bkt(State), FoldFn, {true, State}]).

delete(State=#vstate{db=DB}) ->
    Bucket = mk_bkt(State),
    do_delete(DB, Bucket),
    {ok, State}.

delete(Realm, State=#vstate{db=DB}) ->
    Bucket = mk_pfx(Realm, State),
    do_delete(DB, Bucket),
    {ok, State}.

handle_coverage({wipe, Realm, UUID}, _KeySpaces, {_, ReqID, _}, State) ->
    Bucket = mk_pfx(Realm, State),
    ?FM(fifo_db, delete, [State#vstate.db, Bucket, UUID]),
    {reply, {ok, ReqID}, State};

handle_coverage({lookup, Realm, Name}, _KeySpaces, Sender,
                State=#vstate{state=Mod}) ->
    Bucket = mk_pfx(Realm, State),
    ID = mkid(lookup),
    FoldFn = fun (U, O, [not_found]) ->
                     V = ft_obj:val(O),
                     case Mod:name(Mod:load(ID, V)) of
                         AName when AName =:= Name ->
                             [U];
                         _ ->
                             [not_found]
                     end;
                 (_, _O, Res) ->
                     Res
             end,
    fold(Bucket, FoldFn, [not_found], Sender, State);

handle_coverage(list, _KeySpaces, Sender, State) ->
    Bucket = mk_bkt(State),
    FoldFn = fun(<<_RS:8/integer, Realm:_RS/binary, K/binary>>, _V, Acc) ->
                     maybe_send_partial([{Realm, K} | Acc], Sender, State)
             end,
    fold(Bucket, FoldFn, [], Sender, State);

handle_coverage({list, Realm}, _KeySpaces, Sender, State) ->
    list_keys(Realm, Sender, State);

handle_coverage({list, Realm, Requirements}, KeySpaces, Sender, State) ->
    handle_coverage({list, Realm, Requirements, false}, KeySpaces, Sender,
                    State);

handle_coverage({list, undefined, [], true}, _KeySpaces, Sender,
                State=#vstate{bucket = Bucket, state=SM}) ->
    ID = mkid(),
    FoldFn = fun(K, O, Acc) ->
                     O1 = load_obj(ID, SM, O),
                     [{0, {K, O1}} | Acc]
             end,
    fold(Bucket, FoldFn, [], Sender, State);


handle_coverage({list, Realm, Requirements, Full}, _KeySpaces, Sender,
                State = #vstate{state=Mod}) ->
    case Full of
        true ->
            list(Realm, fun Mod:getter/2, Requirements, Sender, State);
        false ->
            list_keys(Realm, fun Mod:getter/2, Requirements, Sender, State)
    end;

handle_coverage(Req, _KeySpaces, _Sender, State) ->
    lager:warning("Unknown coverage request: ~p", [Req]),
    {stop, not_implemented, State}.

handle_command({sync_repair, {ReqID, _}, {Realm, UUID}, Obj}, _Sender,
               %% VNode equals the control module
               State=#vstate{state=Mod}) ->
    case get(Realm, UUID, State) of
        {ok, Old} ->
            ID = snarl_vnode:mkid(),
            Old1 = load_obj(ID, Mod, Old),
            lager:info("[sync-repair:~s] Merging with old object", [UUID]),
            Merged = ft_obj:merge(snarl_entity_read_fsm, [Old1, Obj]),
            snarl_vnode:put(Realm, UUID, Merged, State);
        not_found ->
            lager:info("[sync-repair:~s] Writing new object", [UUID]),
            snarl_vnode:put(Realm, UUID, Obj, State);
        _ ->
            lager:error("[~s] Read repair failed, data was updated too recent.",
                        [State#vstate.bucket])
    end,
    spawn(State#vstate.service, reindex, [Realm, UUID]),
    {reply, {ok, ReqID}, State};

handle_command({repair, {Realm, UUID}, _VClock, Obj}, _Sender,
               %% VNode equals the control module
               State=#vstate{state=Mod}) ->
    case get(Realm, UUID, State) of
        {ok, Old} ->
            ID = snarl_vnode:mkid(),
            Old1 = load_obj(ID, Mod, Old),
            Merged = ft_obj:merge(snarl_entity_read_fsm, [Old1, Obj]),
            snarl_vnode:put(Realm, UUID, Merged, State);
        not_found ->
            snarl_vnode:put(Realm, UUID, Obj, State);
        _ ->
            lager:error("[~s] Read repair failed, data was updated too recent.",
                        [State#vstate.bucket])
    end,
    spawn(State#vstate.service, reindex, [Realm, UUID]),
    {noreply, State};

handle_command({get, ReqID, {Realm, UUID}}, _Sender, State) ->
    Res = case get(Realm, UUID, State) of
              {ok, R} ->
                  case  load_obj(ReqID, State#vstate.state, R) of
                      R1 when R =/= R1 ->
                          snarl_vnode:put(Realm, UUID, R1, State),
                          R1;
                      R1 ->
                          R1
                  end;
              not_found ->
                  not_found
          end,
    NodeIdx = {State#vstate.partition, State#vstate.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({delete, {ReqID, _Coordinator}, {undefined, UUID}}, _Sender,
               State) ->
    Bucket = State#vstate.bucket,
    ?FM(fifo_db, delete, [State#vstate.db, Bucket, UUID]),
    {reply, {ok, ReqID}, State};

handle_command({delete, {ReqID, _Coordinator}, {Realm, UUID}}, _Sender,
               State) ->
    Bucket = mk_pfx(Realm, State),
    ?FM(fifo_db, delete, [State#vstate.db, Bucket, UUID]),
    riak_core_index_hashtree:delete(
      [{object, {Realm, UUID}}], State#vstate.hashtrees),
    {reply, {ok, ReqID}, State};

handle_command({import, {ReqID, Coordinator} = ID, {Realm, UUID}, Data},
               _Sender, State=#vstate{state=Mod}) ->
    H1 = Mod:load(ID, Data),
    H2 = Mod:uuid(ID, UUID, H1),
    case get(Realm, UUID, State) of
        {ok, O} ->
            Obj = ft_obj:update(H2, Coordinator, O),
            snarl_vnode:put(Realm, UUID, Obj, State),
            {reply, {ok, ReqID}, State};
        _R ->
            Obj = ft_obj:new(H2, Coordinator),
            snarl_vnode:put(Realm, UUID, Obj, State),
            {reply, {ok, ReqID}, State}
    end;

%%%===================================================================
%%% AAE
%%%===================================================================

handle_command({hashtree_pid, Node}, _, State) ->
    %% Handle riak_core request forwarding during ownership handoff.
    %% Following is necessary in cases where anti-entropy was enabled
    %% after the vnode was already running
    case {node(), State#vstate.hashtrees} of
        {Node, undefined} ->
            HT1 =  riak_core_aae_vnode:maybe_create_hashtrees(
                     State#vstate.service,
                     State#vstate.partition,
                     State#vstate.vnode,
                     undefined),
            {reply, {ok, HT1}, State#vstate{hashtrees = HT1}};
        {Node, HT} ->
            {reply, {ok, HT}, State};
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_command({rehash, {Realm, UUID}}, _,
               State=#vstate{hashtrees=HT}) when is_binary(Realm) ->
    case get(Realm, UUID, State) of
        {ok, Obj} ->
            riak_core_aae_vnode:update_hashtree(
              Realm, UUID, vc_bin(ft_obj:vclock(Obj)), HT);
        _ ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete([{object, {Realm, UUID}}], HT)
    end,
    {noreply, State};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
               State=#vstate{db=DB}) ->
    Bucket = mk_bkt(State),
    FoldFn = fun(<<_RS:8/integer, Realm:_RS/binary, K/binary>>, V, O) ->
                     Fun({Realm, K}, V, O)
             end,
    AsyncWork = fun() ->
                        ?FM(fifo_db, fold, [DB, Bucket, FoldFn, Acc0])
                end,
    FinishFun = fun(Acc) ->
                        riak_core_vnode:reply(Sender, Acc)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State};


handle_command({Action, ID, {Realm, UUID}, Param1, Param2}, _Sender, State) ->
    change(Realm, UUID, Action, [Param1, Param2], ID, State);

handle_command({Action, ID, {Realm, UUID}, Param}, _Sender, State) ->
    change(Realm, UUID, Action, [Param], ID, State);

handle_command(Message, _Sender, State) ->
    lager:error("[~s] Unknown command: ~p", [State#vstate.bucket, Message]),
    {noreply, State}.

handle_handoff_data(Data, State=#vstate{state=SM}) ->
    {{Realm, UUID}, O} = binary_to_term(Data),
    ID = snarl_vnode:mkid(handoff),
    Obj = load_obj(ID, SM, O),
    Obj1 = case snarl_vnode:get(Realm, UUID, State) of
               {ok, OldObj} ->
                   OldObj1 = load_obj(ID, SM, OldObj),
                   ft_obj:merge(snarl_entity_read_fsm, [OldObj1, Obj]);
               not_found ->
                   Obj
           end,
    snarl_vnode:put(Realm, UUID, Obj1, State),
    {reply, ok, State}.

partial(Reply, {_, ReqID, _} = Sender, #vstate{node=N, partition=P}) ->
    riak_core_vnode:reply(Sender, {partial, ReqID, {P, N}, Reply}).

reply(Reply, {_, ReqID, _} = Sender, #vstate{node=N, partition=P}) ->
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Reply}).

get(Realm, UUID, State) ->
    Bucket = mk_pfx(Realm, State),
    try
        ?FM(fifo_db, get, [State#vstate.db, Bucket, UUID])
    catch
        E1:E2 ->
            lager:error("[fifo_db] Failed to get object ~s:~s/~s ~p:~p ~w",
                        [Realm, State#vstate.bucket, UUID, E1, E2,
                         erlang:get_stacktrace()]),
            not_found
    end.

handle_info(retry_create_hashtree,
            State=#vstate{service=Srv, hashtrees=undefined, partition=Idx,
                          vnode=VNode}) ->
    lager:debug("~p/~p retrying to create a hash tree.", [Srv, Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(Srv, Idx, VNode, undefined),
    {ok, State#vstate{hashtrees = HT}};

handle_info(retry_create_hashtree, State) ->
    {ok, State};

handle_info({'DOWN', _, _, Pid, _},
            State=#vstate{service=Service, hashtrees=Pid, partition=Idx}) ->
    lager:debug("~p/~p hashtree ~p went down.", [Service, Idx, Pid]),
    erlang:send_after(1000, self(), retry_create_hashtree),
    {ok, State#vstate{hashtrees = undefined}};

handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};

handle_info(_, State) ->
    {ok, State}.


load_obj({T, ID}, Mod, Obj) ->
    V = ft_obj:val(Obj),
    case Mod:load({T-1, ID}, V) of
        V1 when V1 /= V ->
            ft_obj:update(V1, ID, Obj);
        _ ->
            ft_obj:update(Obj)
    end.

vc_bin(VClock) ->
    term_to_binary(lists:sort(VClock)).

do_delete(DB, Bucket) ->
    FoldFn = fun (K, A) -> [{delete, <<Bucket/binary, K/binary>>} | A] end,
    Trans = fold_keys(DB, Bucket, FoldFn),
    ?FM(fifo_db, transact, [DB, Trans]).

fold_keys(DB, Bucket, FoldFn) ->
    ?FM(fifo_db, fold_keys, [DB, Bucket, FoldFn, []]).

maybe_send_partial(L, Sender, State) when length(L) >= ?PARTIAL_SIZE  ->
    partial(L, Sender, State),
    [];
maybe_send_partial(L, _Sender, _State) ->
    L.
