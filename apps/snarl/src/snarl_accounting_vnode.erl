-module(snarl_accounting_vnode).
-behaviour(riak_core_vnode).
-behaviour(riak_core_aae_vnode).

%%-behaviour(riak_core_aae_vnode).

-include("snarl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

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
         handle_exit/3,
         handle_overload_command/3,
         handle_overload_info/2,
         handle_info/2]).

-export([
         master/0,
         aae_repair/2,
         hash_object/2
        ]).

%% Reads
-export([get/4, get/3]).

%% Writes
-export([
         create/4,
         destroy/4,
         update/4,
         repair/3, repair/4, sync_repair/4,
         get_index_n/1
        ]).

-ignore_xref([
              create/4,
              destroy/4,
              update/4,
              sync_repair/4,
              get_index_n/1
             ]).

-define(SERVICE, snarl_accounting).

-define(MASTER, snarl_accounting_vnode_master).

-record(state, {
          partition,
          enabled = false,
          node = node(),
          db_path,
          hashtrees,
          sync_tree
         }).

-type state() :: #state{}.

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(Key, Obj) ->
    integer_to_binary(erlang:phash2({Key, Obj})).

aae_repair(Realm, OrgIDResource) when is_binary(OrgIDResource) ->
    {OrgID, Resource} = binary_to_term(OrgIDResource),
    aae_repair(Realm, {OrgID, Resource});

aae_repair(Realm, {OrgID, Resource}) ->
    lager:debug("AAE Repair: ~p/~p", [OrgID, Resource]),
    snarl_accounting:get(Realm, OrgID, Resource).

get_index_n({Bucket, {Key, _}}) ->
    riak_core_util:get_index_n({Bucket, Key}).


%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, {Realm, Org}, Resources) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Realm, Org, Resources},
                                   ignore,
                                   ?MASTER).

repair(IdxNode, {Realm, Org}, Resource, Entries) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Realm, Org, Resource, Entries},
                                   ignore,
                                   ?MASTER).

%%%===================================================================
%%% API - reads
%%%===================================================================

get(Preflist, ReqID, {Realm, Org}) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Realm, Org},
                                   {fsm, undefined, self()},
                                   ?MASTER).

get(Preflist, ReqID, {Realm, Org}, {Start, Stop}) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Realm, Org, Start, Stop},
                                   {fsm, undefined, self()},
                                   ?MASTER);

get(Preflist, ReqID, {Realm, Org}, Resource) when is_binary(Resource) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Realm, Org, Resource},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

sync_repair(Preflist, ReqID, {Realm, {OrgID, Elem}}, Obj) ->
    sync_repair(Preflist, ReqID, {Realm, OrgID}, {Elem, Obj});

sync_repair(Preflist, ReqID, {Realm, OrgID}, {Elem, Obj}) ->
    riak_core_vnode_master:command(
      Preflist,
      {sync_repair, ReqID, Realm, OrgID, Elem, Obj},
      {fsm, undefined, self()},
      ?MASTER).

create(Preflist, ReqID, {Realm, OrgID}, {ResourceID, Timestamp, Metadata}) ->
    riak_core_vnode_master:command(
      Preflist,
      {create, ReqID, Realm, OrgID, ResourceID, Timestamp, Metadata},
      {fsm, undefined, self()},
      ?MASTER).

update(Preflist, ReqID, {Realm, OrgID}, {ResourceID, Timestamp, Metadata}) ->
    riak_core_vnode_master:command(
      Preflist,
      {update, ReqID, Realm, OrgID, ResourceID, Timestamp, Metadata},
      {fsm, undefined, self()},
      ?MASTER).

destroy(Preflist, ReqID, {Realm, OrgID}, {ResourceID, Timestamp, Metadata}) ->
    riak_core_vnode_master:command(
      Preflist,
      {destroy, ReqID, Realm, OrgID, ResourceID, Timestamp, Metadata},
      {fsm, undefined, self()},
      ?MASTER).

%%%===================================================================
%%% VNode
%%%===================================================================
init([Partition]) ->
    process_flag(trap_exit, true),
    WorkerPoolSize = case application:get_env(snarl, async_workers) of
                         {ok, Val} ->
                             Val;
                         undefined ->
                             5
                     end,
    Enabled = application:get_env(snarl, accounting, false),
    {ok, DBPath} = application:get_env(fifo_db, db_path),
    FoldWorkerPool = {pool, snarl_worker, WorkerPoolSize, []},
    HT = riak_core_aae_vnode:maybe_create_hashtrees(
           snarl_accounting, Partition, snarl_accounting_vnode, undefined),

    {ok,
     #state{
        partition = Partition,
        db_path = DBPath,
        hashtrees = HT,
        enabled = Enabled,
        sync_tree = snarl_sync_tree:get_tree(snarl_accounting)
       },
     [FoldWorkerPool]}.

handle_overload_command(_Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}).

handle_overload_info(_, _Idx) ->
    ok.

%%%===================================================================
%%% General
%%%===================================================================

-type action() :: create | update | destroy.
-spec insert(action(), binary(), binary(), binary(), pos_integer(), term(),
             state()) ->
                    state().
insert(Action, Realm, OrgID, Resource, Timestamp, Metadata, State) ->
    insert1(Action, Realm, OrgID, Resource, Timestamp, Metadata, State),
    Res = for_resource(Realm, OrgID, Resource, State),
    Hash = hash_object({Realm, {OrgID, Resource}}, Res),
    Key = snarl_sync_element:sync_key(snarl_accounting, {OrgID, Resource}),
    snarl_sync_tree:update(State#state.sync_tree, snarl_accounting,
                           {Realm, Key}, Res),
    riak_core_aae_vnode:update_hashtree(
      Realm, term_to_binary({OrgID, Resource}), Hash, State#state.hashtrees),
    State.

insert1(create, Relam, OrgID, Resource, Timestamp, Metadata, State) ->
    with_db(
      Relam, OrgID, State,
      fun(DB) ->
              esqlite3:q("INSERT INTO `create` (uuid, time, metadata) VALUES "
                         "(?1, ?2, ?3)",
                         [Resource, Timestamp, term_to_binary(Metadata)], DB)
      end);
insert1(update, Relam, OrgID, Resource, Timestamp, Metadata, State) ->
    with_db(
      Relam, OrgID, State,
      fun(DB) ->
              esqlite3:q("INSERT INTO `update` (uuid, time, metadata) VALUES "
                         "(?1, ?2, ?3)",
                         [Resource, Timestamp, term_to_binary(Metadata)], DB)
      end);
insert1(destroy, Relam, OrgID, Resource, Timestamp, Metadata, State) ->
    with_db(
      Relam, OrgID, State,
      fun(DB) ->
              esqlite3:q("INSERT INTO `destroy` (uuid, time, metadata) VALUES "
                         "(?1, ?2, ?3)",
                         [Resource, Timestamp, term_to_binary(Metadata)], DB)
      end).

handle_command({_, {ReqID, _}, _, _, _, _, _},
               _Sender, State = #state{enabled = false}) ->
    {reply, {ok, ReqID}, State};

handle_command({create, {ReqID, _}, Relam, OrgID, Resource, Timestamp,
                Metadata}, _Sender, State) ->
    State1 = insert(create, Relam, OrgID, Resource, Timestamp, Metadata, State),
    {reply, {ok, ReqID}, State1};

handle_command({update, {ReqID, _}, Relam, OrgID, Resource, Timestamp,
                Metadata}, _Sender, State) ->
    State1 = insert(update, Relam, OrgID, Resource, Timestamp, Metadata, State),
    {reply, {ok, ReqID}, State1};

handle_command({destroy, {ReqID, _}, Relam, OrgID, Resource, Timestamp,
                Metadata}, _Sender, State) ->
    State1 = insert(destroy, Relam, OrgID, Resource, Timestamp, Metadata,
                    State),
    {reply, {ok, ReqID}, State1};

handle_command({sync_repair, {ReqID, _}, Realm, OrgID, Resource, Entries},
               _Sender, State) ->
    State1 = lists:foldl(fun({Timestamp, Action, Metadata}, SAcc) ->
                                 insert(Action, Realm, OrgID, Resource,
                                        Timestamp, Metadata, SAcc)
                         end, State, Entries),
    {reply, {ok, ReqID}, State1};

handle_command({repair, Realm, OrgID, Entries}, _Sender, State) ->
    State1 = lists:foldl(fun({Timestamp, Action, Resource, Metadata}, SAcc) ->
                                 insert(Action, Realm, OrgID, Resource,
                                        Timestamp, Metadata, SAcc)
                         end, State, Entries),
    {noreply, State1};

handle_command({repair, Realm, OrgID, Resource, Entries}, _Sender, State) ->
    State1 = lists:foldl(fun({Timestamp, Action, Metadata}, SAcc) ->
                                 insert(Action, Realm, OrgID, Resource,
                                        Timestamp, Metadata, SAcc)
                         end, State, Entries),
    {noreply, State1};

handle_command({get, ReqID, _, _}, _Sender, State = #state{enabled = false}) ->
    Res = [],
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({get, ReqID, Realm, OrgID}, _Sender, State) ->
    with_db(
      Realm, OrgID, State,
      fun(DB) ->
              ResC =
                  [{T, create, E, binary_to_term(M)} ||
                      {E, T, M} <- esqlite3:q("SELECT * FROM `create`", DB)],
              ResU =
                  [{T, update, E, binary_to_term(M)} ||
                      {E, T, M} <- esqlite3:q("SELECT * FROM `update`", DB)],
              ResD =
                  [{T, destroy, E, binary_to_term(M)} ||
                      {E, T, M} <- esqlite3:q("SELECT * FROM `destroy`", DB)],
              Res = ResC ++ ResU ++ ResD,
              NodeIdx = {State#state.partition, State#state.node},
              {reply, {ok, ReqID, NodeIdx, Res}, State}
      end);

handle_command({get, ReqID, _, _, _}, _Sender,
               State = #state{enabled = false}) ->
    Res = [],
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({get, ReqID, Realm, OrgID, Resource}, _Sender, State) ->
    Res = for_resource(Realm, OrgID, Resource, State),
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({hashtree_pid, Node}, _, State) ->
    %% Handle riak_core request forwarding during ownership handoff.
    %% Following is necessary in cases where anti-entropy was enabled
    %% after the vnode was already running
    case {node(), State#state.hashtrees} of
        {Node, undefined} ->
            HT1 =  riak_core_aae_vnode:maybe_create_hashtrees(
                     snarl_accounting,
                     State#state.partition,
                     snarl_accounting_vnode,
                     undefined),
            {reply, {ok, HT1}, State#state{hashtrees = HT1}};
        {Node, HT} ->
            {reply, {ok, HT}, State};
        _ ->
            {reply, {error, wrong_node}, State}
    end;

handle_command({rehash, {Realm, OrgRes}}, Sender, State)
  when is_binary(Realm),
       is_binary(OrgRes) ->
    {OrgID, Resource} = binary_to_term(OrgRes),
    handle_command({rehash, {Realm, {OrgID, Resource}}}, Sender, State);

handle_command({rehash, {Realm, {OrgID, Resource}}}, _,
               State=#state{hashtrees=HT}) when is_binary(Realm),
                                                is_binary(OrgID),
                                                is_binary(Resource) ->
    OrgRes = term_to_binary({OrgID, Resource}),
    case for_resource(Realm, OrgID, Resource, State) of
        [] ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete(
              [{object, {Realm, OrgRes}}], HT);
        Res ->
            Hash = hash_object({Realm, {OrgID, Resource}}, Res),
            riak_core_aae_vnode:update_hashtree(
              Realm, OrgRes, Hash, HT)
    end,
    {noreply, State};

handle_command({get, ReqID, _, _, _, _}, _Sender,
               State = #state{enabled = false}) ->
    Res = [],
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State};

handle_command({get, ReqID, Realm, OrgID, Start, Stop}, _Sender, State) ->
    Q = "SELECT `create`.uuid AS uuid, `create`.time AS time,"
        "       `create`.metadata AS metadata, 'create' AS action"
        "  FROM `create`"
        "  LEFT JOIN `destroy` ON `create`.uuid = `destroy`.uuid"
        "  WHERE `create`.time < ?2"
        "    AND (`destroy`.time IS NULL OR `destroy`.time > ?1)"
        " UNION ALL "
        "SELECT `destroy`.uuid AS uuid, `destroy`.time AS time,"
        "       `destroy`.metadata AS metadata, 'destroy' AS action"
        "  FROM `destroy`"
        "  JOIN `create` ON `create`.uuid = `destroy`.uuid"
        "  WHERE `create`.time < ?2"
        "    AND `destroy`.time > ?1 AND `destroy`.time < ?2"
        " UNION ALL "
        "SELECT `update`.uuid AS uuid, `update`.time AS time,"
        "       `update`.metadata AS metadata, 'update' AS action"
        "  FROM `update` WHERE uuid IN ("
        "    SELECT `create`.uuid"
        "      FROM `create`"
        "      LEFT JOIN `destroy` ON `create`.uuid = `destroy`.uuid"
        "      WHERE `create`.time < ?2"
        "        AND (`destroy`.time IS NULL OR `destroy`.time > ?1));",
    with_db(
      Realm, OrgID, State,
      fun(DB) ->
              Res = [{T, a2a(A), E, binary_to_term(M)} ||
                        {E, T, M, A} <- esqlite3:q(Q, [Start, Stop], DB)],
              NodeIdx = {State#state.partition, State#state.node},
              {reply, {ok, ReqID, NodeIdx, lists:sort(Res)}, State}
      end);

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
               State=#state{partition = P, db_path = Path}) ->
    BasePath = filename:join([Path, integer_to_list(P), "accounting"]),
    AsyncWork =
        fun() ->
                case file:list_dir(BasePath) of
                    {ok, Realms} when length(Realms) > 0 ->
                        R = lists:foldl(
                              fun(Realm, AccR) ->
                                      RealmPath = filename:join([BasePath,
                                                                 Realm]),
                                      case file:list_dir(RealmPath) of
                                          {ok, Orgs} when length(Orgs) > 0 ->
                                              lists:foldl(
                                                fun (Org, AccO)
                                                    %% properly formated uuid
                                                      when length(Org) == 36 ->
                                                        handle_org(RealmPath,
                                                                   Fun, Realm,
                                                                   Org, AccO);
                                                    (_Bad, AccO) ->
                                                        AccO
                                                end, AccR, Orgs);
                                          _ ->
                                              AccR
                                      end
                              end, Acc0, Realms),
                        riak_core_vnode:reply(Sender, R);
                    _ ->
                        riak_core_vnode:reply(Sender, Acc0)
                end
        end,
    {async, AsyncWork, Sender, State}.

handle_org(RealmPath, Fun, Realm, Org, Acc) ->
    OrgPath = filename:join([RealmPath, Org]),
    {ok, C} = esqlite3:open(OrgPath),
    try
        Q = "SELECT * FROM ("
            "SELECT `create`.uuid AS uuid, `create`.time AS time,"
            "       `create`.metadata AS metadata, 'create' AS action"
            "  FROM `create`"
            " UNION ALL "
            "SELECT `destroy`.uuid AS uuid, `destroy`.time AS time,"
            "       `destroy`.metadata AS metadata, 'destroy' AS action"
            "  FROM `destroy`"
            " UNION ALL "
            "SELECT `update`.uuid AS uuid, `update`.time AS time,"
            "       `update`.metadata AS metadata, 'update' AS action"
            "  FROM `update`) ORDER BY uuid",
        case esqlite3:q(Q, C) of
            [] ->
                esqlite3:close(C),
                Acc;
            [{Res0, T0, M0, A0} | Es] ->

                In = {Res0, [{T0, a2a(A0), M0}]},
                RealmB = list_to_binary(Realm),
                OrgB = list_to_binary(Org),
                {{ResOut, LOut}, AccOut} =
                    lists:foldl(fun({Res, T, M, A}, {{Res, L}, AccRes}) ->
                                        MB = binary_to_term(M),
                                        {{Res, [{T, a2a(A), MB} | L]}, AccRes};
                                   ({Res, T, M, A}, {{ResOut, LOut}, AccRes}) ->
                                        AccRes1 = Fun({RealmB,
                                                       {OrgB, ResOut}},
                                                      LOut, AccRes),
                                        MB = binary_to_term(M),
                                        {{Res, [{T, a2a(A), MB}]}, AccRes1}
                                end, {In, Acc}, Es),
                Fun({RealmB, {OrgB, ResOut}}, LOut, AccOut)
        end
    after
        esqlite3:close(C)
    end.

a2a(<<"create">>) -> create;
a2a(<<"update">>) -> update;
a2a(<<"destroy">>) -> destroy.

handle_handoff_command(?FOLD_REQ{} = FR, Sender, State) ->
    handle_command(FR, Sender, State);


handle_handoff_command({get, _ReqID, _Realm, _OrgID, _Resource} = Req, Sender,
                       State) ->
    handle_command(Req, Sender, State);

handle_handoff_command({get, _ReqID, _Realm, _OrgID} = Req, Sender, State) ->
    handle_command(Req, Sender, State);

handle_handoff_command(_Req, _Sender, State) ->
    {forward, State}.

handoff_starting(TargetNode, State) ->
    lager:warning("Starting handof to: ~p", [TargetNode]),
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {{Realm, {Org, Resource}}, Entries} = binary_to_term(Data),
    with_db(
      Realm, Org, State,
      fun(DB) ->
              lists:map(
                fun ({Time, create, Meta}) ->
                        esqlite3:q("INSERT INTO `create` "
                                   "(uuid, time, metadata) "
                                   "VALUES (?1, ?2, ?3)",
                                   [Resource, Time, term_to_binary(Meta)], DB);
                    ({Time, update, Meta}) ->
                        esqlite3:q("INSERT INTO `update` "
                                   "(uuid, time, metadata) "
                                   "VALUES (?1, ?2, ?3)",
                                   [Resource, Time, term_to_binary(Meta)], DB);
                    ({Time, destroy, Meta}) ->
                        esqlite3:q("INSERT INTO `destroy` "
                                   "(uuid, time, metadata) "
                                   "VALUES (?1, ?2, ?3)",
                                   [Resource, Time, term_to_binary(Meta)], DB)
                end, Entries)
      end),
    {reply, ok, State}.

encode_handoff_item(Org, Data) ->
    term_to_binary({Org, Data}).

is_empty(State) ->
    BasePath = base_path(State),
    case file:list_dir(BasePath) of
        {ok, [_ | _]} ->
            {false, State};
        _ ->
            {true, State}
    end.

delete(State) ->
    BasePath = base_path(State),
    del_dir(BasePath),
    {ok, State}.

handle_coverage(list, _KeySpaces, Sender,
                State=#state{partition = P}) ->
    BasePath = base_path(State),
    AsyncWork =
        fun() ->
                Realms = case file:list_dir(BasePath) of
                             {ok, RealmsX} ->
                                 RealmsX;
                             _ ->
                                 []
                         end,
                [begin
                     RealmPath = filename:join([BasePath, Realm]),
                     case file:list_dir(RealmPath) of
                         {ok, Orgs} ->
                             [handle_list(
                                Sender,
                                RealmPath,
                                Realm,
                                Org) || Org <- Orgs],
                             ok;
                         _ ->
                             ok
                     end
                 end || Realm <- Realms],
                riak_core_vnode:reply(Sender, {done, {P, node()}})
        end,
    {async, AsyncWork, Sender, State};

handle_coverage(repair_metadata, _KeySpaces, Sender,
                State=#state{partition = P}) ->
    lager:debug("[accounting:metadata_repair:~p] started.", [P]),
    BasePath = base_path(State),
    AsyncWork =
        fun() ->
                Realms = case file:list_dir(BasePath) of
                             {ok, RealmsX} ->
                                 RealmsX;
                             _ ->
                                 []
                         end,
                [begin
                     RealmPath = filename:join([BasePath, Realm]),
                     case file:list_dir(RealmPath) of
                         {ok, Orgs} ->
                             [repair_metadata(
                                Sender,
                                RealmPath,
                                Realm,
                                Org) || Org <- Orgs],
                             ok;
                         _ ->
                             ok
                     end
                 end || Realm <- Realms],
                riak_core_vnode:reply(Sender, {done, {P, node()}})
        end,
    {async, AsyncWork, Sender, State}.

handle_list(Sender, RealmPath, Realm, Org) ->
    OrgPath = filename:join([RealmPath, Org]),
    {ok, C} = esqlite3:open(OrgPath),
    try
        Q = "SELECT uuid FROM `create`",
        UUIDs = esqlite3:q(Q, C),
        riak_core_vnode:reply(Sender, {partial,
                                       list_to_binary(Realm),
                                       list_to_binary(Org), UUIDs})
    after
        esqlite3:close(C)
    end.


repair_metadata(Sender, RealmPath, Realm, Org) ->
    OrgPath = filename:join([RealmPath, Org]),
    {ok, C} = esqlite3:open(OrgPath),
    try
        repair_metadata(Sender, Realm, Org, C, "create"),
        repair_metadata(Sender, Realm, Org, C, "update"),
        repair_metadata(Sender, Realm, Org, C, "destroy")
    after
        esqlite3:close(C)
    end.

repair_metadata(Sender, Realm, Org, C, Table) ->
    Fixed = [{Element, Time, term_to_binary(fix_meta(Meta)), Meta} ||
                {Element, Time, Meta} <-
                    esqlite3:q("SELECT * FROM `" ++ Table ++ "`", C)],
    Different = [{E, T, M} || {E, T, M, M1} <- Fixed, M /= M1],
    lager:debug("[accounting:metadata_repair] ~s/~s has ~p items to repair",
                [Org, Table, length(Different)]),
    Result = case [esqlite3:q("UPDATE `" ++ Table ++ "` SET metadata = ?1 "
                              "WHERE uuid = ?2 AND time = ?3", [M, E, T], C) ||
                      {E, T, M} <- Different] of
                 [] -> [{<<(list_to_binary(Table))/binary, ": ok">>}];
                 _ ->  [{<<(list_to_binary(Table))/binary, ": fixed">>}]
             end,
    riak_core_vnode:reply(Sender, {partial,
                                   list_to_binary(Realm),
                                   list_to_binary(Org),
                                   Result}).


fix_meta(M) when is_binary(M) ->
    try
        fix_meta(binary_to_term(M))
    catch
        _:_ ->
            M
    end;
fix_meta(M) ->
    M.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _) ->
    ok.

%%% AAE
handle_info(retry_create_hashtree,
            State=#state{hashtrees=undefined, partition=Idx}) ->
    lager:debug("snarl_accounting/~p retrying to create a hash tree.", [Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(snarl_accounting, Idx,
                                                    snarl_accounting_vnode,
                                                    undefined),
    {ok, State#state{hashtrees = HT}};

handle_info(retry_create_hashtree, State) ->
    {ok, State};

handle_info({'DOWN', _, _, Pid, _},
            State=#state{hashtrees=Pid, partition=Idx}) ->
    lager:debug("snarl_accounting/~p hashtree ~p went down.", [Idx, Pid]),
    erlang:send_after(1000, self(), retry_create_hashtree),
    {ok, State#state{hashtrees = undefined}};

handle_info({'DOWN', _, _, _, _}, State) ->
    {ok, State};

handle_info(_Msg, State) ->
    {ok, State}.

with_db(Realm, Org, State, Fun) ->
    case get_db(Realm, Org, State) of
        {ok, DB} ->
            try
                Fun(DB)
            after
                esqlite3:close(DB)
            end;
        E ->
            E
    end.

get_db(Realm, Org, State = #state{partition = P, db_path = Path}) ->
    Realms = binary_to_list(Realm),
    Orgs = binary_to_list(Org),
    BasePath = base_path(State, [Realms]),
    file:make_dir(filename:join([Path])),
    file:make_dir(filename:join([Path, integer_to_list(P)])),
    file:make_dir(filename:join([Path, integer_to_list(P),
                                 "accounting"])),
    file:make_dir(BasePath),
    DBFile = filename:join([BasePath, Orgs]),
    {ok, DB} = esqlite3:open(DBFile),
    try
        init_db(DB),
        {ok, DB}
    catch
        E:_ ->
            esqlite3:close(DB),
            {error, E}
    end.

init_db(DB) ->
    ok = esqlite3:exec("CREATE TABLE IF NOT EXISTS `create` "
                       "(uuid CHAR(36), time INTEGER, metadata BLOB, "
                       "PRIMARY KEY (time, uuid))", DB),
    ok = esqlite3:exec("CREATE INDEX IF NOT EXISTS create_time "
                       "ON 'create'(time)", DB),
    ok = esqlite3:exec("CREATE TABLE IF NOT EXISTS `destroy` "
                       "(uuid CHAR(36), time INTEGER, metadata BLOB ,"
                       "PRIMARY KEY (time, uuid))", DB),
    ok = esqlite3:exec("CREATE INDEX IF NOT EXISTS destroy_time "
                       "ON 'destroy'(time)", DB),
    ok = esqlite3:exec("CREATE TABLE IF NOT EXISTS `update` "
                       "(uuid CHAR(36), time INTEGER, metadata BLOB ,"
                       "PRIMARY KEY (time, uuid))", DB),
    ok = esqlite3:exec("CREATE INDEX IF NOT EXISTS update_time ON "
                       "'update'(time)", DB),
    ok = esqlite3:exec("CREATE INDEX IF NOT EXISTS update_uuid "
                       "ON 'update'(uuid)", DB).

del_dir(Dir) ->
    lists:foreach(fun(D) ->
                          ok = file:del_dir(D)
                  end, del_all_files([Dir], [])).

del_all_files([], EmptyDirs) ->
    EmptyDirs;
del_all_files([Dir | T], EmptyDirs) ->
    case file:list_dir(Dir) of
        {ok, FilesInDir}  ->
            {Files, Dirs} = path_or_dir(Dir, FilesInDir),
            lists:foreach(fun(F) ->
                                  ok = file:delete(F)
                          end, Files),
            del_all_files(T ++ Dirs, [Dir | EmptyDirs]);
        _ ->
            del_all_files(T, EmptyDirs)
    end.

path_or_dir(Dir, FilesInDir) ->
    lists:foldl(fun(F, {Fs, Ds}) ->
                        Path = Dir ++ "/" ++ F,
                        case filelib:is_dir(Path) of
                            true ->
                                {Fs, [Path | Ds]};
                            false ->
                                {[Path | Fs], Ds}
                        end
                end, {[], []}, FilesInDir).

for_resource(Realm, OrgID, Resource, State) ->
    with_db(
      Realm, OrgID, State,
      fun(DB) ->
              ResC = [{T, create, binary_to_term(M)} ||
                         {T, M} <-
                             esqlite3:q("SELECT time, metadata FROM `create` "
                                        "WHERE uuid=?", [Resource], DB)],
              ResU = [{T, update, binary_to_term(M)} ||
                         {T, M} <-
                             esqlite3:q("SELECT time, metadata FROM `update` "
                                        "WHERE uuid=?", [Resource], DB)],
              ResD = [{T, destroy, binary_to_term(M)} ||
                         {T, M} <-
                             esqlite3:q("SELECT time, metadata FROM `destroy` "
                                        "WHERE uuid=?", [Resource], DB)],
              ResC ++ ResU ++ ResD
      end).

base_path(State) ->
    base_path(State, []).

base_path(#state{partition = Partition, db_path = Path}, Addition) ->
    filename:join([Path, integer_to_list(Partition), "accounting"] ++ Addition).
