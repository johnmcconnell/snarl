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
         repair/3, sync_repair/4,
         get_index_n/1
        ]).

-ignore_xref([
              create/4,
              destroy/4,
              update/4,
              repair/3, sync_repair/4,
              get_index_n/1
             ]).

-define(SERVICE, snarl_accounting).

-define(MASTER, snarl_accounting_vnode_master).

-record(state, {
          partition,
          node = node(),
          dbs = #{},
          db_path,
          hashtrees,
          sync_tree
         }).

%%%===================================================================
%%% AAE
%%%===================================================================

master() ->
    ?MASTER.

hash_object(Key, Obj) ->
    integer_to_binary(erlang:phash2({Key, Obj})).

aae_repair(Realm, {OrgID, Element}) ->
    lager:debug("AAE Repair: ~p/~p", [OrgID, Element]),
    snarl_accounting:get(Realm, OrgID, Element).

get_index_n({Bucket, {Key, _}}) ->
    riak_core_util:get_index_n({Bucket, Key}).


%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Org, Elements) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Org, Elements},
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

get(Preflist, ReqID, {Realm, Org}, Element) when is_binary(Element) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Realm, Org, Element},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%%%===================================================================
%%% API - writes
%%%===================================================================

sync_repair(Preflist, ReqID, {Realm, OrgID}, {Elem, Obj}) ->
    riak_core_vnode_master:command(Preflist,
                                   {sync_repair, ReqID, Realm, OrgID, Elem, Obj},
                                   {fsm, undefined, self()},
                                   ?MASTER).

create(Preflist, ReqID, {Realm, OrgID}, {ElementID, Timestamp, Metadata}) ->
    riak_core_vnode_master:command(Preflist,
                                   {create, ReqID, Realm, OrgID, ElementID, Timestamp, Metadata},
                                   {fsm, undefined, self()},
                                   ?MASTER).

update(Preflist, ReqID, {Realm, OrgID}, {ElementID, Timestamp, Metadata}) ->
    riak_core_vnode_master:command(Preflist,
                                   {update, ReqID, Realm, OrgID, ElementID, Timestamp, Metadata},
                                   {fsm, undefined, self()},
                                   ?MASTER).

destroy(Preflist, ReqID, {Realm, OrgID}, {ElementID, Timestamp, Metadata}) ->
    riak_core_vnode_master:command(Preflist,
                                   {destroy, ReqID, Realm, OrgID, ElementID, Timestamp, Metadata},
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
    {ok, DBPath} = application:get_env(fifo_db, db_path),
    FoldWorkerPool = {pool, snarl_worker, WorkerPoolSize, []},
    HT = riak_core_aae_vnode:maybe_create_hashtrees(
           snarl_accounting, Partition, snarl_accounting_vnode, undefined),

    {ok,
     #state{
        partition = Partition,
        db_path = DBPath,
        hashtrees = HT,
        sync_tree = snarl_sync_tree:get_tree(snarl_accounting)
       },
     [FoldWorkerPool]}.

%%%===================================================================
%%% General
%%%===================================================================

insert(Action, Realm, OrgID, Element, Timestamp, Metadata, State) ->
    State1 = insert1(Action, Realm, OrgID, Element, Timestamp, Metadata, State),
    {Res, State2} = for_element(Realm, OrgID, Element, State1),
    Hash = hash_object({Realm, OrgID, Element}, Res),
    snarl_sync_tree:update(State#state.sync_tree, snarl_accounting,
                           {Realm, {OrgID, Element}}, Res),
    riak_core_aae_vnode:update_hashtree(
      Realm, {OrgID, Element}, Hash, State#state.hashtrees),
    State2.

insert1(create, Relam, OrgID, Element, Timestamp, Metadata, State) ->
    {DB, State1} = get_db(Relam, OrgID, State),
    esqlite3:q("INSERT INTO `create` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [Element, Timestamp, term_to_binary(Metadata)], DB),
    State1;
insert1(update, Relam, OrgID, Element, Timestamp, Metadata, State) ->
    {DB, State1} = get_db(Relam, OrgID, State),
    esqlite3:q("INSERT INTO `update` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [Element, Timestamp, term_to_binary(Metadata)], DB),
    State1;
insert1(destroy, Relam, OrgID, Element, Timestamp, Metadata, State) ->
    {DB, State1} = get_db(Relam, OrgID, State),
    esqlite3:q("INSERT INTO `destroy` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [Element, Timestamp, term_to_binary(Metadata)], DB),
    State1.

handle_command({create, {ReqID, _}, Relam, OrgID, Element, Timestamp, Metadata},
               _Sender, State) ->
    State1 = insert(create, Relam, OrgID, Element, Timestamp, Metadata, State),
    {reply, {ok, ReqID}, State1};

handle_command({update, {ReqID, _}, Relam, OrgID, Element, Timestamp, Metadata},
               _Sender, State) ->
    State1 = insert(update, Relam, OrgID, Element, Timestamp, Metadata, State),
    {reply, {ok, ReqID}, State1};

handle_command({destroy, {ReqID, _}, Relam, OrgID, Element, Timestamp, Metadata}, _Sender, State) ->
    State1 = insert(destroy, Relam, OrgID, Element, Timestamp, Metadata, State),
    {reply, {ok, ReqID}, State1};

handle_command({get, ReqID, Realm, OrgID}, _Sender, State) ->
    {DB, State1} = get_db(Realm, OrgID, State),
    ResC = [ {T, create, E, binary_to_term(M)} ||
               {E, T, M} <- esqlite3:q("SELECT * FROM `create`", DB)],
    ResU = [ {T, update, E, binary_to_term(M)} ||
               {E, T, M} <- esqlite3:q("SELECT * FROM `update`", DB)],
    ResD = [ {T, destroy, E, binary_to_term(M)} ||
               {E, T, M} <- esqlite3:q("SELECT * FROM `destroy`", DB)],
    Res = ResC ++ ResU ++ ResD,
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State1};

handle_command({get, ReqID, Realm, OrgID, Element}, _Sender, State) ->
    {Res, State1} = for_element(Realm, OrgID, Element, State),
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, Res}, State1};

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

handle_command({rehash, {Realm, {OrgID, Element}}}, _,
               State=#vstate{hashtrees=HT}) when is_binary(Realm) ->
    case for_element(Realm, OrgID, Element, State) of
        {[], State1} ->
            %% Make sure hashtree isn't tracking deleted data
            riak_core_index_hashtree:delete({Realm, {OrgID, Element}}, HT),
            {noreply, State1};
        {Res, State1} ->
            Hash = hash_object({Realm, OrgID, Element}, Res),
            riak_core_aae_vnode:update_hashtree(
              Realm, {OrgID, Element}, Hash, HT),
            {noreply, State1}
    end;

handle_command({get, ReqID, Realm, OrgID, Start, Stop}, _Sender, State) ->
    {DB, State1} = get_db(Realm, OrgID, State),
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
    Res = [{T, a2a(A), E, binary_to_term(M)} ||
              {E, T, M, A} <- esqlite3:q(Q, [Start, Stop], DB)],
    NodeIdx = {State#state.partition, State#state.node},
    {reply, {ok, ReqID, NodeIdx, lists:sort(Res)}, State1};

handle_command({sync_repair, ReqID, Realm, OrgID, Element, Entries}, _Sender, State) ->
    State1 = lists:foldl(fun({Timestamp, Action, Metadata}, SAcc) ->
                                 insert(Action, Realm, OrgID, Element, Timestamp, Metadata, SAcc)
                         end, State, Entries),
    {reply, {ok, ReqID}, State1};

handle_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
               State=#state{partition = P, db_path = Path, dbs = DBs}) ->
    [esqlite3:close(DB) || DB <- maps:values(DBs)],
    BasePath = filename:join([Path, integer_to_list(P), "accounting"]),
    AsyncWork =
        fun() ->
                case file:list_dir(BasePath) of
                    {ok, Realms} when length(Realms) > 0 ->
                        lists:foldl(
                          fun(Realm, AccR) ->
                                  RealmPath = filename:join([BasePath, Realm]),
                                  case file:list_dir(RealmPath) of
                                      {ok, Orgs} when length(Orgs) > 0 ->
                                          lists:foldl(fun (Org, AccO) ->
                                                              handle_org(RealmPath, Fun, Realm, Org, AccO)
                                                      end, AccR, Orgs);
                                      _ ->
                                          AccR
                                  end
                          end, Acc0, Realms);
                    _ ->
                        Acc0
                end
        end,
    FinishFun = fun(Acc) ->
                        riak_core_vnode:reply(Sender, Acc)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State#state{dbs = #{}}}.

handle_org(RealmPath, Fun, Realm, Org, Acc) ->
    OrgPath = filename:join([RealmPath, Org]),
    {ok, C} = esqlite3:open(OrgPath),
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
        [{E0, T0, M0, A0} | Es] ->
            esqlite3:close(C),
            In = {E0, [{T0, a2a(A0), M0}]},
            {{EOut, LOut}, AccOut} =
                lists:foldl(fun({E, T, M, A}, {{E, L}, AccE}) ->
                                    {{E, [{T, a2a(A), M} | L]}, AccE};
                               ({E, T, M, A}, {{EOut, LOut}, AccE}) ->
                                    AccE1 = Fun({Realm, {Org,EOut}}, LOut, AccE),
                                    {{E, [{T, a2a(A), M}]}, AccE1}
                            end, {In, Acc}, Es),
            Fun({Realm, {Org, EOut}}, LOut, AccOut)
    end.

a2a(<<"create">>) -> create;
a2a(<<"update">>) -> update;
a2a(<<"destroy">>) -> destroy.

handle_handoff_command(?FOLD_REQ{} = FR, Sender, State) ->
    handle_command(FR, Sender, State);

handle_handoff_command({get, _ReqID, _Vm} = Req, Sender, State) ->
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

handle_handoff_data({{R, O}, {T, create, E, M}}, State) ->
    {DB, State1} = get_db(R, O, State),
    esqlite3:q("INSERT INTO `create` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [E, T, term_to_binary(M)], DB),
    {ok, State1};

handle_handoff_data({{R, O}, {T, update, E, M}}, State) ->
    {DB, State1} = get_db(R, O, State),
    esqlite3:q("INSERT INTO `update` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [E, T, term_to_binary(M)], DB),
    {ok, State1};

handle_handoff_data({{R, O}, {T, destroy, E, M}}, State) ->
    {DB, State1} = get_db(R, O, State),
    esqlite3:q("INSERT INTO `destroy` (uuid, time, metadata) VALUES "
               "(?1, ?2, ?3)",
               [E, T, term_to_binary(M)], DB),
    {ok, State1}.

encode_handoff_item(Org, Data) ->
    term_to_binary({Org, Data}).

is_empty(State  = #state{partition = P, db_path = Path}) ->
    BasePath = filename:join([Path, integer_to_list(P), "accounting"]),
    case file:list_dir(BasePath) of
        {ok, [_ | _]} ->
            {false, State};
        _ ->
            {true, State}
    end.

delete(State = #state{partition = P, db_path = Path, dbs = DBs}) ->
    [esqlite3:close(DB) || DB <- maps:values(DBs)],
    BasePath = filename:join([Path, integer_to_list(P), "accounting"]),
    del_dir(BasePath),
    {ok, State#state{dbs = #{}}}.

handle_coverage(list, _KeySpaces, Sender,
                State=#state{partition = P, db_path = Path, dbs = DBs}) ->
    [esqlite3:close(DB) || DB <- maps:values(DBs)],
    BasePath = filename:join([Path, integer_to_list(P), "accounting"]),
    AsyncWork =
        fun() ->
                case file:list_dir(BasePath) of
                    {ok, Realms} ->
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
                        ok;
                    _ ->
                        ok
                end
        end,
    FinishFun = fun(_Acc) ->
                        riak_core_vnode:reply(Sender, {done, {P, node()}})

                end,

    {async, {fold, AsyncWork, FinishFun}, Sender, State#state{dbs = #{}}}.

handle_list(Sender, RealmPath, Realm, Org) ->
    OrgPath = filename:join([RealmPath, Org]),
    {ok, C} = esqlite3:open(OrgPath),
    Q = "SELECT uuid FROM `create`",
    UUIDs = esqlite3:q(Q, C),
    esqlite3:close(C),
    riak_core_vnode:reply(Sender, {partial,
                                   list_to_binary(Realm),
                                   list_to_binary(Org), UUIDs}).

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{dbs = DBs}) ->
    [esqlite3:close(DB) || DB <- maps:values(DBs)],
    ok.

%%% AAE
handle_info(retry_create_hashtree,
            State=#state{hashtrees=undefined, partition=Idx}) ->
    lager:debug("snarl_accounting/~p retrying to create a hash tree.", [Idx]),
    HT = riak_core_aae_vnode:maybe_create_hashtrees(snarl_accounting, Idx, snarl_accounting_vnode, undefined),
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

get_db(Realm, Org, State = #state{partition = P, dbs = DBs, db_path = Path}) ->
    Key = {Realm, Org},
    case maps:find(Key, DBs) of
        {ok, DB} ->
            {DB, State};
        error ->
            Realms = binary_to_list(Realm),
            Orgs = binary_to_list(Org),
            BasePath = filename:join([Path, integer_to_list(P),
                                      "accounting", Realms]),
            file:make_dir(filename:join([Path])),
            file:make_dir(filename:join([Path, integer_to_list(P)])),
            file:make_dir(filename:join([Path, integer_to_list(P),
                                         "accounting"])),
            file:make_dir(filename:join([Path, integer_to_list(P),
                                         "accounting", Realms])),
            DBFile = filename:join([BasePath, Orgs]),
            {ok, DB} = esqlite3:open(DBFile),
            init_db(DB),
            DBs1 = maps:put(Key, DB, DBs),
            {DB, State#state{dbs = DBs1}}
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
    {ok, FilesInDir} = file:list_dir(Dir),
    {Files, Dirs} = lists:foldl(fun(F, {Fs, Ds}) ->
                                        Path = Dir ++ "/" ++ F,
                                        case filelib:is_dir(Path) of
                                            true ->
                                                {Fs, [Path | Ds]};
                                            false ->
                                                {[Path | Fs], Ds}
                                        end
                                end, {[],[]}, FilesInDir),
    lists:foreach(fun(F) ->
                          ok = file:delete(F)
                  end, Files),
    del_all_files(T ++ Dirs, [Dir | EmptyDirs]).

for_element(Realm, OrgID, Element, State) ->
    {DB, State1} = get_db(Realm, OrgID, State),
    ResC = [ {T, create, binary_to_term(M)} ||
               {T, M} <- esqlite3:q("SELECT time, metadata FROM `create` "
                                    "WHERE uuid=?", [Element], DB)],
    ResU = [ {T, update, binary_to_term(M)} ||
               {T, M} <- esqlite3:q("SELECT time, metadata FROM `update` "
                                    "WHERE uuid=?", [Element], DB)],
    ResD = [ {T, destroy, binary_to_term(M)} ||
               {T, M} <- esqlite3:q("SELECT time, metadata FROM `destroy` "
                                    "WHERE uuid=?", [Element], DB)],
    {ResC ++ ResU ++ ResD, State1}.