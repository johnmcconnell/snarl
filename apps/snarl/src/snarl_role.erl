-module(snarl_role).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include("snarl_ent.hrl").

-behaviour(snarl_indexed).
-behaviour(snarl_sync_element).

-export([
         sync_repair/3,
         sync_key/1,
         list/0, list/1, list/3, list/4, list_/1,
         get/2, raw/2,
         lookup/2,
         add/2, delete/2,
         grant/3, revoke/3,
         set_metadata/3,
         create/3,
         revoke_prefix/3,
         import/3, wipe/2,
         reindex/2
        ]).

-ignore_xref([
              wipe/2,
              list_/1,
              create/3, raw/2, sync_repair/3,
              import/3, lookup/2
             ]).

-define(TIMEOUT, 5000).
-define(MASTER,  snarl_role_vnode_master).
-define(VNODE,   snarl_role_vnode).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, role, Met},
          Mod, Fun, Args)).

%% Public API

-define(NAME_2I, {?MODULE, name}).

reindex(Realm, UUID) ->
    case ?MODULE:get(Realm, UUID) of
        {ok, O} ->
            snarl_2i:add(Realm, ?NAME_2I, ft_role:name(O), UUID),
            ok;
        E ->
            E
    end.

wipe(Realm, UUID) ->
    ?FM(wipe, ?COVERAGE, start,
        [?MASTER, ?MODULE, {wipe, Realm, UUID}]).

sync_repair(Realm, UUID, Obj) ->
    do_write(Realm, UUID, sync_repair, Obj).

sync_key(B) when is_binary(B) ->
    B.

import(Realm, Role, Data) ->
    do_write(Realm, Role, import, Data).

-spec lookup(Realm::binary(), Role::binary()) ->
                    not_found |
                    {error, timeout} |
                    {ok, Role::fifo:role()}.
lookup(Realm, Role) ->
    folsom_metrics:histogram_timed_update(
      {snarl, role, lookup},
      fun() ->
              case snarl_2i:get(Realm, ?NAME_2I, Role) of
                  {ok, UUID} ->
                      ?MODULE:get(Realm, UUID);
                  R ->
                      R
              end
      end).

-spec get(Realm::binary(), Role::fifo:role_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, Role::fifo:role()}.
get(Realm, Role) ->
    case ?FM(get, ?READ_FSM, start,
             [{?VNODE, ?MODULE}, get, {Realm, Role}]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

raw(Realm, Role) ->
    case ?FM(get, ?READ_FSM, start,
             [{?VNODE, ?MODULE}, get,
              {Realm, Role}, undefined, true]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

list() ->
    ?FM(list_all, ?COVERAGE, start,
        [?MASTER, ?MODULE, list]).

-spec list(Realm::binary()) -> {ok, [fifo:role_id()]} |
                               not_found |
                               {error, timeout}.

list(Realm) ->
    ?FM(list, ?COVERAGE, start,
        [?MASTER, ?MODULE, {list, Realm}]).

list_(Realm) ->
    {ok, Res} =
        ?FM(list, ?COVERAGE, raw,
            [?MASTER, ?MODULE,
             Realm, []]),
    Res1 = [R || {_, R} <- Res],
    {ok,  Res1}.


%%--------------------------------------------------------------------
%% @doc Lists all vm's and fiters by a given matcher set.
%% @end
%%--------------------------------------------------------------------
-spec list(Realm::binary(), [fifo:matcher()], boolean()) ->
                  {error, timeout} |
                  {ok, [{integer(), fifo:uuid()} |
                        {integer(), ft_role:role()}]}.

list(Realm, Requirements, Full) ->
    {ok, Res} =
        ?FM(list, ?COVERAGE, full,
            [?MASTER, ?MODULE,
             Realm, Requirements]),
    Res1 = rankmatcher:apply_scales(Res),
    Res2 = case Full of
               true ->
                   Res1;
               false ->
                   [{P, ft_role:uuid(O)} || {P, O} <- Res1]
           end,
    {ok,  lists:sort(Res2)}.

-spec list(Realm::binary(), [fifo:matcher()],
           FoldFn::snal_coverage:fold_fun(), Acc0::term()) ->
                  {error, timeout} | {ok, term()}.

list(Realm, Requirements, FoldFn, Acc0) ->
    ?FM(list_all, ?COVERAGE, full,
        [?MASTER, ?MODULE, Realm, Requirements, FoldFn, Acc0]).

-spec add(Realm::binary(), Role::binary()) ->
                 {ok, UUID::fifo:role_id()} |
                 douplicate |
                 {error, timeout}.

add(Realm, Role) ->
    UUID = fifo_utils:uuid(role),
    create(Realm, UUID, Role).

create(Realm, UUID, Role) ->
    case ?MODULE:lookup(Realm, Role) of
        not_found ->
            ok = do_write(Realm, UUID, add, Role),
            snarl_2i:add(Realm, ?NAME_2I, Role, UUID),
            {ok, UUID};
        {ok, _RoleObj} ->
            duplicate
    end.

-spec delete(Realm::binary(), Role::fifo:role_id()) ->
                    ok |
                    not_found|
                    {error, timeout}.

delete(Realm, Role) ->
    case ?MODULE:get(Realm, Role) of
        {ok, O} ->
            snarl_2i:delete(Realm, ?NAME_2I, ft_role:name(O)),
            ok;
        E ->
            E
    end,
    spawn(
      fun () ->
              Prefix = [<<"roles">>, Role],
              {ok, Users} = snarl_user:list(Realm),
              [begin
                   snarl_user:leave(Realm, U, Role),
                   snarl_user:revoke_prefix(Realm, U, Prefix)
               end
               || U <- Users],
              {ok, Roles} = ?MODULE:list(Realm),
              [revoke_prefix(Realm, R, Prefix) || R <- Roles],
              {ok, Orgs} = snarl_org:list(Realm),
              [snarl_org:remove_target(Realm, O, Role) || O <- Orgs]
      end),
    do_write(Realm, Role, delete).

-spec grant(Realm::binary(), Role::fifo:role_id(), fifo:permission()) ->
                   ok |
                   not_found|
                   {error, timeout}.

grant(Realm, Role, Permission) ->
    do_write(Realm, Role, grant, Permission).

-spec revoke(Realm::binary(),
             Role::fifo:role_id(), fifo:permission()) ->
                    ok |
                    not_found|
                    {error, timeout}.

revoke(Realm, Role, Permission) ->
    do_write(Realm, Role, revoke, Permission).

-spec revoke_prefix(Realm::binary(),
                    Role::fifo:role_id(), fifo:permission()) ->
                           ok |
                           not_found|
                           {error, timeout}.

revoke_prefix(Realm, Role, Prefix) ->
    do_write(Realm, Role, revoke_prefix, Prefix).

-spec set_metadata(Realm::binary(), Role::fifo:role_id(),
                   Attirbutes::fifo:attr_list()) ->
                          not_found |
                          {error, timeout} |
                          ok.
set_metadata(Realm, Role, Attributes) ->
    do_write(Realm, Role, set_metadata, Attributes).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, Role, Op) ->
    ?FM(Op, ?WRITE_FSM, write,
        [{?VNODE, ?MODULE}, {Realm, Role}, Op]).

do_write(Realm, Role, Op, Val) ->
    ?FM(Op, ?WRITE_FSM, write,
        [{?VNODE, ?MODULE}, {Realm, Role}, Op, Val]).
