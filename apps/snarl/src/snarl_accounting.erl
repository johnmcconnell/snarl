-module(snarl_accounting).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-behaviour(snarl_sync_element).

-export([
         sync_repair/3,
         sync_key/1,
         create/5,
         update/5,
         delete/2,
         destroy/5,
         raw/2,
         get/2,
         get/3,
         get/4,
         list/0
        ]).

-ignore_xref([
              create/5,
              update/5,
              destroy/5,
              get/2,
              get/3,
              get/4
             ]).

-define(TIMEOUT, 5000).

-define(FM(Met, Mod, Fun, Args),
        folsom_metrics:histogram_timed_update(
          {snarl, accounting, Met},
          Mod, Fun, Args)).


sync_key(B) when is_binary(B) ->
    B;
sync_key({_, _} = T) ->
    term_to_binary(T).

delete(_Realm, _Element) ->
    lager:error("[acounting] Delete is not supported for accounting data").

sync_repair(Realm, Key, Obj) ->
    {Org, Elem} = binary_to_term(Key),
    do_write(Realm, Org, sync_repair, {Elem, Obj}).

create(Realm, Org, Resource, Time, Metadata) ->
    do_write(Realm, Org, create, {Resource, Time, Metadata}).

update(Realm, Org, Resource, Time, Metadata) ->
    do_write(Realm, Org, update, {Resource, Time, Metadata}).

destroy(Realm, Org, Resource, Time, Metadata) ->
    do_write(Realm, Org, destroy, {Resource, Time, Metadata}).

raw(Realm, Key) when is_binary(Key) ->
    {Org, UUID} = binary_to_term(Key),
    get(Realm, Org, UUID).

list() ->
    ?FM(list_all, snarl_accounting_coverage, start,
        [snarl_accounting_vnode_master, snarl_accounting, list]).


%% sync_repair(Realm, UUID, Obj) ->
%%     do_write(Realm, UUID, sync_repair, Obj).


-spec get(Realm::binary(), Org::fifo:org_id()) ->
                 not_found |
                 {error, timeout} |
                 {ok, Org::fifo:org()}.
get(Realm, Org) when is_binary(Org) ->
    case ?FM(get, snarl_accounting_read_fsm, start,
             [{snarl_accounting_vnode, snarl_accounting}, get, {Realm, Org}]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

get(Realm, Org, Resource) ->
    case ?FM(get, snarl_accounting_read_fsm, start,
             [{snarl_accounting_vnode, snarl_accounting}, get,
              {Realm, Org}, Resource, false]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

get(Realm, Org, Start, Stop) ->
    case ?FM(get, snarl_accounting_read_fsm, start,
             [{snarl_accounting_vnode, snarl_accounting}, get,
              {Realm, Org}, {Start, Stop}, false]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

%===================================================================
%%% Internal Functions
%%%===================================================================

do_write(Realm, Org, Op, Val) ->
    ?FM(Op, snarl_entity_write_fsm, write,
        [{snarl_accounting_vnode, snarl_accounting}, {Realm, Org}, Op, Val]).
