-module(snarl_accounting).
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         sync_repair/3,
         create/5,
         update/5,
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
          {snarl,accounting, Met},
          Mod, Fun, Args)).

sync_repair(Realm, {Org, Elem}, Obj) ->
    do_write(Realm, Org, sync_repair, {Elem, Obj}).

create(Realm, Org, Element, Time, Metadata) ->
    do_write(Realm, Org, create, {Element, Time, Metadata}).

update(Realm, Org, Element, Time, Metadata) ->
    do_write(Realm, Org, update, {Element, Time, Metadata}).

destroy(Realm, Org, Element, Time, Metadata) ->
    do_write(Realm, Org, destroy, {Element, Time, Metadata}).

raw(Realm, {Org, UUID}) ->
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
get(Realm, Org) ->
    case ?FM(get, snarl_accounting_read_fsm, start,
             [{snarl_accounting_vnode, snarl_accounting}, get, {Realm, Org}]) of
        {ok, not_found} ->
            not_found;
        R ->
            R
    end.

get(Realm, Org, Element) ->
    case ?FM(get, snarl_accounting_read_fsm, start,
             [{snarl_accounting_vnode, snarl_accounting}, get,
              {Realm, Org}, Element, false]) of
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
