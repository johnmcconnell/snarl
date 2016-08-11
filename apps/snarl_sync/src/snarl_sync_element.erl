-module(snarl_sync_element).

-export([raw/3, repair/4, delete/3, sync_key/2]).

-type element_id() :: binary() | {binary(), binary()}.
-callback raw(Realm::binary(), Element::element_id()) ->
    not_found |
    {ok, term()}.

-callback sync_repair(Realm::binary(),
                      Element::element_id(),
                      Obj::term()) ->
    ok.

-callback delete(Realm::binary(), UUID::binary()) ->
    ok | {error, _} | not_found.

-callback sync_key(UUID::element_id()) ->
    binary().

raw(System, Realm, Element) ->
    System:raw(Realm, Element).

repair(System, Realm, UUID, Obj) ->
    System:sync_repair(Realm, UUID, Obj).

delete(System, Realm, UUID) ->
    System:delete(Realm, UUID).

sync_key(System, Element) ->
    System:sync_key(Element).
