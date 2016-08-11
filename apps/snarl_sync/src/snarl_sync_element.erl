-module(snarl_sync_element).

-export([raw/3, repair/4, delete/3]).

-callback raw(Realm::binary(), Element::binary() | {binary(), binary()}) ->
    not_found |
    {ok, term()}.

-callback sync_repair(Realm::binary(),
                      Element::binary() | {binary(), binary()},
                      Obj::term()) ->
    ok.

-callback delete(Realm::binary(), UUID::binary()) ->
    ok | {error, _} | not_found.

raw(System, Realm, Element) ->
    System:raw(Realm, Element).

repair(System, Realm, UUID, Obj) ->
    System:sync_repair(Realm, UUID, Obj).

delete(System, Realm, UUID) ->
    System:delete(Realm, UUID).
