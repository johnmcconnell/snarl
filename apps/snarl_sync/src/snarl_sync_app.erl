%%%-------------------------------------------------------------------
%% @doc snarl_sync public API
%% @end
%%%-------------------------------------------------------------------

-module(snarl_sync_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    R = snarl_sync_sup:start_link(),
    case snarl_sync:enabled() of
        true ->
            ok;
        false ->
            ok
    end,
    R.

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
