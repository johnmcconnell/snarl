-module(snarl_coverage_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    snarl_coverage_sup:start_link().

stop(_State) ->
    ok.
