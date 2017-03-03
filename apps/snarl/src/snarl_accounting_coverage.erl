-module(snarl_accounting_coverage).

-include("snarl.hrl").

-behaviour(riak_core_coverage_fsm).

-export([
         init/2,
         process_results/2,
         process_results/3,
         finish/2,
         start/3,
         plan/2
        ]).

-ignore_xref([start/3]).

-record(state, {replies = #{} :: maps:map(),
                r :: integer(),
                reqid,
                from,
                nodes = []}).

start(VNodeMaster, NodeCheckService, Request) ->
    ReqID = snarl_vnode:mk_reqid(),
    snarl_coverage_sup:start_coverage(
      ?MODULE, {self(), ReqID, something_else},
      {VNodeMaster, NodeCheckService, Request}),
    receive
        {ok, ReqID} ->
            ok;
        {ok, ReqID, Result} ->
            {ok, Result}
    after 60000 ->
            {error, timeout}
    end.

%% The first is the vnode service used
init({From, ReqID, _}, {VNodeMaster, NodeCheckService, Request}) ->
    %% all - full coverage; allup - partial coverage
    R = ?R,
    NVal = ?N,
    VNodeSelector = allup,
    %% Same as R value here, TODO: Make this dynamic
    PrimaryVNodeCoverage = R,
    %% We timeout after 5s
    Timeout = 60000,
    State = #state{
               %%replies = dict:new(),
               r = R,
               from = From, reqid = ReqID},
    {Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, riak_core_coverage_plan, State}.

plan(Plan, State) ->
    {ok, State#state{nodes = Plan}}.

process_results(_, State) ->
    {error, State}.

process_results(_VNode, {partial, Realm, Org, UUIDs},
                State = #state{replies = Replies}) ->
    {_C, Replies1} =
        lists:foldl(fun
                        %%({UUID}, {I, M}) when (I rem 1000) == 0 ->
                        %%    {I+1, inc({Realm, {Org, UUID}}, M)};
                        ({UUID}, {I, M}) ->
                           {I+1, inc({Realm, {Org, UUID}}, M)}
                   end, {0, Replies}, UUIDs),
    {ok, State#state{replies = Replies1}};

process_results(VNode, {done, {_P, _N}}, State = #state{nodes = [VNode]}) ->
    {done, State#state{nodes = []}};

process_results(VNode, {done, {_P, _N}}, State = #state{nodes = VNodes}) ->
    {done, State#state{nodes = lists:delete(VNode, VNodes)}};

process_results(_VNode, Result, State) ->
    lager:error("Unknown process results call: ~p ~p", [Result, State]),
    {done, State}.

finish(clean, State = #state{replies = Replies,
                             from = From, r = R, reqid = ReqID}) ->
    MergedReplies = maps:fold(fun(_Key, Count, Keys) when Count < R->
                                      Keys;
                                 (Key, _Count, Keys) ->
                                      [Key | Keys]
                              end, [], Replies),
    From ! {ok, ReqID, MergedReplies},
    {stop, normal, State};

finish(How, State) ->
    lager:error("Unknown finish call: ~p ~p", [How, State]),
    {error, failed}.

inc(K, M) ->
    case maps:find(K, M) of
        {ok, V} ->
            maps:update(K, V+1, M);
        error ->
            maps:put(K, 1, M)
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
