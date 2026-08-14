-module(shorturl_supervisor).
-behaviour(supervisor).

-include("onex.hrl").

-export([start_link/1, init/1, start_workers/1]).

start_link(Domains) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Domains]).

init([Domains]) ->
  SupFlags = #{
    strategy => one_for_one,
    intensity => 10,
    period => 60
  },

  FillerSpec = #{
    id => shorturl_filler,
    start => {shorturl_filler, start_link, [Domains]},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [shorturl_filler]
  },

  WorkerSpecs = lists:map(fun(Domain) ->
    #{
      id       => list_to_atom("shorturl_queue_" ++ binary_to_list(Domain)),
      start    => {shorturl_queue_worker, start_link, [Domain, shorturl_filler]},
      restart  => permanent,
      shutdown => 5000,
      type     => worker,
      modules  => [shorturl_queue_worker]
    }
  end, Domains),

  ChildSpecs = [FillerSpec | WorkerSpecs],

  {ok, {SupFlags, ChildSpecs}}.

start_workers(Domains) when is_list(Domains) ->
    lists:foreach(fun start_worker/1, Domains);

start_workers(_) ->
  ?oxlog_e("Invalid Domains list provided to start_workers~n", []).

start_worker(Domain) ->
  ChildId = list_to_atom("shorturl_queue_" ++ binary_to_list(Domain)),

  ChildSpec = #{
    id       => ChildId,
    start    => {shorturl_queue_worker, start_link, [Domain, shorturl_filler]},
    restart  => permanent,
    shutdown => 5000,
    type     => worker,
    modules  => [shorturl_queue_worker]
  },
  supervisor:start_child(?MODULE, ChildSpec).
