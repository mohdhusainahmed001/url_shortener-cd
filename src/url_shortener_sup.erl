%%%-------------------------------------------------------------------
%% @doc url_shortener top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(url_shortener_sup).

-behaviour(supervisor).

-include("onex.hrl").

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

-define(EROCKS_DB_PATH, "/data/tmpdb/erocksdb.fold.test").

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    init_rocks_db(),
    ets:new(?REGISTRY_CACHE,[named_table, set, public,{write_concurrency, true},{read_concurrency, true}]),
    ets:new(?DOMAIN_CACHE,[named_table, set, public,{write_concurrency, true},{read_concurrency, true}]),
    elocaltime:start(),
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    ChildSpecs = [
        #{id => registry_manager,
          start => {registry_manager,start_link,[]}
         },
        #{id => registry_cache,
          start => {registry_cache,start_link,[]}
        },
        #{id => domain_cache,
          start => {domain_cache,start_link,[]}
        },
         #{id => shorturl_queue,
          start => {shorturl_queue,start_link,[]}
        },
        #{id => shorturl_queue_reader,
          start => {shorturl_queue_reader,start_link,[]}
        },
        #{id => clicker_aggregator,
          start => {clicker_aggregator,start_link,[]}
        },
        #{id => kafka_backup_queue,
          start => {kafka_backup_queue,start_link,[]}
        },
        #{id => timezone_updater,
          start => {timezone_updater, start_link,[]}
        },
        #{id => tuc_cache,
          start => {tuc_cache, start_link,[]}
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions

init_rocks_db() ->
  {ok, CWD} = file:get_cwd(),
  Path      = lists:concat([CWD, ?EROCKS_DB_PATH]),
  filelib:ensure_dir(lists:concat([CWD, "/data/tmpdb/"])),
  Options   = [{create_if_missing, true}],
  {ok, DB}  = rocksdb:open(Path, Options),
  persistent_term:put(rocksdb, DB).
