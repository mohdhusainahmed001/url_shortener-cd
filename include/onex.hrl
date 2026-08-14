
-define(oxlog_d(Format, Args),
  logger:debug("~s" ++ Format ++ "~n",
    [string:left(lists:flatten(io_lib:format("~p(~p):", [?MODULE, ?LINE])), 20, $ ) | Args])).

-define(oxlog_i(Format, Args),
  logger:info("~s" ++ Format ++ "~n",
    [string:left(lists:flatten(io_lib:format("~p(~p):", [?MODULE, ?LINE])), 20, $ ) | Args])).

-define(oxlog_w(Format, Args),
  logger:warning("~s" ++ Format ++ "~n",
    [string:left(lists:flatten(io_lib:format("~p(~p):", [?MODULE, ?LINE])), 20, $ ) | Args])).

-define(oxlog_e(Format, Args),
  logger:error("~s" ++ Format ++ "~n",
    [string:left(lists:flatten(io_lib:format("~p(~p):", [?MODULE, ?LINE])), 20, $ ) | Args])).

-define(SERVER_TYPE, "url_shortener").

-define(REGISTRY_ENDPOINT, "/publish").

-define(HEADERS, [{"Content-Type", "application/json"}]).

-define(READY_STATUS, "ready").

-define(UP_STATUS, "up").

-define(REGISTRY_TIME_TO_REFRESH, 2000).

-define(REGISTRY_CACHE, registry_cache).

-define(DOMAIN_CACHE, domain_cache).

-define(EXPIRY_TS_SHORTURL, 90 * 24 * 60 * 60 * 1000).  % 90 days

-define(ROCKSDB_SHARDS_CONFIG, [
      #{
          name => shard1,
          host => "10.20.3.53",
          port => 8098
      },
      #{
          name => shard2,
          host => "10.20.3.116",
          port => 8098
      },
      #{
          name => shard3,
          host => "10.20.3.125",
          port => 8098
      }
  ]).

-define(UTC_ZERO_TIMEZONE, <<"UTC">>).