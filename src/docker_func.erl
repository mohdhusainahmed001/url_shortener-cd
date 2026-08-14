%% This module contains helper functions for docker
-module(docker_func).

-export([
  get_container_id/0,
  get_container_ip/0,
  get_port/0,
  put_env/2,
  get_registry_ip/0,
  get_registry_port/0,
  get_odp_ip/0,
  get_odp_port/0,
  get_redis_host/0,
  get_redis_port/0,
  get_redis_pass/0
  ]).

-include("onex.hrl").

%%%%%%%%%%%%%%%%%%%%%% DOCKER UTIL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%

get_container_ip()  -> get_env("DOCKER_IP").

get_port()          -> get_env("DOCKER_PORT").

get_odp_ip()        -> get_env("ODP_IP").
get_odp_port()      -> get_env("ODP_PORT").

get_redis_host()    -> get_env("REDIS_HOST").
get_redis_port()    -> get_env("REDIS_PORT").
get_redis_pass()    -> get_env("REDIS_PASS").

get_registry_ip()   ->
  case get_env("REGISTRY_IP") of
    <<"undefined">> ->
      % ?oxlog_e("REGISTRY_IP variable not set! Please set it using put_env()!~n", []),
      "undefined";
    Val             ->
      Val
  end.

get_registry_port()   ->
  case get_env("REGISTRY_PORT") of
    <<"undefined">> ->
      % ?oxlog_e("REGISTRY_PORT variable not set! Please set it using put_env()!~n", []),
      "undefined";
    Val             ->
      Val
  end.

get_container_id()  ->
  {ok, Hostname} = inet:gethostname(),
  Hostname.

%%%%%%%%%%%%%%%%%%%%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%

get_env(Key) ->
  case os:getenv(Key) of
    false -> <<"undefined">>;
    Value ->
      case Key of
        "DOCKER_PORT"   -> list_to_integer(Value);
        "REGISTRY_PORT" -> list_to_integer(Value);
        _             -> Value
      end
  end.

put_env(Key, Val)   -> os:putenv(Key, Val).