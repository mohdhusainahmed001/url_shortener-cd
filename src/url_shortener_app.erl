%%%-------------------------------------------------------------------
%% @doc url_shortener public API
%% @end
%%%-------------------------------------------------------------------

-module(url_shortener_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    Dispatch = cowboy_router:compile([
            {'_', [ {"/generate_short_url", short_url_handler, []},
                    {"/longurl_by_shorturl", shorturl_to_longurl_handler, []}
                    ]}
    ]),

    {ok, _} = cowboy:start_clear(http, [{port, 12000}, {max_connections, 10000},{num_acceptors, 100}], #{
            env => #{dispatch => Dispatch}
    }),
    publish:send(),
    url_shortener_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
