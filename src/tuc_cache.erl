-module(tuc_cache).

-include("onex.hrl").

-behaviour(gen_server).

%API
-export([start_link/0, stop/0, get_tuc_timezone/1]).

%Gen Server Callbacks
-export([init/1, handle_info/2, terminate/2, handle_call/3, handle_cast/2]).

-define(TUC_DB_ENDPOINT, "/tuc_cache").
-define(TABLE, tuc_cache).
-define(GUN_TIMEOUT, 5000).
-define(REFRESH_TIMEOUT, 60000).    % 1min

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
  gen_server:stop(?MODULE).

get_tuc_timezone(TucId) ->
  case ets:lookup(?TABLE, TucId) of
    []   ->
      timezone_util:get_server_timezone();
    [{_, #{<<"timezone">> := Timezone}}]                                      ->
      Timezone
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Gen Server Callbacks %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
  ets:new(?TABLE,[named_table,set,public,
    {write_concurrency, true},
    {read_concurrency, true}]),

  erlang:send_after(?REFRESH_TIMEOUT, self(), refresh_cache),
  {ok, #{}}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(refresh_cache, State) ->
  case get_auth_ip_port() of
    {error, _Reason} ->
      ?oxlog_e("Failed to get Ip and Port from Registry Cache~n",[]);
    {Ip, Port}       ->
      case connect(Ip, Port) of
        {error, _} ->
          ok;
        ConnPid    ->
          case send_request(ConnPid) of
            {ok, 200, Body} ->
              decode_and_store_ets(Body),
              ok;
            {error, Reason} ->
              ?oxlog_e("Failed with reason:- ~p~n", [Reason])
          end,
          gun:close(ConnPid)
      end
  end,
  erlang:send_after(?REFRESH_TIMEOUT, self(), refresh_cache),
  {noreply, State};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Internal Functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
decode_and_store_ets(Body) ->
  case catch jiffy:decode(Body, [return_maps]) of
    #{<<"data">> := TucsList} ->
      case TucsList of
        []       ->
          io:format("~nReceived empty body, skipping processing~n"),
          ok;
        TucList  ->
          NewKeys = [maps:get(<<"tuc_id">>, Item) || Item <- TucList],
          CurrentKeys = [Key || {Key, _} <- ets:tab2list(?TABLE)],
          ObsoleteKeys = CurrentKeys -- NewKeys,
          [ets:delete(?TABLE, Key) || Key <- ObsoleteKeys],

          lists:foreach(
            fun(#{<<"tuc_id">>       := TucId,
              <<"multipart_failed">> := MultipartFailed,
              <<"message_expiry">>   := MessageExpiry,
              <<"sla">>              := SLA,
              <<"agec">>             := AGEC,
              <<"timezone">>         := Timezone,
              <<"timezone_offset">>  := TimezoneOffset}) ->

                MsgExp =
                  case MessageExpiry of
                    <<"undefined">>                 -> undefined;
                    undefined                       -> undefined;
                    _ when is_binary(MessageExpiry) -> binary_to_integer(MessageExpiry);
                    _                               -> undefined
                  end,
                Value =
                  #{
                    <<"multipart_failed">> => MultipartFailed,
                    <<"message_expiry">>   => MsgExp,
                    <<"agec">>             => AGEC,
                    <<"sla">>              => SLA,
                    <<"timezone">>         => Timezone,
                    <<"timezone_offset">>  => TimezoneOffset
                  },
              ets:insert(?TABLE, {TucId, Value})
            end, TucList)
      end;
    _                         ->
      io:format("~nInvalid body format,skipping processing~n")
  end.

get_auth_ip_port() ->
  case registry_cache:get_node(<<"auth">>) of
    {error, Reason}                        ->
      ?oxlog_e("Failed to get Auth IP and PORT:- ~p~n", [Reason]),
      {error, Reason};
    #{<<"ip">> := Ip, <<"port">> := Port}  ->
      {Ip, Port}
  end.

send_request(ConnPid) ->
  case send(ConnPid) of
    {ok, 200, Body}       ->
      {ok, 200, Body};
    {ok, StatusCode, Msg} ->
      ?oxlog_e("Failed to get data with Status: ~p and Reason: ~p~n", [StatusCode, Msg]),
      {error, StatusCode};
    {error, Reason}       ->
      ?oxlog_e("Failed to get data: ~p", [Reason]),
      {error, Reason}
  end.

send(ConnPid) ->
  StreamRef = gun:get(ConnPid, ?TUC_DB_ENDPOINT, ?HEADERS),
  case gun:await(ConnPid, StreamRef, ?GUN_TIMEOUT) of
    {response, fin, Status, _Headers}   ->
      {ok, Status, []};
    {response, nofin, Status, _Headers} ->
      case gun:await_body(ConnPid, StreamRef, ?GUN_TIMEOUT) of
        {ok, Body}      ->
          {ok, Status, Body};
        {error, Reason} ->
          {error, {body_receive_failed, Reason}}
      end;
    {error, Reason}                     ->
      ?oxlog_e("Error: Failed with Reason: ~p~n", [Reason]),
      gun:close(ConnPid),
      {error, Reason}
  end.

connect(Ip, Port) ->
  case gun:open(Ip, Port, #{retry => 0}) of
    {ok, ConnPid}   ->
      case gun:await_up(ConnPid, ?GUN_TIMEOUT) of
        {ok, _Protocol}  ->
          ConnPid;
        {error, timeout} ->
          ?oxlog_e("Connection Timeout on server: ~p Port: ~p~n",[Ip, Port]),
          {error, timeout};
        {error, Reason}  ->
          ?oxlog_e("Error in gun await_up connecting to ~p: ~p~n", [Ip, Reason]),
          gun:close(ConnPid),
          {error, Reason}
      end;
    {error, Reason} ->
      ?oxlog_e("Failed to open connection: ~p~n", [Reason]),
      {error, Reason}
  end.
