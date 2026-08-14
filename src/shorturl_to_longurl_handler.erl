-module(shorturl_to_longurl_handler).

-include("onex.hrl").

-export([init/2]).

init(Req0, Opts) ->
  Method = cowboy_req:method(Req0),
  Req1 =
    case Method of
      <<"GET">>  ->
        QsVals   = cowboy_req:parse_qs(Req0),
        handle_get(Req0, QsVals);
      <<"POST">> ->
       {ok, Body, _} = cowboy_req:read_body(Req0),
        process_body(Req0, Body);
      _          ->
        RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, <<"Method_Not_Allowed">>},{<<"code">>, 405}]},
        cowboy_req:reply(405, content_type(), jiffy:encode(RespBody), Req0)
    end,
  {ok, Req1, Opts}.

handle_get(Req, QsVals) ->
  ShortUrl = proplists:get_value(<<"short_url">>, QsVals),
  case ShortUrl of
    undefined ->
      RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, <<"No ShortUrl Present">>},{<<"code">>, 400}]},
      cowboy_req:reply(400, content_type(), jiffy:encode(RespBody), Req);
    _ ->
      case get_longurl(ShortUrl) of
        {LongUrl, TrackLocation} ->
          RespBody = {[{<<"long_url">>, LongUrl}, {<<"track_location">>, TrackLocation}]},
          cowboy_req:reply(200, content_type(), jiffy:encode(RespBody), Req);
        error ->
          RespBody = {[{<<"error">>, <<"Unable to get LongUrl">>}]},
          cowboy_req:reply(200, content_type(), jiffy:encode(RespBody), Req)
      end
  end.

process_body(Req, Body) ->
  VBody = try json:decode(Body) of
    DBody  -> {true, DBody};
    _      -> {false, <<"format_not_supported">>}
    catch
    _:_ ->
    {false, <<"Invalid_JSON">>}
    end,
  case VBody of
    {true, DecodedBody} ->
      ShortUrl = maps:get(<<"short_url">>, DecodedBody, undefined),
      UaData   = maps:get(<<"ua_data">>, DecodedBody, #{}),
      case get_longurl(ShortUrl, UaData) of
        {LongUrl, TrackLocation} ->
          RespBody = {[{<<"long_url">>, LongUrl}, {<<"track_location">>, TrackLocation}]},
          cowboy_req:reply(200, content_type(), jiffy:encode(RespBody), Req);
        error ->
          RespBody = {[{<<"error">>, <<"Unable to get LongUrl">>}]},
          cowboy_req:reply(200, content_type(), jiffy:encode(RespBody), Req)
      end;
    {false, Msg}   ->
      RespBody = {[{<<"status">>, <<"Failed">>},{<<"reason">>, Msg},{<<"code">>, 400}]},
      cowboy_req:reply(400, content_type(), jiffy:encode(RespBody), Req)
  end.

get_longurl(ShortUrl) ->
  case search_long_url_rocksdb(ShortUrl) of
    {LongUrl, TrackLocation, _} ->
       {LongUrl, TrackLocation};
    error ->
      ShorturlWOProtocol = strip_protocol(ShortUrl),
      case search_long_url_rocksdb(ShorturlWOProtocol) of
        {LongUrl, TrackLocation, _} ->
           {LongUrl, TrackLocation};
        error ->
          case fetch_longurl_data_from_odp(ShortUrl) of
            {LongUrl, TrackLocation, _, _} ->
              {LongUrl, TrackLocation};
            {error, _Reason} ->
              case fetch_longurl_data_from_odp(ShorturlWOProtocol) of
                {LongUrl, TrackLocation, _, _} ->
                  {LongUrl, TrackLocation};
                {error, _} ->
                  error
              end
          end
      end
  end.

get_longurl(ShortUrl, UaData) ->
  case search_long_url_rocksdb(ShortUrl) of
    {LongUrl, TrackLocation, ShortUrlClickerData} ->
       insert_in_clicker_queue(ShortUrlClickerData, UaData),
       {LongUrl, TrackLocation};
    error ->
      ShorturlWOProtocol = strip_protocol(ShortUrl),
      case search_long_url_rocksdb(ShorturlWOProtocol) of
        {LongUrl, TrackLocation, ShortUrlClickerData} ->
           insert_in_clicker_queue(ShortUrlClickerData, UaData),
           {LongUrl, TrackLocation};
        error ->
          case fetch_longurl_data_from_odp(ShortUrl) of
            {LongUrl, TrackLocation, Key, Payload} ->
              insert_in_clicker_queue(Key, Payload, UaData),
              {LongUrl, TrackLocation};
            {error, _Reason} ->
              case fetch_longurl_data_from_odp(ShorturlWOProtocol) of
                {LongUrl, TrackLocation, Key, Payload} ->
                  insert_in_clicker_queue(Key, Payload, UaData),
                  {LongUrl, TrackLocation};
                {error, _} ->
                  error
              end
          end
      end
  end.

check_channel_based_odp_url(ShortUrl) ->
  case string:find(ShortUrl, "_1") of
    true  ->
      get_odp_url("wa");
    _     ->
      case string:find(ShortUrl, "_2") of
        true -> get_odp_url("rcs");
        _    -> get_odp_url("sms")
      end
  end.

fetch_longurl_data_from_odp(ShortUrl) ->
  Url  =  check_channel_based_odp_url(ShortUrl),
  Body = #{<<"short_url">> => ShortUrl},
  case send_request(Url, Body) of
    {ok, RespBody}  ->
      case json:decode(list_to_binary(RespBody)) of
        #{<<"response">> := <<"No Records found">>} ->
          {error, <<"No Records found">>};
        DecodedResp ->
          Payload       = maps:get(<<"payload">>, DecodedResp),
          Key           = maps:get(<<"key">>, DecodedResp),
          LongUrl       = maps:get(<<"long_url">>, Payload),
          TrackLocation = maps:get(<<"track_location">>, Payload),
          {LongUrl, TrackLocation, Key, Payload}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

strip_protocol(Url) when is_binary(Url) ->
  case Url of
    <<"https://", Rest/binary>> -> Rest;
    <<"http://",  Rest/binary>> -> Rest;
    _ -> Url
  end.

search_long_url_rocksdb(ShortUrl) ->
  ShardName = util:get_shard_name(ShortUrl),
  Payload   = #{
    key => #{
      channel    => <<"shorturl">>,
      identifier => ShortUrl,
      msg_uuid   => ShortUrl
    }
  },
  case get_request_to_rocksdb(Payload, ShardName) of
    error ->
      BackupChannels = util:get_backup_channels(),
      search_in_backup_shards(Payload, BackupChannels);
    Result ->
      Result
  end.

search_in_backup_shards(_Payload, []) -> error;

search_in_backup_shards(Payload, [ShardName | RestBackupChannels]) ->
  case get_request_to_rocksdb(Payload, ShardName) of
    error ->
      search_in_backup_shards(Payload, RestBackupChannels);
    Res ->
      Res
  end.

get_request_to_rocksdb(Payload, ShardName) ->
  case catch rocksbdhandler_message_state_store_client:get(Payload, #{channel => ShardName}) of
    {ok, #{value := #{json_payload := JsonBin}}, _Header} ->
      DataMap       = json:decode(JsonBin),
      LongUrl       = maps:get(<<"long_url">>, DataMap),
      TrackLocation = maps:get(<<"track_location">>, DataMap),
      {LongUrl, TrackLocation, DataMap};
    _ ->
      error
  end.

send_request(Url, Body) ->
  Json        = json:encode(Body),
  Request     = {Url, ?HEADERS, "application/json", Json},
  SSL_Options = [{ssl, [{verify, none}]}],
  case httpc:request(post, Request, SSL_Options, []) of
    {ok, {{_, 200, _}, _, ResponseBody}}        ->
      ?oxlog_i("POST done to Mon by server type: ~p~n", [?SERVER_TYPE]),
      {ok, ResponseBody};
    {ok, {{_, StatusCode, _}, _, ResponseBody}} ->
      ?oxlog_e("POST request to mon failed with Status: ~p~n and Response: ~p~n", [StatusCode, ResponseBody]),
      {error, {StatusCode, ResponseBody}};
    {error, Reason}                             ->
      ?oxlog_e("POST request to mon failed with Reason: ~p~n", [Reason]),
      {error, Reason}
  end.

get_channel(ShortUrlData) ->
  ShortUrl = maps:get(<<"short_url">>, ShortUrlData, null),
  case string:find(binary_to_list(ShortUrl), "_1") of
    true -> "wa";
    _    ->
      case string:find(binary_to_list(ShortUrl), "_2") of
        true -> "rcs";
        _    -> "sms"
      end
  end.

get_odp_url(Type) ->
  OdpIp   = docker_func:get_odp_ip(),
  OdpPort = docker_func:get_odp_port(),
  lists:concat(["http://", OdpIp, ":", OdpPort, "/api/v1/", Type, "/get-long-url"]).

insert_in_clicker_queue(ShortUrlData, UaData) ->
  Uuid = maps:get(<<"uuid">>, ShortUrlData, undefined),
  NewShortUrlData = maps:put(<<"onex_msg_id">>, Uuid, ShortUrlData),
  clicker_aggregator:enqueue({NewShortUrlData, UaData}).

insert_in_clicker_queue(Key, Payload, UaData) ->
  ShortUrlData = maps:merge(Key, Payload),
  Channel = get_channel(ShortUrlData),
  NewShortUrlData = maps:put(<<"channel">>, Channel, ShortUrlData),
  clicker_aggregator:enqueue({NewShortUrlData, UaData}).

content_type() ->
  #{<<"content-type">> => <<"application/json; charset=utf-8">>}.
