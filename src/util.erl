-module(util).

-include("onex.hrl").

-export([
    get_auth_ip_port/0,
    get_warcsauth_ip_port_with_path/1,
    normalize_url/1,
    add_protocols_to_domain/1,
    get_shard_name/1,
    get_backup_channels/0,
    get_grpc_channels/0,
    map_to_json/1
]).

map_to_json(Map) ->
  try json:encode(Map) of
    EncodedMap -> {ok, EncodedMap}
  catch
    _:_ -> {error, <<"bad_map">>}
  end.

get_backup_channels() ->
  Channels = get_all_grpc_channels(),
  lists:foldl(fun({Name, _Endpoints, _Opts}, Acc) ->
      NameStr = atom_to_list(Name),
      case string:find(string:lowercase(NameStr), "backup") of
        nomatch ->
          Acc;
        _       ->
          [Name | Acc]
      end
  end, [], Channels).

get_shard_name(ShortUrl) ->
  AllChannels = get_grpc_channels(),
  ShardIndex = get_shard_index(ShortUrl, length(AllChannels)),
  case length(AllChannels) of
    0 ->
      default_channel;
    _ ->
      {ChannelName, _, _} = lists:nth(ShardIndex + 1, AllChannels),
      ChannelName
  end.

get_all_grpc_channels() ->
   case application:get_env(grpcbox, client) of
    {ok, ChannelsMap} ->
      maps:get(channels, ChannelsMap, []);
    _ ->
      []
  end.

get_grpc_channels() ->
  case application:get_env(grpcbox, client) of
    {ok, ChannelsMap} ->
      Channels = maps:get(channels, ChannelsMap, []),
      lists:foldl(fun({Name, Endpoints, Opts}, Acc) ->
        NameStr = atom_to_list(Name),
        case string:find(string:lowercase(NameStr), "backup") of
          nomatch ->
            [{Name, Endpoints, Opts} | Acc];
          _       ->
            Acc
        end
      end, [], Channels);
    _ ->
      []
  end.

get_shard_index(ShortUrl, ShardCount) when is_binary(ShortUrl) ->
    Shortcode = extract_shortcode(ShortUrl),
    Base62Number = base62_to_integer(Shortcode),
    Base62Number rem ShardCount.

extract_shortcode(ShortUrl) when is_binary(ShortUrl) ->
    %% Split by "/" and get the last part
    Parts = binary:split(ShortUrl, <<"/">>, [global]),
    lists:last(Parts).

base62_to_integer(Base62Binary) when is_binary(Base62Binary) ->
    base62_to_integer(binary_to_list(Base62Binary), 0).

base62_to_integer([], Acc) ->
    Acc;
base62_to_integer([$_ | _], Acc) ->
    Acc;
base62_to_integer([Char | Rest], Acc) ->
    Value = char_to_base62_value(Char),
    base62_to_integer(Rest, Acc * 62 + Value).

char_to_base62_value(Char) when Char >= $0, Char =< $9 ->
    Char - $0;
char_to_base62_value(Char) when Char >= $A, Char =< $Z ->
    Char - $A + 10;
char_to_base62_value(Char) when Char >= $a, Char =< $z ->
    Char - $a + 36;
char_to_base62_value(_) ->
    0.

add_protocols_to_domain(DomainName) ->
    case binary:split(DomainName, <<"://">>, [global]) of
        [Domain] ->
        {<<"http://", Domain/binary>>, <<"https://", Domain/binary>>};
        _ ->
        {DomainName}
    end.

get_warcsauth_ip_port_with_path(Path) ->
  case registry_cache:get_node(<<"authapi_warcs">>) of
     {error, Reason}          ->
      ?oxlog_e("Failed to get Auth IP and PORT:- ~p~n", [Reason]),
      {error, Reason};
    NodeData ->
      {Ip, Port} = {maps:get(<<"ip">>, NodeData), maps:get(<<"port">>, NodeData)},
      lists:concat(["http://", Ip, ":", Port, Path])
  end.

get_auth_ip_port() ->
  case registry_cache:get_node(<<"auth">>) of
    {error, Reason}          ->
      ?oxlog_e("Failed to get Auth IP and PORT:- ~p~n", [Reason]),
      {error, Reason};
    NodeData ->
      {maps:get(<<"ip">>, NodeData), maps:get(<<"port">>, NodeData)}
  end.

normalize_url(Url) when is_list(Url) ->
    list_to_binary(Url);
normalize_url(Url) when is_binary(Url) ->
    % Remove whitespaces with binary pattern matching
    NoSpaces = remove_whitespaces(Url),
    % Remove trailing slashes
    remove_trailing_slashes(NoSpaces);
normalize_url(_) ->
    <<>>.

remove_whitespaces(<<>>) -> 
    <<>>;
remove_whitespaces(<<$\s, Rest/binary>>) ->
    remove_whitespaces(Rest);
remove_whitespaces(<<$\t, Rest/binary>>) ->
    remove_whitespaces(Rest);
remove_whitespaces(<<$\n, Rest/binary>>) ->
    remove_whitespaces(Rest);
remove_whitespaces(<<$\r, Rest/binary>>) ->
    remove_whitespaces(Rest);
remove_whitespaces(<<Char, Rest/binary>>) ->
    <<Char, (remove_whitespaces(Rest))/binary>>.

remove_trailing_slashes(<<>>) ->
    <<>>;
remove_trailing_slashes(Url) ->
    case binary:last(Url) of
        $/ -> 
            Size = byte_size(Url) - 1,
            <<Rest:Size/binary, $/>> = Url,
            remove_trailing_slashes(Rest);
        _ -> 
            Url
    end.