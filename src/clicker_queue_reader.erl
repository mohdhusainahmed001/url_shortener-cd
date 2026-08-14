-module(clicker_queue_reader).

-behaviour(gen_server).

-include("onex.hrl").

-export([start_link/1, stop/0, produce_message/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(EMPTY_QUEUE_INTERVAL, 1000).
-define(QUEUE_INTERVAL, 100).
-define(BATCH_SIZE, 300).

%%%=================================================================================
%%%                                     API
%%%=================================================================================

start_link(Index) ->
  ServerName = list_to_atom("clicker_queue_reader" ++ integer_to_list(Index)),
  ?oxlog_i("Starting clicker queue reader with Index:- ~p", [Index]),
  gen_server:start_link({local, ServerName}, ?MODULE, [], []).

stop() ->
  gen_server:cast(?MODULE, stop).

%%%=================================================================================
%%%                            GENSERVER  CALL BACKS
%%%=================================================================================

init([]) ->
  erlang:send_after(10, self(), queue_reader),
  {ok, []}.

handle_call(_Msg, _From, State) ->
  {reply, invalid_request, State}.

handle_cast(stop, State) ->
  {stop, normal, State};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(queue_reader, State) ->
  %%?oxlog_e("*******Queue reader dequeue called**********",[]),
  case clicker_aggregator:dequeue_batch(?BATCH_SIZE) of
    {ok, []} ->
      erlang:send_after(?EMPTY_QUEUE_INTERVAL, self(), queue_reader);
    {ok, Recs} ->
      send_to_kafka(Recs),
      erlang:send_after(?QUEUE_INTERVAL, self(), queue_reader)
  end,
  {noreply, State};

handle_info(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%=================================================================================
%%%                             INTERNAL FUNCTIONS
%%%=================================================================================

send_to_kafka(Recs) ->
  lists:foreach(
    fun(Msg) ->
      case catch kafka_producer:create_clicker_payload(Msg) of
        {'EXIT', _} ->
          clicker_aggregator:enqueue(Msg);
        {Key, {Payload, Topic, Client}} ->
          send_callback(Msg),
          produce_message(Client, Key, Topic, Payload)
      end
    end,
    Recs
  ).

get_endpoint({ShortUrlData,UaData}) ->
  Channel = maps:get(<<"channel">>, ShortUrlData, null ),
  case Channel of
    <<"rcs">> ->
      "/send_dlr_rcs";
    _         ->
      "/send_dlr_wa"
  end.



send_callback(Msg) ->
  try
    {ShortUrlData, UaData} = Msg,
    Payload = maps:merge(ShortUrlData, maps:merge(#{<<"callback_url_type">> => <<"url_click">>},(UaData))),
    case registry_cache:get_node(<<"dlr_sender">>) of
      {error, Reason} ->
        ?oxlog_e("Failed to get DLR Sender Node:- ~p~n", [Reason]);
      NodeData ->
        {Ip, Port} = {maps:get(<<"ip">>, NodeData), maps:get(<<"port">>, NodeData)},
        HttpUrl = lists:concat(["http://", Ip, ":", Port, get_endpoint(Msg)]),
        case http_client:send_request(HttpUrl, Payload) of
          {ok, _Response} ->
            ?oxlog_i("Successfully sent callback to DLR Sender for short URL: ~p~n", [Payload]);
          {error, Reason} ->
            ?oxlog_e("Failed to send callback to DLR Sender for short URL: ~p with Reason: ~p~n",
                      [Payload, Reason])
        end
    end
  catch
    _:ErrReason ->
      ?oxlog_e("Exception occurred while sending callback: ~p~n", [ErrReason])
  end.


%send_to_kafka(Recs) ->
% lists:foreach(
%    fun(Msg) ->
%      {ShortUrlData, _} = Msg,
%      Channel           = maps:get(<<"channel">>, ShortUrlData, null),
%      ?oxlog_e("**************Channel**************~p~n",[Channel]),
%      case Channel of 
%      <<"wa">> ->
%                send_wa_to_kafka(Msg);
%      _->       send_rcs_to_kafka(Msg)  
%      end
%    end,
%    Recs
%  ).

%send_wa_to_kafka(Msg) ->
%	 ?oxlog_e("**************wa Channel enqueue**************~p",[Msg]),
%      case catch kafka_producer:create_clicker_payload(Msg) of
%        {'EXIT', _} ->
%	 ?oxlog_e("**************wa Channel enqueue**************~",[]),
%          clicker_aggregator:enqueue(Msg);
%        {Key, {Payload, Topic, Client}} ->
%	  ?oxlog_e("**************send_wa_to_kafka **************~p~n",[Payload]),
%          produce_message(Client, Key, Topic, Payload)
%      end.

%send_rcs_to_kafka(Msg) ->
%      case catch kafka_producer:create_clicker_payload(Msg) of
%        {'EXIT', _} ->
%          clicker_aggregator:enqueue(Msg);
%        {Key, {Payload, Topic, Client}} ->
%          produce_message(Client, Key, Topic, Payload)
%	end.

produce_message(Client, Key, Topic, Message) ->
  case brod:start_producer(Client, Topic, []) of
    ok ->
      case catch <<Message/binary>> of
    {'EXIT', _} ->
      ?oxlog_e("brod:start_producer with error ~p, ~p~n", [Topic, Message]);
    Value ->
      Partition = random,  %% Default partition
      case brod:produce(Client, Topic, Partition, Key, Value) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            kafka_backup_queue:enqueue({Message, Topic, Client, Key}),
            ?oxlog_e("Failed to send message to Kafka topic ~s: ~p~n", [Topic, Reason])
      end
    end;
    {error, Reason} ->
      kafka_backup_queue:enqueue({Message, Topic, Client, Key}),
      ?oxlog_e("Failed to start producer: ~p~n", [Reason])
  end.
%%%=================================================================================
%%%                                     END
%%%=================================================================================
