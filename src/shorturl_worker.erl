-module(shorturl_worker).
-behaviour(gen_server).
-include("onex.hrl").
-export([start_link/1, stop/1, produce_message/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%=================================================================================
%%% API
%%%=================================================================================
start_link(Index) ->
  ServerName = list_to_atom("shorturl_worker" ++ integer_to_list(Index)),
  gen_server:start_link({local, ServerName}, ?MODULE, [Index], []).

stop(Index) ->
  ServerName = list_to_atom("shorturl_worker" ++ integer_to_list(Index)),
  gen_server:cast(ServerName, stop).

%%%=================================================================================
%%% GENSERVER CALLBACKS
%%%=================================================================================
init([Index]) ->
  {ok, #{index => Index}}.

handle_call(_Msg, _From, State) ->
  {reply, invalid_request, State}.

handle_cast({process_batch, Recs}, State) ->
  %% Process batch of records (send all to Kafka)
  send_batch_to_kafka(Recs),
  {noreply, State};
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%=================================================================================
%%% INTERNAL FUNCTIONS
%%%=================================================================================
send_batch_to_kafka(Recs) ->
  lists:foreach(
    fun(Msg) ->
      case catch kafka_producer:create_shorturl_payload(Msg) of
        {'EXIT', _} ->
          shorturl_queue:enqueue(Msg);
        {Key, {Payload, Topic, Client}} ->
          insert_in_rocksdb(Msg),
          produce_message(Client, Key, Topic, Payload)
      end
    end,
    Recs
  ).

produce_message(Client, Key, Topic, Message) ->
  case brod:start_producer(Client, Topic, []) of
    ok ->
      case catch <<Message/binary>> of
        {'EXIT', _} ->
          ?oxlog_e("brod:start_producer with error ~p, ~p~n", [Topic, Message]);
        Value ->
          Partition = random, %% Default partition
          case brod:produce(Client, Topic, Partition, Key, Value) of
            {ok, _} ->
              case persistent_term:get(counterRef, undefined) of
                undefined ->
                  ok;
                CounterRef ->
                  counters:add(CounterRef, 1, 1)
              end,
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

insert_in_rocksdb(Msg) ->
  ShortUrl = maps:get(<<"short_url">>, Msg),
  Payload = #{
    key => #{
      channel    => <<"shorturl">>,
      identifier => ShortUrl,
      msg_uuid   => ShortUrl
    },
    value => #{
      json_payload => json:encode(Msg),
      if_absent    => false
    }
  },

  ChannelName = util:get_shard_name(ShortUrl),
  insert_in_shard(Payload, ChannelName),
  BackupChannels = util:get_backup_channels(),
  lists:foreach(fun(Channel) ->
    insert_in_shard(Payload, Channel)
  end, BackupChannels).

insert_in_shard(Payload, ChannelName) ->
  case catch rocksbdhandler_message_state_store_client:put(Payload, #{channel => ChannelName}) of
    {ok, _, _} ->
      ok;
    {error, Reason, _Metadata} ->
      ?oxlog_e("Unable to insert in RocksDb Shard:- ~p", [Reason]);
    {error, Reason} ->
      ?oxlog_e("Unable to insert in RocksDb Shard:- ~p", [Reason]);
    {'EXIT', ErrReason} ->
      ?oxlog_e("Failed to insert in RocksDb Shard due to :- ~p", [ErrReason]);
    Any ->
      ?oxlog_e("Unknown error while inserting in RocksDb Shard:- ~p", [Any])
  end.

%%%=================================================================================
%%% END
%%%=================================================================================
