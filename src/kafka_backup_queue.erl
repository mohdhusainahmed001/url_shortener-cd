-module(kafka_backup_queue).
-behaviour(gen_server).
-include("onex.hrl").

-define(QUEUE_READER_COUNT, 3).
-define(BASE_QUEUE_PATH, "data/onexq").
-define(BATCH_SIZE, 300).
-define(QUEUE_INTERVAL, 100).
-define(EMPTY_QUEUE_INTERVAL, 1000).

-record(state, {q, queue_name}).

-export([
  stop/0, start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  enqueue/1, enqueue_batch/1, len/0, dequeue_batch/1]).

%%% ---------------------- Public APIs ----------------------

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
  gen_server:stop(?MODULE).

enqueue(Payload) ->
  gen_server:cast(?MODULE, {enqueue, Payload}).

enqueue_batch(Payloads) when is_list(Payloads) ->
  gen_server:cast(?MODULE, {enqueue_batch, Payloads}).

call(M, A) ->
  case whereis(M) of
    undefined ->
      ?oxlog_e("Kafka backup queue ~p not running", [M]),
      {error, not_running};
    Pid ->
      gen_server:call(Pid, A)
  end.

len() ->
  call(?MODULE, len).

dequeue_batch(Count) ->
  call(?MODULE, {dequeue_batch, Count}).

%%% ------------------- GenServer Callbacks ------------------

init([]) ->
  process_flag(trap_exit, true),
  QueueName = "kafka_backup_queue",
  {ok, CWD} = file:get_cwd(),
  QueuePath = lists:concat([CWD, "/", ?BASE_QUEUE_PATH, "/", QueueName]),
  filelib:ensure_dir(QueuePath),
  Q = replayq:open(#{dir => QueuePath, seg_bytes => 10000000}),
  erlang:send_after(10, self(), queue_reader),
  {ok, #state{q = Q, queue_name = QueueName}}.

handle_call(stop, _From, State) ->
  {stop, normal, stopped, State};

handle_call(len, _From, #state{q = Q} = State) ->
  {reply, replayq:count(Q), State};

handle_call({dequeue_batch, Count}, _From, #state{q = Q} = State) when is_integer(Count), Count > 0 ->
  case replayq:pop(Q, #{count_limit => Count}) of
    {NewQ, _AckRef, []}     ->
      {reply, {ok, []}, State#state{q = NewQ}};
    {NewQ, AckRef, BinList} ->
      replayq:ack(NewQ, AckRef),
      Msgs = [binary_to_term(Bin) || Bin <- BinList],
      {reply, {ok, Msgs}, State#state{q = NewQ}}
  end;

handle_call(_Request, _From, State) ->
 {reply, ok, State}.

handle_cast({enqueue, Payload}, #state{q = Q} = State) ->
  NQ = replayq:append(Q, [term_to_binary(Payload)]),
  {noreply, State#state{q = NQ}};

handle_cast({enqueue_batch, Payloads}, #state{q = Q} = State) ->
  BinList = [term_to_binary(P) || P <- Payloads],
  NQ = replayq:append(Q, BinList),
  {noreply, State#state{q = NQ}};

handle_cast(_Msg, State) ->
  {noreply, State}.

 handle_info(queue_reader, #state{q = Q} = State) ->
  case replayq:pop(Q, #{count_limit => 1}) of
    {NewQ, _AckRef, []}     ->
      erlang:send_after(?EMPTY_QUEUE_INTERVAL, self(), queue_reader),
      {noreply, State#state{q = NewQ}};
    {NewQ, AckRef, [Bin]} ->
      replayq:ack(NewQ, AckRef),
      try
        {Message, Topic, Client, Key} = binary_to_term(Bin),
        case Client of
          clicker  ->
            clicker_queue_reader:produce_message(Client, Key, Topic, Message);
          shorturl ->
            shorturl_worker:produce_message(Client, Key, Topic, Message)
        end
      catch
        _:Reason ->
          ?oxlog_e("Unable to Produce Message enqueing in backup queue ~p", [Reason]),
          kafka_backup_queue:enqueue(binary_to_term(Bin))
      end,
      erlang:send_after(?QUEUE_INTERVAL, self(), queue_reader),
      {noreply, State#state{q = NewQ}}
  end;

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{q = Q, queue_name = QueueName}) ->
  ?oxlog_e("Terminating shorturl_queue ~p~n", [QueueName]),
  replayq:close(Q),
  ok.