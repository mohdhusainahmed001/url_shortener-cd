-module(shorturl_queue_worker).
-behaviour(gen_server).

-include("onex.hrl").

-define(CHECK_INTERVAL, 5000).           % 5 seconds
-define(THRESHOLD, 100000).              % 1 lakh
-define(BASE_QUEUE_PATH, "data/shorturl_queues").

-record(state, {
    q,
    domain,
    filler_pid
}).

-export([
    start_link/2,
    stop/1,
    enqueue/2,
    enqueue_batch/2,
    dequeue/1,
    dequeue_batch/2,
    len/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%% ---------------------- Public APIs ----------------------

start_link(Domain, FillerPid) ->
    WorkerName = domain_to_worker_name(Domain),
    gen_server:start_link({local, WorkerName}, ?MODULE, [Domain, FillerPid], []).

stop(Domain) ->
    WorkerName = domain_to_worker_name(Domain),
    gen_server:stop(WorkerName).

call(M, A) ->
  case whereis(M) of
    undefined ->
      ?oxlog_e("Queue worker ~p not running", [M]),
      {error, not_running};
    Pid ->
      gen_server:call(Pid, A)
  end.

enqueue(Domain, ShortCode) ->
    WorkerName = domain_to_worker_name(Domain),
    gen_server:cast(WorkerName, {enqueue, ShortCode}).

enqueue_batch(Domain, ShortCodes) when is_list(ShortCodes) ->
    WorkerName = domain_to_worker_name(Domain),
    gen_server:cast(WorkerName, {enqueue_batch, ShortCodes}).

dequeue(Domain) ->
    WorkerName = domain_to_worker_name(Domain),
    call(WorkerName, dequeue).

dequeue_batch(Domain, Count) ->
    WorkerName = domain_to_worker_name(Domain),
    call(WorkerName, {dequeue_batch, Count}).

len(Domain) ->
    WorkerName = domain_to_worker_name(Domain),
    call(WorkerName, len).

%%% ------------------- GenServer Callbacks ------------------

init([Domain, FillerPid]) ->
    process_flag(trap_exit, true),
    {ok, CWD} = file:get_cwd(),
    QueuePath = lists:concat([CWD, "/", ?BASE_QUEUE_PATH, "/", binary_to_list(Domain)]),
    filelib:ensure_dir(QueuePath ++ "/"),
    Q = replayq:open(#{dir => QueuePath, seg_bytes => 10000000}),
    
    % Start periodic check
    erlang:send_after(?CHECK_INTERVAL, self(), check_threshold),
    
    {ok, #state{q = Q, domain = Domain, filler_pid = FillerPid}}.

handle_call(len, _From, #state{q = Q} = State) ->
    {reply, replayq:count(Q), State};

handle_call(dequeue, _From, #state{q = Q} = State) ->
    case replayq:pop(Q, #{count_limit => 1}) of
        {NewQ, _AckRef, []} ->
            {reply, {error, empty}, State#state{q = NewQ}};
        {NewQ, AckRef, [Bin]} ->
            replayq:ack(NewQ, AckRef),
            ShortCode = binary_to_term(Bin),
            {reply, {ok, ShortCode}, State#state{q = NewQ}}
    end;

handle_call({dequeue_batch, Count}, _From, #state{q = Q} = State) when is_integer(Count), Count > 0 ->
    case replayq:pop(Q, #{count_limit => Count}) of
        {NewQ, _AckRef, []} ->
            {reply, {ok, []}, State#state{q = NewQ}};
        {NewQ, AckRef, BinList} ->
            replayq:ack(NewQ, AckRef),
            ShortCodes = [binary_to_term(Bin) || Bin <- BinList],
            {reply, {ok, ShortCodes}, State#state{q = NewQ}}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({enqueue, ShortCode}, #state{q = Q} = State) ->
    NQ = replayq:append(Q, [term_to_binary(ShortCode)]),
    {noreply, State#state{q = NQ}};

handle_cast({enqueue_batch, ShortCodes}, #state{q = Q} = State) ->
    BinList = [term_to_binary(SC) || SC <- ShortCodes],
    NQ = replayq:append(Q, BinList),
    {noreply, State#state{q = NQ}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_threshold, #state{q = Q, domain = Domain, filler_pid = FillerPid} = State) ->
    QLen = replayq:count(Q),
    if
        QLen < ?THRESHOLD ->
            % Notify filler to generate more short codes
            gen_server:cast(FillerPid, {fill_queue, Domain});
        true ->
            ok
    end,
    % Schedule next check
    erlang:send_after(?CHECK_INTERVAL, self(), check_threshold),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{q = Q, domain = Domain}) ->
    ?oxlog_e("Terminating queue worker for domain: ~p~n", [Domain]),
    replayq:close(Q),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% ------------------ Internal Utilities ------------------

domain_to_worker_name(Domain) ->
    list_to_atom("shorturl_queue_" ++ binary_to_list(Domain)).