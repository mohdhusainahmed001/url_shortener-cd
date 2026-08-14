-module(shorturl_filler).
-behaviour(gen_server).

-include("onex.hrl").

-define(CHECK_ALL_INTERVAL, 10000).      % 30 seconds
-define(THRESHOLD, 100000).               % 1 lakh
-define(BATCH_SIZE, 500000).              % 5 lakh

-record(state, {
    domains = [],
    redis_conn
}).

-export([
    start_link/1,
    stop/0
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

start_link(Domains) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Domains], []).

stop() ->
    gen_server:stop(?MODULE).

%%% ------------------- GenServer Callbacks ------------------

init([Domains]) ->
    process_flag(trap_exit, true),
    % Connect to Redis
    RedisHost  = docker_func:get_redis_host(),
    RedisPort  = list_to_integer(docker_func:get_redis_port()),
    RedisPass  = docker_func:get_redis_pass(),
    {ok, Conn} = eredis:start_link(RedisHost, RedisPort),
    {ok, _}    = eredis:q(Conn, ["AUTH", RedisPass]),
    erlang:send_after(?CHECK_ALL_INTERVAL, self(), check_all_queues),
    {ok, #state{domains = Domains, redis_conn = Conn}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({fill_queue, Domain}, State) ->
    spawn(fun() -> fill_queue_async(Domain, State#state.redis_conn) end),
    {noreply, State};

handle_cast({update_domains}, State) ->
	case ets:lookup(?DOMAIN_CACHE, all_domains) of
		[] ->
			{noreply, State};
		[{all_domains, Domains}] ->
			{noreply, State#state{domains = Domains}}
	end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_all_queues, #state{domains = Domains, redis_conn = RedisConn} = State) ->
    % Check all domain queues and fill if needed
    lists:foreach(fun(Domain) ->
        case catch shorturl_queue_worker:len(Domain) of
            Len when is_integer(Len), Len < ?THRESHOLD ->
                spawn(fun() -> fill_queue_async(Domain, RedisConn) end);
            _ ->
                ok
        end
    end, Domains),
    % Schedule next check
    erlang:send_after(?CHECK_ALL_INTERVAL, self(), check_all_queues),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{redis_conn = Conn}) ->
    ?oxlog_e("Terminating filler service~n", []),
    eredis:stop(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% ------------------ Internal Utilities ------------------

fill_queue_async(Domain, RedisConn) ->
	try
		% Check if queue really needs filling (avoid race condition)
		case catch shorturl_queue_worker:len(Domain) of
			Len when is_integer(Len), Len >= ?THRESHOLD ->
				?oxlog_i("Queue for domain ~p already filled, skipping~n", [Domain]),
				ok;
			_ ->
				RedisKey = "shorturl:counter:" ++ Domain,
				case eredis:q(RedisConn, ["INCR", RedisKey]) of
					{ok, CounterBin} ->
						Counter = binary_to_integer(CounterBin),
						StartRange = (Counter - 1) * ?BATCH_SIZE,
						EndRange = Counter * ?BATCH_SIZE - 1,
						ShortCodes = generate_short_codes(StartRange, EndRange),
						insert_in_batches(Domain, ShortCodes, 10000);
					{error, Reason} ->
						?oxlog_e("Redis error for domain ~p: ~p~n", [Domain, Reason]),
						{error, redis_failed}
				end
		end
	catch
		Error:Reason1 ->
			?oxlog_e("Error filling queue for domain ~p: ~p:~p~n", 
								[Domain, Error, Reason1]),
			{error, fill_failed}
	end.

generate_short_codes(Start, End) ->
    Max = 62*62*62*62*62*62,
    generate_short_codes(Start, End, Max, []).

generate_short_codes(Current, End, _Max, Acc) when Current >= End ->
    lists:reverse(Acc);

generate_short_codes(Current, End, Max, Acc) ->
    Wrapped = Current rem Max,
    ShortCode = integer_to_short_code(Wrapped),
    generate_short_codes(Current + 1, End, Max, [ShortCode | Acc]).

integer_to_short_code(N) ->
    Chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    Code = encode_base62(N, Chars, []),
    pad_to_length(Code, 6).

encode_base62(0, _Chars, []) ->
    "0";
encode_base62(0, _Chars, Acc) ->
    Acc;
encode_base62(N, Chars, Acc) ->
    Idx = (N rem 62) + 1,
    Char = lists:nth(Idx, Chars),
    encode_base62(N div 62, Chars, [Char | Acc]).

pad_to_length(Code, TargetLen) ->
    PaddingNeeded = TargetLen - length(Code),
    if
        PaddingNeeded > 0 ->
            lists:duplicate(PaddingNeeded, $0) ++ Code;
        true ->
            Code
    end.

insert_in_batches(_Domain, [], _BatchSize) ->
    ok;
insert_in_batches(Domain, ShortCodes, BatchSize) ->
    {Batch, Rest} = if
        length(ShortCodes) > BatchSize ->
            lists:split(BatchSize, ShortCodes);
        true ->
            {ShortCodes, []}
    end,
    shorturl_queue_worker:enqueue_batch(Domain, Batch),
    insert_in_batches(Domain, Rest, BatchSize).