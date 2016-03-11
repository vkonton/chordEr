-module(test).
-export([start_nodes/3,
	 make_ring/1,
	 parse_inserts/1,
	 parse_queries/1,
	 parse_requests/1,
	 test_inserts/1,
	 test_inserts/2,
	 test_queries/1,
	 test_queries/2,
	 test_removes/1,
	 test_removes/2,
	 test_requests/1,
	 test_requests/2,
	 test_storage_length/1,
	 pretty_print/2,
	 write/2,
	 output_stores/2,
	 take/2,
	 clr/0]).


start_nodes(From, To, Peer) ->
  [wait_join(X, Peer) || X <-lists:seq(From, To)].


make_ring(N) ->
  P0 = dht:join(0),
  [wait_join(X,P0) || X <- lists:seq(1, N-1)].


wait_join(X,P0) ->
  timer:sleep(1000),
  dht:join(X, P0).


test_inserts(Peers) ->
  test_inserts("insert.txt", Peers).
test_inserts(Filename, Peers) ->
  L = parse_inserts(Filename),
  T0 = erlang:monotonic_time(micro_seconds),
  Res = [random_store(Key, Val, Peers) || {Key, Val} <- L],
  T1= erlang:monotonic_time(micro_seconds),
  TimeElapsed = T1-T0,
  io:format("Time elapsed in μSeconds: ~p ~n", [TimeElapsed]),
  io:format("Write Throughtput: ~pμS/Write~n", [TimeElapsed/length(L)]),
  io:format("~p ~n", [Res]).

test_queries(Peers) ->
  test_queries("query.txt", Peers).
test_queries(Filename, Peers) ->
  L = parse_queries(Filename),
  T0 = erlang:monotonic_time(micro_seconds),
  Res = [random_query(X, Peers) || X <- L],
  T1= erlang:monotonic_time(micro_seconds),
  TimeElapsed = T1-T0,
  io:format("Time elapsed in μSeconds: ~p ~n", [TimeElapsed]),
  io:format("Read Throughtput: ~pμS/Read~n", [TimeElapsed/length(L)]),
  io:format("~p ~n", [Res]).

test_requests(Peers) ->
  test_requests("requests.txt", Peers).
test_requests(Filename, Peers) ->
  L = parse_requests(Filename),
  Res = [exec_request(Req, Peers) || Req <- L],
  io:format("~p ~n", [Res]).

test_removes(Peers) ->
  test_removes("remove.txt", Peers).
test_removes(Filename, Peers) ->
  L = parse_queries(Filename),
  T0 = erlang:monotonic_time(micro_seconds),
  Res = [random_remove(X, Peers) || X <- L],
  T1= erlang:monotonic_time(micro_seconds),
  TimeElapsed = T1-T0,
  io:format("Time elapsed in μSeconds: ~p ~n", [TimeElapsed]),
  io:format("Read Throughtput: ~pμS/Read~n", [TimeElapsed/length(L)]),
  io:format("~p ~n", [Res]).


exec_request(["insert", Key, Value], Peers) ->
  {IntVal, _} = string:to_integer(Value),
  random_store(Key, IntVal, Peers);
exec_request(["query", Key], Peers) ->
  random_query(Key, Peers).


random_store(Key, Val, Peers) ->
  R = random:uniform(length(Peers)),
  RandomPeer = lists:nth(R, Peers),
  dht:store(Key, Val, RandomPeer).


random_query(Key, Peers) ->
  R = random:uniform(length(Peers)),
  RandomPeer = lists:nth(R, Peers),
  dht:locate(Key, RandomPeer).


random_remove(Key, Peers) ->
  R = random:uniform(length(Peers)),
  RandomPeer = lists:nth(R, Peers),
  dht:remove(Key, RandomPeer).


parse_inserts(Filename) ->
  {ok, Data} = file:read_file(Filename),
  D = binary_to_list(Data),
  Xs = string:tokens(D,",\n"),
  {Keys, Values} = split_it(Xs),
  lists:zip(Values, Keys).


parse_queries(Filename) ->
  {ok, Data} = file:read_file(Filename),
  D = binary_to_list(Data),
  string:tokens(D,"\n").


parse_requests(Filename) ->
  {ok, Data} = file:read_file(Filename),
  D = binary_to_list(Data),
  L = string:tokens(D,"\n"),
  [string:tokens(X, ",") || X <-L].


test_storage_length(Peer) ->
  [length(maps:to_list(X)) || {_, _, X} <- dht:locate("*", Peer)].


write(Filename, Data) ->
  file:write_file(Filename, io_lib:fwrite("~p.\n", [Data])).


split_it(L) -> split_it(L, [], []).
split_it([], Keys, Values) ->
  {Keys, Values};
split_it([X, Y|Xs], Keys, Values) ->
  {K, _} = string:to_integer(Y),
  split_it(Xs, [K|Keys], [X|Values]).


pretty_print(Peer, N) ->
  AllStores = dht:locate("*", Peer),
  L = [{Pid, Id, bucket_pretty_print(maps:to_list(HshTbl), N)} || {Id, Pid, HshTbl} <- AllStores],
  io:format("~p ~n", [L]).

output_stores(Filename, Peer) ->
  AllStores = dht:locate("*", Peer),
  L = [{Pid, Id, bucket_pretty_print(maps:to_list(HshTbl), all)} || {Id, Pid, HshTbl} <- AllStores],
  write(Filename, L).

bucket_pretty_print(Bucket, N) ->
  [{Id, del_keys_from_hash(MiniHshTbl, N)} || {Id, MiniHshTbl} <- Bucket].


del_keys_from_hash(Hash, N) ->
  case [Y || {_X, Y} <- maps:to_list(Hash)] of
    [] ->
      no_data;
    X ->
      case N of
	all ->
	  X;
	N ->
	  take(N,X)
      end
  end.

take(_, []) -> [];
take(0, _) -> [];
take(N, [H|T]) when N > 0 ->
  [H|take(N-1, T)].

clr() -> io:format("\033[2J").
