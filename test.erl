-module(test).
-export([make_ring/1,
	 parse/1,
	 parse_queries/1,
	 parse_requests/1,
	 test_inserts/1,
	 test_inserts/2,
	 test_queries/1,
	 test_requests/1,
	 test_storage_length/1,
	 pretty_print/1,
	 write/2,
	 clr/0
	]).

make_ring(N) ->
  P0 = node:join(0),
  T = lists:foldl(fun (X, Acc) -> H = timer:sleep(1000), node:join(X, P0),  [H|Acc] end, [], lists:seq(1, N-1)),
  T ++ [P0].

test_inserts(Peer) ->
  test_inserts("insert.txt", Peer).

test_inserts(Filename, Peer) ->
  L = parse(Filename),
  [node:store(Key, Val, Peer) || {Key, Val} <- L].


test_requests(Peer) ->
  L = parse_requests("requests.txt"),
  [exec_request(Req, Peer) || Req <- L].

exec_request(["insert", Key, Value], Peer) ->
  IntVal = string:to_integer(Value),
  node:store(Key, IntVal, Peer);
exec_request(["query", Key], Peer) ->
  node:locate(Key, Peer).


test_queries(Peer) ->
  L = parse_queries("query.txt"),
  [node:locate(X, Peer) || X <- L].


parse(Filename) ->
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
  [string:tokens(X, ",")|| X <-L].



test_storage_length(Peer) ->
  [length(maps:to_list(X)) || {_, _, X} <- node:locate("*", Peer)].

clr() -> io:format("\033[2J").


write(Filename, Data) ->
  file:write_file(Filename, io_lib:fwrite("~p.\n", [Data])).

split_it(L) -> split_it(L, [], []).

split_it([], Keys, Values) ->
  {Keys, Values};
split_it([X, Y|Xs], Keys, Values) ->
  {K, _} = string:to_integer(Y),
  split_it(Xs, [K|Keys], [X|Values]).

pretty_print(AllStores) ->
  [{Pid, Id, bucket_pretty_print(maps:to_list(HshTbl))} || {Id, Pid, HshTbl} <- AllStores].

bucket_pretty_print(Bucket) ->
  [{Id, del_keys_from_hash(MiniHshTbl)} || {Id, MiniHshTbl} <- Bucket].

del_keys_from_hash(Hash) ->
  case [Y || {_X, Y} <- maps:to_list(Hash)] of
    [] ->
      no_data;
    X ->
      hd(X)
  end.
