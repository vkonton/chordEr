-module(test).
-export([make_ring/1,
	 parse/1,
	 test_inserts/1,
	 test_inserts/2,
	 test_storage_length/1,
	 pretty_print/1,
	 clr/0
	]).

make_ring(N) ->
  P0 = node:join(0),
  T = lists:foldl(fun (X, Acc) -> H = node:join(X, P0),  [H|Acc] end, [], lists:seq(1, N-1)),
  T ++ [P0].

test_inserts(N) ->
  test_inserts("insert.txt", N).

test_inserts(Filename, N) ->
  [H | _] = make_ring(N),
  timer:sleep(26000),
  L = parse(Filename),
  [node:store(Key, Val, H) || {Key, Val} <- L].

test_storage_length(Peer) ->
  [length(maps:to_list(X)) || {_, _, X} <- node:locate("*", Peer)].

clr() -> io:format("\033[2J").

parse(Filename) ->
  {ok, Data} = file:read_file(Filename),
  D = binary_to_list(Data),
  Xs = string:tokens(D,",\n"),
  {Keys, Values} = split_it(Xs),
  lists:zip(Values, Keys).

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
