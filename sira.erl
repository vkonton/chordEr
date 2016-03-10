-module(sira).
-export([hash_sort/1]).

hash_sort(Nds) ->
	case (Nds) of
		all ->
			Nodes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		Nodes ->
			Nodes
	end,
	Tuples = lists:map(fun(X) -> <<Id:160/integer>> = crypto:hash(sha, <<X>>) , {Id, X} end, Nodes),
	lists:sort(Tuples).
