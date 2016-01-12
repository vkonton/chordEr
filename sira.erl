-module(sira).
-export([hash_sort/1]).

hash_sort(Nds) ->
	case (Nds) of
		all ->
			Nodes = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
		Nodes ->
			Nodes
	end,
	Tuples = [{crypto:hash(sha, X), X} || X <- Nodes],
	lists:sort(Tuples).
