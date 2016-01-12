-module(storage).
-export([create/0,
	 add/3,
	 lookup/2,
	 split/3,
	 merge/2]).

create() ->
  [].

add(Key, Value, Store) ->
  [{Key, Value} | Store].

lookup(Key, Store) ->
  case lists:keyfind(Key, 1, Store) of
    {Key, Value} ->
      Value;
    false ->
      not_found
  end.

split(From, To, Store) ->
  lists:partition(fun({Key, _}) -> not (node:between(Key, From, To)) end, Store).

merge(Store1, Store2) ->
  Store1 ++ Store2.
