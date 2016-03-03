-module(storage_map).
-export([create/0,
	 add/4,
	 lookup/3,
	 split/4,
	 merge/2]).

create() ->
  #{}.

add(Root, Key, Value, Store) ->
  case maps:find(Root, Store) of
    % root node has already data Xs
    {ok, Xs} ->
      Xs;
    % no data in map for root node, we must create a storage for root.
    error ->
      Xs = []
  end,
  maps:put(Root, [{Key, Value} | Xs] , Store).

lookup(Root, Key, Store) ->
  case maps:find(Root, Store) of
    % root node has data Xs, search for Key in Xs.
    {ok, Xs} ->
      case lists:keyfind(Key, 1, Xs) of
	{Key, Value} ->
	  Value;
	false ->
	  not_found
      end;
    % root node does not exist in the map.
    error ->
      not_found
  end.

split(Root, From, To, Store) ->
  case maps:find(Root, Store) of
    {ok, Xs} ->
      lists:partition(fun({Key, _}) -> not (node:between(Key, From, To)) end, Xs);
    error ->
      not_found
  end.

merge(Store1, Store2) ->
  maps:merge(Store1, Store2).
