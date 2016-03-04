-module(storage_map).
-export([create/0,
	 add/4,
	 lookup/3,
	 delete/2,
	 change_metadata/3,
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
      Xs = #{}
  end,
  Nxs = maps:put(Key, Value, Xs),
  maps:put(Root, Nxs, Store).


change_metadata(OldMeta, NewMeta, Store) ->
  case maps:find(OldMeta, Store) of
    {ok, Xs} ->
      Nstore = maps:remove(OldMeta, Store),
      maps:put(NewMeta, Xs, Nstore);
    error ->
      Store
  end.


lookup(Root, Key, Store) ->
  case maps:find(Root, Store) of
    % root node has data Xs, search for Key in Xs.
    {ok, Xs} ->
      case maps:find(Key, Xs) of
	{Key, Value} ->
	  Value;
	false ->
	  not_found
      end;
    % root node does not exist in the map.
    error ->
      not_found
  end.


delete(Root, Store) ->
  maps:remove(Root, Store).


split(Root, From, To, Store) ->
  case maps:find(Root, Store) of
    {ok, Xs} ->
      lists:partition(fun({Key, _}) -> not (node:between(Key, From, To)) end, Xs);
    error ->
      not_found
  end.


merge(Store1, Store2) ->
  maps:merge(Store1, Store2).
