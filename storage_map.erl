-module(storage_map).
-export([create/0,
	 add/4,
	 lookup/3,
	 full_lookup/2,
	 find_value/2,
	 merge_storage/1,
	 delete_key/3,
	 delete_node_storage/2,
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


merge_storage(Store) ->
  Ms = [X || {_,X} <- maps:to_list(Store)],
  lists:foldl(fun(X, Y) -> maps:merge(X,Y) end, #{}, Ms).

full_lookup(Key, Store) ->
  Merged = merge_storage(Store),
  find_value(Key, Merged).

find_value(Key, L) ->
  case maps:find(Key, L) of
    {ok, Value} ->
      Value;
    error ->
      not_found
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


delete_node_storage(Metadata, Store) ->
  maps:remove(Metadata, Store).

delete_key(Metadata, Key, Store) ->
  {ok, Xs} = maps:find(Metadata, Store),
  NewXs = maps:remove(Key, Xs),
  maps:update(Metadata, NewXs, Store).
  

split(Metadata, From, To, Store) ->
  case maps:find(Metadata, Store) of
    {ok, Xs} ->
      Ys = maps:to_list(Xs),
      {KeepL, RestL} = lists:partition(fun({Key, _}) -> not (node:between(Key, From, To)) end, Ys),
      {maps:from_list(KeepL), maps:from_list(RestL)};
    error ->
      not_found
  end.


merge(Store1, Store2) ->
  maps:merge(Store1, Store2).
