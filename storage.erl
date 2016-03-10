-module(storage).
-export([create/0,
	 add/4,
	 delete/3,
	 lookup/3,
	 full_lookup/2,
	 find_value/2,
	 add_bucket/3,
	 put_bucket/3,
	 get_bucket/2,
	 delete_bucket/2,
	 merge_buckets/3,
	 split/3]).


create() ->
  #{}.

%%-----------------------------------------------------------------------------
%% Element Functions.

%% adds {key, value} to bucket with Id == Root.
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

delete(Metadata, Key, Store) ->
  {ok, Xs} = maps:find(Metadata, Store),
  NewXs = maps:remove(Key, Xs),
  maps:update(Metadata, NewXs, Store).

lookup(Root, Key, Store) ->
  case maps:find(Root, Store) of
    % root node has data Xs, search for Key in Xs.
    {ok, Xs} ->
      case maps:find(Key, Xs) of
	{ok, Value} ->
	  Value;
	false ->
	  not_found
      end;
    % root node does not exist in the map.
    error ->
      not_found
  end.


full_lookup(Key, Store) ->
  Merged = merge_storage(Store),
  find_value(Key, Merged).


merge_storage(Store) ->
  Ms = [X || {_,X} <- maps:to_list(Store)],
  lists:foldl(fun(X, Y) -> maps:merge(X,Y) end, #{}, Ms).


find_value(Key, L) ->
  case maps:find(Key, L) of
    {ok, Value} ->
      Value;
    error ->
      not_found
  end.



%%-----------------------------------------------------------------------------
%% Bucket Functions
%%
%% A Bucket is the hashtable which contains all the {key,value} a node is
%% responsible for. A node's store contains several bucket (one for each
%% node


%% adds a bucket to the store.
%% if there is already a bucket with the same Metadata in the store we update
%% it with a bucket having the union of the two.
add_bucket(Metadata, Bucket, Store) ->
  case maps:find(Metadata, Store) of
    {ok, Xs} ->
      Xs;
    error ->
      Xs = #{}
  end,
  NB = maps:merge(Bucket, Xs),
  maps:put(Metadata, NB, Store).


%% puts bucket in store.
%% OVERWRITES previous Buckets with the same Metadata.
put_bucket(Metadata, Bucket, Store) ->
  maps:put(Metadata, Bucket, Store).


delete_bucket(Metadata, Store) ->
  maps:remove(Metadata, Store).


%% returns the bucket with Key == Metadata.
get_bucket(Metadata, Store) ->
  case maps:find(Metadata, Store) of
    % root node has already data Xs
    {ok, Xs} ->
      Xs;
    % no data in map for root node, we must create a storage for root.
    error ->
      #{}
  end.

merge_buckets(OldMeta, NewMeta, Store) ->
  case maps:find(OldMeta, Store) of
    {ok, Xs} ->
      Xs;
      error ->
      Xs = #{}
  end,
  case maps:find(NewMeta, Store) of
    {ok, Ys} ->
      Ys;
      error ->
      Ys = #{}
  end,
  MergedBucket = maps:merge(Xs, Ys),
  NStore = maps:remove(OldMeta, Store),
  maps:put(NewMeta, MergedBucket, NStore).



%% returns two Buckets {B1, B2}.
%% B1 : To > Key
%% B2 : From < Key <= To
split(From, To, Store) ->
  case maps:find(From, Store) of
    {ok, Xs} ->
      Ys = maps:to_list(Xs),
      {KeepL, RestL} = lists:partition(fun({Key, _}) -> not (node:between(Key, From, To)) end, Ys),
      {maps:from_list(KeepL), maps:from_list(RestL)};
    error ->
      not_found
  end.
