-module(dht).
-export([join/1, join/2, stop/1, locate/2, store/3, remove/2]).
-include("macros.hrl").

%%--------------------------------------------------------------------
%% Node Initiation and Connection (start/join, stop/exit).

join(Inc_id) ->
  timer:start(),
  spawn(fun() -> init(Inc_id) end).

join(Inc_id, Peer) ->
  timer:start(),
  spawn(fun() -> init(Inc_id, Peer) end).

stop(Peer) ->
  Peer ! stop.

init(Inc_id) ->
  <<Id:?Hash_length/integer>> = crypto:hash(sha, <<Inc_id>>),
  Predecessor = nil,
  Successor = {Id, self()},
  FingerTable = {0, array:new(?Hash_length+1, {default, Successor})},
  node:schedule_stabilize(),
  node:fix_fingers(),
  node:node(Id, Predecessor, Successor, storage:create(), FingerTable).


init(Inc_id, Peer) ->
  <<Id:?Hash_length/integer>> = crypto:hash(sha, <<Inc_id>>),
  Predecessor = nil,
  Peer ! {find_successor, Id, self()},
  receive
    {successor, Id, Successor} ->
      Successor
  end,
  FingerTable = {0, array:new(?Hash_length+1, {default, Successor})},
  node:schedule_stabilize(),
  node:fix_fingers(),
  node:node(Id, Predecessor, Successor, storage:create(), FingerTable).


%%-----------------------------------------------------------------------------
%% Client API (store, locate, remove).

store(Key, Value, Peer) ->
  Qref = make_ref(),
  <<Hkey:?Hash_length/integer>> = crypto:hash(sha, Key),
  Peer ! {add, Hkey, {Key,Value}, Qref, self()},
  receive
    {Qref, Ans} ->
      {inserted, {Key, Value}, Ans}
  after
    ?Timeout ->
      io:format("Time out: no response~n", [])
  end.


locate("*", Peer) ->
  Qref = make_ref(),
  Peer ! {collect_all, Qref, self()},
  receive
    {Qref, All_stores} ->
      All_stores
  after
    ?Timeout ->
      io:format("Time out: no response~n", [])
  end;
locate(Key, Peer) ->
  Qref = make_ref(),
  <<HKey:?Hash_length/integer>> = crypto:hash(sha, Key),
  Peer ! {lookup, HKey, Qref, self()},
  receive
    {Qref, Node, Item} ->
      {Node, Item}
  after
    ?Timeout ->
      io:format("Time out: no response~n", [])
  end.


remove(Key, Peer) ->
  Qref = make_ref(),
  <<HKey:?Hash_length/integer>> = crypto:hash(sha, Key),
  Peer ! {delete, HKey, Qref, self()},
  receive
    {Qref, Ans} ->
      Ans
  after
    ?Timeout ->
      io:format("Time out: no response~n", [])
  end.
