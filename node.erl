-module(node).
-export([schedule_stabilize/0, fix_fingers/0, node/5]).
-include("macros.hrl").

%% Default Replication scheme is Eventual Consistency.
-ifndef(EVENTUAL).
-ifndef(CHAIN).
-define(EVENTUAL, 1).
-endif.
-endif.


%%--------------------------------------------------------------------
%% Finger Table Functions

fix_fingers() ->
  timer:send_interval(?Fix_fingers, self(), fix_fingers).


% Called periodically, fixes the FT, one finger at a time
fix_fingers(Id, Successor, {Index, FT}) ->
  case Index+1 > ?Hash_length of
    true ->
      NewIndex = 1;
    false ->
      NewIndex = Index + 1
  end,
  NewFT = fix_finger(Id, Successor, NewIndex, FT),
  {NewIndex, NewFT}.


% Used by fix_fingers, fixes a finger table entry
fix_finger(Id, Successor, Index, FT) ->
  Node = kth_finger(Id, Index),
  find_successor(Node, self(), Id, Successor, FT),
  receive
    {successor, Node, Succ} ->
      Succ
  after
    ?FTTimeout ->
      Succ = array:get(1, FT)
  end,
  array:set(Index, Succ, FT).


% Function that calculates the node of whom the successor will be on the FT entry
kth_finger(Id, K) -> (Id + (1 bsl (K-1))) rem (1 bsl ?Hash_length).


% Finds the successor of the specified node
find_successor(Node, Peer, Id, Successor, FT) ->
  {Skey, _} = Successor,
  case utilities:between(Node, Id, Skey) of
    true ->
      Peer ! {successor, Node, Successor};
    false ->
      Next = closest_preceding_node(Node, Id, FT),
      Next ! {find_successor, Node, Peer}
  end.


% Finds the closest preceding node to the target, to whom a request must be forwarded
closest_preceding_node(Node, Id, FT) ->
  closest_preceding_node(?Hash_length, Node, Id, FT).
closest_preceding_node(0, _, _, _) ->
  self();
closest_preceding_node(Iter, NodeId, Id, FT) ->
  {Fkey, FPid} = array:get(Iter, FT),
  case utilities:between2(Fkey, Id, NodeId) of
    true ->
      FPid;
    false ->
      closest_preceding_node(Iter-1, NodeId, Id, FT)
  end.


% Refreshes finger table entries when a node has exited the ring
refresh_fingers(Node, NodeSucc, FT) ->
  lists:foldl(
    fun(X,Acc) ->
	case array:get(X,Acc) =:= Node of
	  true ->
	    array:set(X, NodeSucc, Acc);
	  false->
	    Acc
	end
    end,
    FT,
    lists:seq(1, ?Hash_length)).


%%--------------------------------------------------------------------
%% Node Helper Functions

schedule_stabilize() ->
  timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, Spid}) ->
  Spid ! {request_predecessor, self()}.


stabilize(Pred, Id, Successor) ->
  {Skey, Spid} = Successor,
  case Pred of
    nil ->
      Spid ! {notify, {Id, self()}},
      Successor;
    {Id, _} ->
      Successor;
    {Skey, _} ->
      Spid ! {notify, {Id, self()}},
      Successor;
    {Xkey, Xpid} ->
      case utilities:between(Xkey, Id, Skey) of
	true ->
	  Xpid ! {request_predecessor, self()},
	  Pred;
	false ->
	  Spid ! {notify, {Id, self()}},
	  Successor
      end
  end.


notify({Nkey, Npid}, Id, Predecessor, {_Skey, Spid}, Store) ->
  case Predecessor of
    nil ->
      {KeepStore, ToSend} = update_store(Nkey, Id, Store),
      if
	ToSend =:= #{} ->
	  ok;
	true ->
	  Npid ! {handover, ToSend}
      end,
      Spid ! {update_store, Nkey, Id, ?ReplicationFactor},
      Npid ! {delete_extra_replicas, ?ReplicationFactor-1, {Id, self()}},
      {{Nkey, Npid}, KeepStore};
    {Pkey, _} ->
      case utilities:between(Nkey, Pkey, Id) of
	true ->
	  {KeepStore, ToSend} = update_store(Nkey, Id, Store),
	  if
	    ToSend =:= #{} ->
	      ok;
	    true ->
	      Npid ! {handover, ToSend}
	  end,
	  Spid ! {update_store, Nkey, Id, ?ReplicationFactor-1},
	  {{Nkey, Npid}, KeepStore};
	false ->
	  {Predecessor, Store}
      end
  end.


%% Split my store and return two buckets upon node insert.
update_store(Nkey, Id, Store) ->
  case storage:split(Id, Nkey, Store) of
    {Keep, Rest} ->
      Mine = storage:put_bucket(Id, Keep, Store),
      if
	Rest =:= #{} ->
	  NewStore = Mine;
	true ->
	  NewStore = storage:put_bucket(Nkey, Rest, Mine)
      end,
      {NewStore, Rest};
    not_found ->
      {Store, #{}}
  end.


replicate_chain(Key, RootId, RepFactor, Value, {_, Spid}, Qref, Client, Store) ->
  if
    RepFactor > 1 ->
      Nstore = storage:add(RootId, Key, Value, Store),
      Spid ! {replicate_chain, Key, RootId, RepFactor-1, Value, Qref, Client};
    RepFactor =:= 1 ->
      Nstore = storage:add(RootId, Key, Value, Store),
      %% The write is complete on all k nodes, inform client.
      Client ! {Qref, self()};
    RepFactor < 1 ->
      Nstore = Store
  end,
  Nstore.


replicate_eventual(Key, RootId, RepFactor, Value, {_, Spid}, Store) ->
  if
    RepFactor > 1 ->
      Spid ! {replicate_eventual, Key, RootId, RepFactor-1, Value},
      storage:add(RootId, Key, Value, Store);
    RepFactor =:= 1 ->
      storage:add(RootId, Key, Value, Store);
    RepFactor < 1 ->
      Store
  end.


delete_replica_chain(Key, RootId, RepFactor, {_, Spid}, Qref, Client, Store) ->
  if
    RepFactor > 1 ->
      Nstore = storage:delete(RootId, Key, Store),
      Spid ! {delete_replica_chain, Key, RootId, RepFactor-1, Qref, Client};
    RepFactor =:= 1 ->
      Nstore = storage:delete(RootId, Key, Store),
      %% The write is complete on all k nodes, inform client.
      Client ! {Qref, self()};
    RepFactor < 1 ->
      Nstore = Store
  end,
  Nstore.


delete_replica_eventual(Key, RootId, RepFactor, {_, Spid}, Store) ->
  if
    RepFactor > 1 ->
      Nstore = storage:delete(RootId, Key, Store),
      Spid ! {delete_replica_eventual, Key, RootId, RepFactor-1};
    RepFactor =:= 1 ->
      Nstore = storage:delete(RootId, Key, Store);
    RepFactor < 1 ->
      Nstore = Store
  end,
  Nstore.


lookup_chain(Key, RootId, RepFactor, Qref, Client, {_, Spid}, Store) ->
  if
    RepFactor > 1 ->
      Spid ! {lookup_chain, Key, RootId, RepFactor-1, Qref, Client};
    RepFactor =:= 1 ->
      case storage:lookup(RootId, Key, Store) of
        not_found ->
	  Client ! {Qref, self(), not_exists};
	X ->
	  Client ! {Qref, self(), X}
      end;
    RepFactor < 1 ->
      ok
  end.


find_root(Key, Qref, Client, Id, {Pkey, _}, FT) ->
  case utilities:between(Key, Pkey, Id) of
    true ->
      Client ! {Qref, Id, self()};
    false ->
      find_successor(Key, self(), Id, array:get(1, FT), FT),
      receive
	{successor, Key, {_, Npid}} ->
	  Npid
      end,
      Npid ! {find_root, Key, Qref, Client}
  end.


%%----------------------------------------------------------------------------
%% Core functions Implementing Chain Replication.
-ifdef(CHAIN).

add(Key, Value, Qref, Client, Id, {Pkey, _}, Successor={_, Spid}, FT, Store) ->
  case utilities:between(Key, Pkey, Id) of
    % my Id is RootId for this Key.
    true ->
      Spid ! {replicate_chain, Key, Id, ?ReplicationFactor-1, Value, Qref, Client},
      Nstore = storage:add(Id, Key, Value, Store);
    false ->
      find_successor(Key, self(), Id, Successor, FT),
      receive
	{successor, Key, {_, Npid}} ->
	  Npid
      end,
      Npid ! {add, Key, Value, Qref, Client},
      Nstore = Store
  end,
  Nstore.


delete(Key, Qref, Client, Id, {Pkey, _}, Successor={_, Spid}, FT, Store) ->
  case utilities:between(Key, Pkey, Id) of
    % my Id is RootId for this Key.
    true ->
      Nstore = storage:delete(Id, Key, Store),
      Client ! {Qref, self()},
      Spid ! {delete_replica_chain, Key, Id, ?ReplicationFactor-1, Qref, Client};
    false ->
      find_successor(Key, self(), Id, Successor, FT),
      receive
	{successor, Key, {_, Npid}} ->
	  Npid
      end,
      Npid ! {delete, Key, Qref, Client},
      Nstore = Store
  end,
  Nstore.


lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, FT, _) ->
  case utilities:between(Key, Pkey, Id) of
    % my Id is RootId for this Key.
    true ->
      {_, Spid} = Successor,
      % delegate read to the last replication node.
      Spid ! {lookup_chain, Key, Id, ?ReplicationFactor-1, Qref, Client};
    false ->
      find_successor(Key, self(), Id, Successor, FT),
      receive
	{successor, Key, {_, Npid}} ->
	  Npid
      end,
      Npid ! {lookup, Key, Qref, Client}
  end.

-endif.


%%----------------------------------------------------------------------------
%% Core functions Implementing Eventual Replication.
-ifdef(EVENTUAL).

%% The message to client that the write is complete is sent by the
%% root(master) node.
add(Key, Value, Qref, Client, Id, {Pkey, _}, Successor={_, Spid}, FT, Store) ->
  case utilities:between(Key, Pkey, Id) of
    % my Id is RootId for this Key.
    true ->
      Client ! {Qref, self()},
      Spid ! {replicate_eventual, Key, Id, ?ReplicationFactor-1, Value},
      Nstore = storage:add(Id, Key, Value, Store);
    false ->
      find_successor(Key, self(), Id, Successor, FT),
      receive
	{successor, Key, {_, Npid}} ->
	  Npid
      end,
      Npid ! {add, Key, Value, Qref, Client},
      Nstore = Store
  end,
  Nstore.


delete(Key, Qref, Client, Id, {Pkey, _}, Successor={_, Spid}, FT, Store) ->
  case utilities:between(Key, Pkey, Id) of
    % my Id is RootId for this Key.
    true ->
      Nstore = storage:delete(Id, Key, Store),
      Client ! {Qref, self()},
      Spid ! {delete_replica_eventual, Key, Id, ?ReplicationFactor-1};
    false ->
      find_successor(Key, self(), Id, Successor, FT),
      receive
	{successor, Key, {_, Npid}} ->
	  Npid
      end,
      Npid ! {delete, Key, Qref, Client},
      Nstore = Store
  end,
  Nstore.


lookup(Key, Qref, Client, Id, _, _, FT, Store) ->
  Result = storage:full_lookup(Key, Store),
  case Result of
    not_found ->
      find_successor(Key, self(), Id, array:get(1, FT), FT),
      receive
	{successor, Key, {_, Npid}} ->
	  Npid
      end,
      if
	Npid =:= self() ->
	  Client ! {Qref, self(), not_exists};
	true ->
	  Npid ! {lookup, Key, Qref, Client}
      end;
    Value ->
      Client ! {Qref, self(), Value}
  end.

-endif.


%%--------------------------------------------------------------------
%% The main loop for the node proc.

node(Id, Predecessor, Successor, Store, FingerTable = {NextFingerToUpdate, FT}) ->
  receive

    %%--------------------------------------------------------------------
    %% Basic Ring Stabilization
    %% A peer needs to know our key Id
    {key, Qref, Peer} ->
      Peer ! {Qref, Id},
      node(Id, Predecessor, Successor, Store, FingerTable);

    %% A new node entered the ring
    {notify, New} ->
      {Pred, Keep} = notify(New, Id, Predecessor, Successor, Store),
      node(Id, Pred, Successor, Keep, FingerTable);

    {successor_status, Pred} ->
      Succ = stabilize(Pred, Id, Successor),
      node(Id, Predecessor, Succ, Store, {NextFingerToUpdate, array:set(1, Succ, FT)});

    {request_predecessor, Peer} ->
      Peer ! {successor_status, Predecessor},
      node(Id, Predecessor, Successor, Store, FingerTable);

    stabilize ->
      stabilize(Successor),
      node(Id, Predecessor, Successor, Store, FingerTable);


    %%--------------------------------------------------------------------
    %% Finger Table Messages.
    %% A peer asks us to find the successor of a node
    {find_successor, Node, Peer} ->
      find_successor(Node, Peer, Id, Successor, FT),
      node(Id, Predecessor, Successor, Store, FingerTable);

    fix_fingers ->
      Updated_FT = fix_fingers(Id, Successor, FingerTable),
      node(Id, Predecessor, Successor, Store, Updated_FT);


    %%--------------------------------------------------------------------
    %% Basic Ring Queries
    %% add an element to the dht
    {add, Key, Value, Qref, Client} ->
      Added = add(Key, Value, Qref, Client,
		  Id, Predecessor, Successor, FT, Store),
      node(Id, Predecessor, Successor, Added, FingerTable);

    {delete, Key, Qref, Client} ->
      Removed = delete(Key, Qref, Client,
		       Id, Predecessor, Successor, FT, Store),
      node(Id, Predecessor, Successor, Removed, FingerTable);

    %% query for an element in the dht
    {lookup, Key, Qref, Client} ->
      lookup(Key, Qref, Client, Id, Predecessor, Successor, FT, Store),
      node(Id, Predecessor, Successor, Store, FingerTable);

    {lookup_chain, Key, RootId, RepFactor, Qref, Client} ->
      lookup_chain(Key, RootId, RepFactor, Qref, Client, Successor, Store),
      node(Id, Predecessor, Successor, Store, FingerTable);

    %% finds the primary storage of a key
    {find_root, Key, Qref, Client} ->
      find_root(Key, Qref, Client, Id, Predecessor, FT),
      node(Id, Predecessor, Successor, Store, FingerTable);

    %%-------------------------------------------------------------------------
    %% Welcome to Replication Hell.

    %% replicate an element when its added to the node's k-1 successors.

    %% The message to client that the write is complete is sent by the
    %% last node i.e the k-1 successor of the root(master) node.
    {replicate_chain, Key, RootId, RepFactor, Value, Qref, Client} ->
      Added = replicate_chain(Key, RootId, RepFactor, Value, Successor, Qref, Client, Store),
      node(Id, Predecessor, Successor, Added, FingerTable);

    {replicate_eventual, Key, RootId, RepFactor, Value} ->
      Added = replicate_eventual(Key, RootId, RepFactor, Value, Successor, Store),
      node(Id, Predecessor, Successor, Added, FingerTable);

    %% delete an element's replica from the k-1 successors.
    {delete_replica_chain, Key, RootId, RepFactor, Qref, Client} ->
      Deleted = delete_replica_chain(Key, RootId, RepFactor, Successor, Qref, Client, Store),
      node(Id, Predecessor, Successor, Deleted, FingerTable);

    {delete_replica_eventual, Key, RootId, RepFactor} ->
      Deleted = delete_replica_eventual(Key, RootId, RepFactor, Successor, Store),
      node(Id, Predecessor, Successor, Deleted, FingerTable);

    %%-------------------------------------------------------------------------
    %% Node Join Replication.

    {delete_extra_replicas, RepFactor, NewNode} ->
      case Predecessor of
	nil ->
	  node(Id, Predecessor, Successor, Store, FingerTable);
	Predecessor ->
	  Predecessor
      end,
      {_Pkey, Ppid} = Predecessor,
      {_Skey, Spid} = Successor,
      case NewNode of
	nil ->
	  ok;
	{_Nkey, Npid} ->
	  MyData = storage:get_bucket(Id, Store),
	  if
	    MyData =:= #{} ->
	      ok;
	    true ->
	      Npid ! {handover, Id, MyData}
	  end
      end,
      if
	RepFactor > 1 ->
	  Ppid ! {delete_extra_replicas, RepFactor-1, NewNode},
	  Spid ! {delete_my_storage, Id, ?ReplicationFactor};
	RepFactor =:= 1 ->
	  Spid ! {delete_my_storage, Id, ?ReplicationFactor};
	RepFactor < 1 ->
	  ok
      end,
      node(Id, Predecessor, Successor, Store, FingerTable);

    {delete_my_storage, RootId, RepFactor} ->
      {_Skey, Spid} = Successor,
      if
	RepFactor > 1 ->
	  Spid ! {delete_my_storage, RootId, RepFactor-1},
	  NewStore = Store;
	RepFactor =:= 1 ->
	  NewStore = storage:delete_bucket(RootId, Store);
	RepFactor < 1 ->
	  NewStore = Store
      end,
      node(Id, Predecessor, Successor, NewStore, FingerTable);

    {update_store, Nkey, RootId, RepFactor} ->
      {_Skey, Spid} = Successor,
      if
	RepFactor > 1 ->
	  {NewStore, _Rest} = update_store(Nkey, RootId, Store),
	  Spid ! {update_store, Nkey, RootId, RepFactor-1};
	RepFactor =:= 1 ->
	  {NStore, _Rest} = update_store(Nkey, RootId, Store),
	  NewStore = storage:delete_bucket(Nkey, NStore);
	RepFactor < 1 ->
	  NewStore = Store
      end,
      node(Id, Predecessor, Successor, NewStore, FingerTable);


    {handover, Bucket} ->
      Merged = storage:add_bucket(Id, Bucket, Store),
      node(Id, Predecessor, Successor, Merged, FingerTable);

    {handover, RootId, Bucket} ->
      Merged = storage:put_bucket(RootId, Bucket, Store),
      node(Id, Predecessor, Successor, Merged, FingerTable);

    %%-------------------------------------------------------------------------
    %% Node Depart Replication.
    {fix_replicas, OldRootId, Bucket} ->
      DelStore = storage:delete_bucket(OldRootId, Store),
      NStore = storage:add_bucket(Id, Bucket, DelStore),
      {_, Spid} = Successor,
      Spid ! {merge_replicas, ?ReplicationFactor-1, OldRootId, {Id, self()}},
      node(Id, Predecessor, Successor, NStore, FingerTable);

    {merge_replicas, RepFactor, OldRootId, NewRoot} ->
      {_, Spid} = Successor,
      {NewRootId, NewRootPid} = NewRoot,
      if
	RepFactor > 1 ->
	  NStore = storage:merge_buckets(OldRootId, NewRootId, Store),
	  Spid ! {merge_replicas, RepFactor-1, OldRootId, NewRoot};
	RepFactor =:= 1 ->
	  NewRootPid ! {give_me_replicas, self()},
	  NStore = Store;
	RepFactor < 1 ->
	  NStore = Store
      end,
      node(Id, Predecessor, Successor, NStore, FingerTable);

    {give_me_replicas, Npid} ->
      RootBucket = storage:get_bucket(Id, Store),
      Npid ! {handover, Id, RootBucket},
      node(Id, Predecessor, Successor, Store, FingerTable);

    {last_node, RepFactor, RootPid} ->
      {_, Spid} = Successor,
      if
	RepFactor > 1 ->
	  Spid ! {last_node, RepFactor-1, RootPid};
	RepFactor =:= 1 ->
	  RootPid ! {give_me_replicas, self()};
	RepFactor < 1 ->
	  ok
      end,
      node(Id, Predecessor, Successor, Store, FingerTable);

    {replicate_on_last_node, RepFactor} ->
      {_, Spid} = Successor,
      {_, Ppid} = Predecessor,
      if
	RepFactor >= 1 ->
	  Ppid ! {replicate_on_last_node, RepFactor-1},
	  Spid ! {last_node, ?ReplicationFactor-1, self()};
	RepFactor < 1 ->
	  ok
      end,
      node(Id, Predecessor, Successor, Store, FingerTable);


    %%-------------------------------------------------------------------------
    %% Node Departure.
    {predecessor_stopped, Pred} ->
      node(Id, Pred, Successor, Store, FingerTable);

    {successor, Id, Succ} ->
      node(Id, Predecessor, Succ, Store, {NextFingerToUpdate, array:set(1, Succ, FT)});

    {node_exit, Node, NodePredecessor, NodeSuccessor} ->
      {_, Spid} = Successor,
      if
	{Id, self()} =:= NodeSuccessor ->
	  Spid ! exit_ok,
	  FTnew = FT;
	true ->
	  FTnew = refresh_fingers(Node, NodeSuccessor, FT),
	  {_Pkey, Ppid} = Predecessor,
	  Ppid ! {node_exit, Node, NodePredecessor, NodeSuccessor},
	  receive
	    exit_ok ->
	      if
		{Id, self()} =:= NodePredecessor ->
		  {_Nkey, Npid} = Node,
		  Npid ! exit_ok;
		true ->
		  Spid ! exit_ok
	      end
	  end
      end,
      node(Id, Predecessor, Successor, Store, {NextFingerToUpdate, FTnew});

    %% stop node gracefully
    stop ->
      {_, Spid} = Successor,
      {Pkey, Ppid} = Predecessor,
      Spid ! {predecessor_stopped, Predecessor},
      Ppid ! {successor, Pkey, Successor},
      Ppid ! {node_exit, {Id, self()}, Predecessor, Successor},
      receive
	exit_ok ->
	  ToSend = storage:get_bucket(Id,Store),
	  Spid ! {fix_replicas, Id, ToSend},
	  Ppid ! {replicate_on_last_node, ?ReplicationFactor-1},
	  exit(normal)
      end;


    %%----------------------------------------------------------------
    %% Debug messages

    %% Collect all Data from the DHT.
    {collect_all, Qref, Client} ->
      {Skey, Spid} = Successor,
      case Skey of
	Id ->
	  Client ! {Qref, [{Id, self(), Store}]};
	Skey ->
	  Spid ! {collect_all, Qref, Client, Id, [{Id, self(), Store}]}
      end,
      node(Id, Predecessor, Successor, Store, FingerTable);

    {collect_all, Qref, Client, Start, All_stores} ->
      case Start of
	Id ->
	  Client ! {Qref, All_stores};
	Start ->
	  {_, Spid} = Successor,
	  Spid ! {collect_all, Qref, Client, Start, [{Id , self(), Store} | All_stores]}
      end,
      node(Id, Predecessor, Successor, Store, FingerTable);
    %% probe messages for ring connectivity testing

    list_store ->
      io:format("~p ", [Store]),
      node(Id, Predecessor, Successor, Store, FingerTable);

    probe ->
      create_probe(self(), Successor),
      node(Id, Predecessor, Successor, Store, FingerTable);

    {probe, Ref, Nodes, T} ->
      case Ref =:= self() of
	true ->
	  remove_probe(T, Nodes);
	false ->
	  forward_probe(Ref, T, Nodes, Successor)
      end,
      node(Id, Predecessor, Successor, Store, FingerTable);

    %% a simple message to print node state
    state ->
      io:format(' Id : ~w~n Predecessor : ~w~n Successor : ~w~n', [Id, Predecessor, Successor]),
      node(Id, Predecessor, Successor, Store, FingerTable);

    get_ft ->
      io:format('Finger Table: ~w~n', [tl(array:to_list(FT))]),
      node(Id, Predecessor, Successor, Store, FingerTable);

    Msg ->
      io:format('Invalid message received (~w)~n',[Msg]),
      node(Id, Predecessor, Successor, Store, FingerTable)
  end.


%%-------------------------------------------------------------------
%% Probe functions for ring connectivity testing

create_probe(Pid, {_, Spid}) ->
  Spid ! {probe, Pid, [Pid], erlang:timestamp()}.


remove_probe(T, Nodes) ->
  Duration = timer:now_diff(erlang:timestamp(),T),
  Printer = fun(E) -> io:format("~w ",[E]) end,
  lists:foreach(Printer,Nodes),
  io:format("~n Time = ~p ~n",[Duration]).


forward_probe(Ref, T, Nodes, {_,Spid}) ->
  Spid ! {probe,Ref,Nodes ++ [self()],T}.
