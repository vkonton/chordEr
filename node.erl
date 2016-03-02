-module(node).
-export([between/3, locate/2, store/2, join/1, join/2, kth_finger/2]).

-define(Stabilize, 100).
-define(Timeout, 10000).
-define(Fix_fingers, 300).
-define(Hash_length, 3).

%--------------------------------------------------------------------
% Node Initiation and Connection.

join(Inc_id) ->
  timer:start(),
  spawn(fun() -> init(Inc_id) end).

join(Inc_id, Peer) ->
  timer:start(),
  spawn(fun() -> init(Inc_id, Peer) end).

init(Inc_id) ->
  Id = Inc_id,
  %<<Id:?Hash_length/integer>> = crypto:hash(sha, <<Inc_id>>),   
  Predecessor = nil,
  Successor = {Id, self()},
  FingerTable = {0, array:new(?Hash_length+1, {default, Successor})},
  schedule_stabilize(),
  fix_fingers(),
  node(Id, Predecessor, Successor, storage:create(), FingerTable).

init(Inc_id, Peer) ->
  Id = Inc_id,
  %<<Id:?Hash_length/integer>> = crypto:hash(sha, <<Inc_id>>),   
  Predecessor = nil,
  Peer ! {find_successor, Id, self()},
  receive
    {successor, Id, Successor} ->
      Successor
  end,
  FingerTable = {0, array:new(?Hash_length+1, {default, Successor})},
  schedule_stabilize(),
  fix_fingers(),
  io:fwrite("Successor found: ~w~n", [Successor]),
  node(Id, Predecessor, Successor, storage:create(), FingerTable).


store(Value, Peer) ->
  Qref = make_ref(),
  Key = crypto:hash(sha, Value),
  Peer ! {add, Key, Value, Qref, self()},
  receive
    {Qref, Ans} ->
      Ans
  after ?Timeout ->
	  io:format("Time out: no response~n", [])
  end.

locate("*", Peer) ->
  Qref = make_ref(),
  Peer ! {collect_all, Qref, self()},
  receive
    {Qref, All_stores} ->
      All_stores
  end;
locate(Value, Peer) ->
  Qref = make_ref(),
  Key = crypto:hash(sha, Value),
  Peer ! {lookup, Key, Qref, self()},
  receive
    {Qref, Node, Item} ->
      {Node, Item}
  after ?Timeout ->
	  io:format("Time out: no response~n", [])
  end.


between(_, From, From) ->
  true;
between(Key, From, To) when From < To ->
  (Key > From) and (Key =< To);
between(Key, From, To) when From > To ->
  (Key > From) or (Key =< To).

between2(_, From, From) ->
  true;
between2(Key, From, To) when From < To ->
  (Key > From) and (Key < To);
between2(Key, From, To) when From > To ->
  (Key > From) or (Key < To).


%--------------------------------------------------------------------
% Finger Table Functions 

fix_fingers() ->
  timer:send_interval(?Fix_fingers, self(), fix_fingers).


fix_fingers(Id, Successor, {Index, FT}) ->
  NewIndex = case Index+1 > ?Hash_length of 
	       true ->
	         1;
	       false ->
		 Index + 1
	     end,
  NewFT = fix_finger(Id, Successor, NewIndex, FT),
  {NewIndex, NewFT}.

fix_finger(Id, Successor, Index, FT) ->
  Node = kth_finger(Id, Index),
  find_successor(Node, self(), Id, Successor, FT),
  io:fwrite("Id=~w: Requesting to find successor of Node ~w~n",[Id,Node]),
  receive 
    {successor, Node, Succ} ->
      Succ
  end,
  io:fwrite("Id=~w: Found successor of Node ~w = ~w~n",[Id,Node,Succ]),
  array:set(Index, Succ, FT).

kth_finger(Id, K) -> (Id + (1 bsl (K-1))) rem (1 bsl ?Hash_length).


find_successor(Node, Peer, Id, Successor, FT) ->
 {Skey, _} = Successor,
 case between(Node, Id, Skey) of
   true ->
     Peer ! {successor, Node, Successor};
   false ->
     Next = closest_preceding_node(Node, Id, FT),
     Next ! {find_successor, Node, Peer}
 end.


closest_preceding_node(Node, Id, FT) ->
  closest_preceding_node(?Hash_length, Node, Id, FT).

closest_preceding_node(0, _, _, _) ->
  self();
closest_preceding_node(Iter, NodeId, Id, FT) ->
  {Fkey, FPid} = array:get(Iter, FT),
  case between2(Fkey, Id, NodeId) of
    true ->
      FPid;
    false ->
      closest_preceding_node(Iter-1, NodeId, Id, FT)
  end.  



  
%--------------------------------------------------------------------
% Node Helper Functions

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
      case between(Xkey, Id, Skey) of
	true ->
	  Xpid ! {request_predecessor, self()},
	  Pred;
	false ->
	  Spid ! {notify, {Id, self()}},
	  Successor
      end
  end.


notify({Nkey, Npid}, Id, Predecessor, Store) ->
  case Predecessor of
    nil ->
      Keep = handover(Id, Store, Nkey, Npid),
      {{Nkey, Npid}, Keep};
    {Pkey, _} ->
      case between(Nkey, Pkey, Id) of
	true ->
	  Keep = handover(Pkey, Store, Nkey, Npid),
	  {{Nkey, Npid}, Keep};
	false ->
	  {Predecessor, Store}
      end
  end.


handover(Id, Store, Nkey, Npid) ->
  {Keep, Rest} = storage:split(Id, Nkey, Store),
  Npid ! {handover, Rest},
  Keep.


add(Key, Value, Qref, Client, Id, {Pkey, _}, {_, Spid}, Store) ->
  case between(Key, Pkey, Id) of
    true ->
      Client ! {Qref, self()},
      storage:add(Key, Value, Store);
    false ->
      Spid ! {add, Key, Value, Qref, Client},
      Store
  end.


lookup(Key, Qref, Client, Id, {Pkey, _}, Store, FT) ->
  case between(Key, Pkey, Id) of
    true ->
      Result = storage:lookup(Key, Store),
      Client ! {Qref, self(), Result};
    false ->
      {_, Spid} = closest_preceding_node(Key, Id, FT),
      Spid ! {lookup, Key, Qref, Client}
  end.


%--------------------------------------------------------------------
% The main loop for the node proc.

node(Id, Predecessor, Successor, Store, FingerTable = {NextFingerToUpdate, FT}) ->
  receive
    % A peer needs to know our key Id
    {key, Qref, Peer} ->
      Peer ! {Qref, Id},
      node(Id, Predecessor, Successor, Store, FingerTable);

    % A new node entered the ring
    {notify, New} ->
      {Pred, Keep} = notify(New, Id, Predecessor, Store),
      node(Id, Pred, Successor, Keep, FingerTable);

    {successor_status, Pred} ->
      Succ = stabilize(Pred, Id, Successor),
      node(Id, Predecessor, Succ, Store, {NextFingerToUpdate, array:set(1, Succ, FT)});

    {successor, Id, Succ} ->
      node(Id, Predecessor, Succ, Store, {NextFingerToUpdate, array:set(1, Succ, FT)});

    {request_predecessor, Peer} ->
      Peer ! {successor_status, Predecessor},
      node(Id, Predecessor, Successor, Store, FingerTable);

    % A peer asks us to find the successor of a node
    {find_successor, Node, Peer} ->
      find_successor(Node, Peer, Id, Successor, FT),
      node(Id, Predecessor, Successor, Store, FingerTable);

    stabilize ->
      stabilize(Successor),
      node(Id, Predecessor, Successor, Store, FingerTable);

    fix_fingers ->
      Updated_FT = fix_fingers(Id, Successor, FingerTable),
      node(Id, Predecessor, Successor, Store, Updated_FT);

    % Predecessor failed
    predecessor_failed ->
      node(Id, nil, Successor, Store, FingerTable);

    % add an element to the dht
    {add, Key, Value, Qref, Client} ->
      Added = add(Key, Value, Qref, Client,
		  Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Added, FingerTable);

    % query for an element in the dht
    {lookup, Key, Qref, Client} ->
      lookup(Key, Qref, Client, Id, Predecessor, Store, FingerTable),
      node(Id, Predecessor, Successor, Store, FingerTable);

    {handover, Elements} ->
      Merged = storage:merge(Store, Elements),
      node(Id, Predecessor, Successor, Merged, FingerTable);

    {collect_all, Qref, Client} ->
      {Skey, Spid} = Successor,
      case Skey of
	Id ->
	  Client ! {Qref, [{Id, Store}]};
        Skey ->
	  Spid ! {collect_all, Qref, Client, Id, [{Id, Store}]}
      end,
      node(Id, Predecessor, Successor, Store, FingerTable);

    {collect_all, Qref, Client, Start, All_stores} ->
      case Start of
	Id ->
	  Client ! {Qref, All_stores};
	Start ->
	  {_, Spid} = Successor,
	  Spid ! {collect_all, Qref, Client, Start, [{Id , Store} | All_stores]}
      end,
      node(Id, Predecessor, Successor, Store, FingerTable);

      % stop node gracefully
      stop ->
	{_, SPid} = Successor,
	SPid ! {handover, Store},
	SPid ! predecessor_failed,
	SPid ! {notify, Predecessor},
	exit(normal);


	%----------------------------------------------------------------
	% Debug messages

	% probe messages for ring connectivity testing

	list_store ->
	  [io:format("~p ", [Y]) || {_,Y} <- Store],
	  node(Id, Predecessor, Successor, Store, FingerTable);

	probe ->
	  create_probe(Id, array:get(1, FingerTable)),
	  node(Id, Predecessor, Successor, Store, FingerTable);

	{probe, Id, Nodes, T} ->
	  remove_probe(T, Nodes),
	  node(Id, Predecessor, Successor, Store, FingerTable);

	{probe, Ref, Nodes, T} ->
	  forward_probe(Ref, T, Nodes, Id, Successor),
	  node(Id, Predecessor, Successor, Store, FingerTable);

	% a simple message to print node state
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

create_probe(Id,{_,Spid}) ->
  Spid ! {probe,Id,[Id],erlang:timestamp()}.

remove_probe(T, Nodes) ->
  Duration = timer:now_diff(erlang:timestamp(),T),
  Printer = fun(E) -> io:format("~p ",[E]) end,
  lists:foreach(Printer,Nodes),
  io:format("~n Time = ~p",[Duration]).

forward_probe(Ref, T, Nodes, Id, {_,Spid}) ->
  Spid ! {probe,Ref,Nodes ++ [Id],T}.


