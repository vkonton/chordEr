-module(node).
-export([test/0, between/3, locate/2, store/2, start/1, start/2]).

-define(Stabilize, 1000).
-define(Timeout, 10000).
-define(ReplicationFactor, 2).

%--------------------------------------------------------------------
% Node Initiation and Connection.

test() ->
  Pid = start("0"),
  _ = start("1", Pid),
  _ = start("2", Pid),
  _ = start("3", Pid),
  timer:sleep(6000),
  [store(lists:flatten(io_lib:format("~p", [X])),  Pid) || X <- lists:seq(0,3)].


start(Inc_id) ->
  start(Inc_id, nil).

start(Inc_id, Peer) ->
  timer:start(),
  spawn(fun() -> init(Inc_id, Peer) end).

init(Inc_id, Peer) ->
  Predecessor = nil,
  Id = crypto:hash(sha, Inc_id),
  {ok, Successor} = connect(Id, Peer),
  schedule_stabilize(),
  node(Id, Predecessor, Successor, storage:create()).

connect(Id, nil) ->
  {ok, {Id, self()}};

connect(_, Peer) ->
  Qref = make_ref(),
  Peer ! {key, Qref, self()},
  receive
    {Qref, Skey} ->
      {ok, {Skey, Peer}}
  after ?Timeout ->
	  io:format("Time out: no response~n", [])
  end.


store(Value, Peer) ->
  Qref = make_ref(),
  Key = crypto:hash(sha, Value),
  Peer ! {add_replicate, Key, ?ReplicationFactor, Value, Qref, self()},
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


%--------------------------------------------------------------------
% Node Helper Functions

schedule_stabilize() ->
  timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, Spid}) ->
  Spid ! {request, self()}.


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
	  Xpid ! {request, self()},
	  Pred;
	false ->
	  Spid ! {notify, {Id, self()}},
	  Successor
      end
  end.


request(Peer, Predecessor) ->
  case Predecessor of
    nil ->
      Peer ! {status, nil};
    {Pkey, Ppid} ->
      Peer ! {status, {Pkey, Ppid}}
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


add_replicate(Key, RepFactor, Value, Qref, Client, Id, {Pkey, _}, {_, Spid}, Store) ->
  case between(Key, Pkey, Id) of
    true ->
      Client ! {Qref, self()},
      Spid ! {replicate, Key, RepFactor-1, Value},
      storage:add(Key, Value, Store);
    false ->
      Spid ! {add_replicate, Key, RepFactor, Value, Qref, Client},
      Store
  end.


replicate(Key, Value, RepFactor, {_, Spid}, Store) ->
  if
    RepFactor > 1 ->
      Spid ! {replicate, Key, RepFactor-1, Value},
      storage:add(Key, Value, Store);
    RepFactor =:= 1 ->
       storage:add(Key, Value, Store);
    RepFactor < 1 ->
       Store
  end.


lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, Store) ->
  case between(Key, Pkey, Id) of
    true ->
      Result = storage:lookup(Key, Store),
      Client ! {Qref, self(), Result};
    false ->
      {_, Spid} = Successor,
      Spid ! {lookup, Key, Qref, Client}
  end.


%--------------------------------------------------------------------
% The main loop for the node proc.

node(Id, Predecessor, Successor, Store) ->
  receive
    % A peer needs to know our key Id
    {key, Qref, Peer} ->
      Peer ! {Qref, Id},
      node(Id, Predecessor, Successor, Store);

    % New node
    {notify, New} ->
      {Pred, Keep} = notify(New, Id, Predecessor, Store),
      node(Id, Pred, Successor, Keep);

    % Message coming from the predecessor who wants to know our predecessor
    {request, Peer} ->
      request(Peer, Predecessor),
      node(Id, Predecessor, Successor, Store);

    % What is the predecessor of the next node (successor)
    {status, Pred} ->
      Succ = stabilize(Pred, Id, Successor),
      node(Id, Predecessor, Succ, Store);

    stabilize ->
      stabilize(Successor),
      node(Id, Predecessor, Successor, Store);

    % add an element to the dht
    {add, Key, Value, Qref, Client} ->
      Added = add(Key, Value, Qref, Client,
		  Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Added);

    % add an element to the dht and inform k-1 successors to
    % replicate it.
    {add_replicate, Key, RepFactor, Value, Qref, Client} ->
      Added = add_replicate(Key, RepFactor, Value, Qref, Client,
		            Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Added);

    {replicate, Key, RepFactor, Value} ->
      Added = replicate(Key, Value, RepFactor, Successor, Store),
      node(Id, Predecessor, Successor, Added);

    % query for an element in the dht
    {lookup, Key, Qref, Client} ->
      lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
      node(Id, Predecessor, Successor, Store);

    {handover, Elements} ->
      Merged = storage:merge(Store, Elements),
      node(Id, Predecessor, Successor, Merged);

    {collect_all, Qref, Client} ->
      {Skey, Spid} = Successor,
      case Skey of
	Id ->
	  Client ! {Qref, [{Id, Store}]};
	Skey ->
	  Spid ! {collect_all, Qref, Client, Id, [{Id, Store}]}
      end,
      node(Id, Predecessor, Successor, Store);

    {collect_all, Qref, Client, Start, All_stores} ->
      case Start of
	Id ->
	  Client ! {Qref, All_stores};
	Start ->
	  {_, Spid} = Successor,
	  Spid ! {collect_all, Qref, Client, Start, [{Id , Store} | All_stores]}
      end,
      node(Id, Predecessor, Successor, Store);

    % stop node gracefully
    stop ->
      Successor ! {handover, Store},
      exit(normal);


    %----------------------------------------------------------------
    % Debug messages

    % probe messages for ring connectivity testing

    list_store ->
      [io:format("~p ", [Y]) || {_,Y} <- Store],
      node(Id, Predecessor, Successor, Store);

    probe ->
      create_probe(Id, Successor),
      node(Id, Predecessor, Successor, Store);

    {probe, Id, Nodes, T} ->
      remove_probe(T, Nodes),
      node(Id, Predecessor, Successor, Store);

    {probe, Ref, Nodes, T} ->
      forward_probe(Ref, T, Nodes, Id, Successor),
      node(Id, Predecessor, Successor, Store);

    % a simple message to print node state
    state ->
      io:format(' Id : ~w~n Predecessor : ~w~n Successor : ~w~n', [Id, Predecessor, Successor]),
      node(Id, Predecessor, Successor, Store);

    _ ->
      io:format('Invalid message received.'),
      node(Id, Predecessor, Successor, Store)
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
