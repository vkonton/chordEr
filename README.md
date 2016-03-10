ChordEr
=======

This is an implementation for the DHT "Chord", as presented in the paper https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf written in Erlang, for the Distributed Systems Course,
9th Semester, National Technical University of Athens.


Authors: Vasilis Kontonis, Michalis Kokologiannakis.

Course info and details (in greek): http://www.cslab.ntua.gr/courses/distrib/

* [Licence](#licence)
* [Dependencies](#dependencies)
* [Installing](#installing)
* [Usage](#usage)
* [Features](#features)

Licence
-------

This program is distributed under the GPL, version 3 or later. Please see
the COPYING file for details.

Dependencies
------------

In order to use the tool, you only need a working version of Erlang.

Installing
----------

* Download ChordEr's sources or clone this repository.

* For a default build (w/ eventual consistency):

		make

* For linearizability (w/ chain replication):

		make chain

Usage
-----

* To create a ring, open an Erlang shell and enter the command:

		node:join(Int).

	where Int is any integer.

* To add nodes in an existing ring enter the command:

		node:join(Int, Node).

	where Int is any integer, and Node a process already in the ring.

* To remove a node from an existing ring use:

		node:stop(Node).

* To insert data enter the command:

		node:store(Key, Value, Node).

* To delete data enter the command:

		node:remove(Key, Node)

* To search for a key and its corresponding data use:

		node:locate(Key, Node).

* To get the data from all nodes use:

		node:locate("*", Node).


Features
--------

Some features of ChordEr are:

* Support for *concurrent* node joins
* Efficient implementation of finger tables
* Logarithmic data lookup time (O(log n))
* Logarithmic time for node joins (O(log n))
* Data replication (default: eventual consistency -- linearizability w/ chain replication also offered)

