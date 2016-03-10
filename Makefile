.SUFFIXES: .beam .erl
.PHONY: utilities default clean chain eventual

default: eventual

utilities: node.beam storage.beam test.beam sira.beam utilities.beam

chain: utilities
	erlc +debug_info -DCHAIN dht.erl

eventual: utilities
	erlc +debug_info -DEVENTUAL dht.erl

%.beam: %.erl
	erlc +debug_info $<

clean:
	@$(RM) *.beam
