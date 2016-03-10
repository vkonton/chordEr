.SUFFIXES: .beam .erl
.PHONY: utilities default clean chain eventual

default: eventual

utilities: storage.beam test.beam sira.beam

chain: utilities
	erlc +debug_info -DCHAIN node.erl

eventual: utilities
	erlc +debug_info -DEVENTUAL node.erl

%.beam: %.erl
	erlc +debug_info $<

clean:
	@$(RM) *.beam
