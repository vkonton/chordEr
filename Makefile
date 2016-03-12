.SUFFIXES: .beam .erl
.PHONY: utilities default clean chain eventual

MAIN_FILE=node.erl
MACROS_FILE=macros.hrl

default: eventual

utilities: dht.beam storage.beam test.beam sira.beam utilities.beam

chain: utilities
	erlc +debug_info -DCHAIN ${MAIN_FILE}

eventual: utilities
	erlc +debug_info -DEVENTUAL ${MAIN_FILE}

%.beam: %.erl ${MACROS_FILE}
	erlc +debug_info $<

clean:
	@$(RM) *.beam
