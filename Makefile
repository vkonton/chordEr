.SUFFIXES: .beam .erl
.PHONY: utilities default clean chain eventual

MAIN_FILE=src/node.erl
MACROS_FILE=src/macros.hrl

default: eventual

test: $(addprefix helper/, sira.beam test.beam)

utilities: $(addprefix src/, dht.beam storage.beam utilities.beam)

chain: utilities
	erlc +debug_info -DCHAIN ${MAIN_FILE}

eventual: utilities
	erlc +debug_info -DEVENTUAL ${MAIN_FILE}

%.beam: %.erl ${MACROS_FILE}
	erlc +debug_info $<

clean:
	@$(RM) *.beam
