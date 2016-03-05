.SUFFIXES: .beam .erl
.PHONY: default clean

default: node.beam storage_map.beam test.beam sira.beam
%.beam: %.erl
	erlc +debug_info $<

clean:
	@$(RM) *.beam
