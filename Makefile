.SUFFIXES: .beam .erl
.PHONY: default clean

default: node.beam

%.beam: %.erl
	erlc +debug_info $<

clean:
	@$(RM) *.beam
