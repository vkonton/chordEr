-module(utilities).
-export([between/3, between2/3]).

%%-----------------------------------------------------------------------------
%% Help Functions to ensure node and key sorting.
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
