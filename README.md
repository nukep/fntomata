# fntomata: A library for asynchronous flow control on functions

It works by composing continuation-passing-style (CPS) functions.

Pronounced: fun-TAA-muh-tuh (like automata)

TODO: describe what this is, and what this isn't.

You would generally use this to describe functions that eventually get a single result back. Those functions may return successful values, or may throw exceptions (which get propagated if you compose them).

It can be viewed as a subset of reactive programming. This library borrows ideas from that paradigm.