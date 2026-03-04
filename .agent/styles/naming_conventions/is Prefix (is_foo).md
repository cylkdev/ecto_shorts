# is Prefix (is_foo)

Type checks and other boolean checks that are allowed in guard clauses are named with an `is_` prefix.

Examples: `Integer.is_even/1`, `is_list/1`

These functions and macros follow the Erlang convention of an `is_` prefix, instead of a trailing question mark, precisely to indicate that they are allowed in guard clauses.

Note that type checks that are not valid in guard clauses do not follow this convention. For example: `Keyword.keyword?/1`.
