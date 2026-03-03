# Replace nil source with EctoShorts.Source struct

---
Status: accepted
Date: 2026-03-02
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

`EctoShorts.CommonFilters.convert_params_to_filter/3` accepted `nil` as the first argument to support fully data-driven queries where the source was not known at compile time. The caller would pass `nil` and include a `:from` key in the params with a `:query` value that identified the source. This worked but had two problems: `nil` was implicit and easy to confuse with a programming error, and the `:query` value inside `:from` exposed internal source details (schema modules, table strings) directly to the client.

The question is: how should the API accept data-driven queries without exposing internal source details and without relying on `nil` as a sentinel?

## Decision Drivers

* Explicit over implicit: a dedicated struct communicates intent better than `nil`.
* Client isolation: the client should reference tables by name, not by internal source term.
* Backward compatibility for non-nil callers: existing schema module, string, tuple, and query callers must be unaffected.
* Return type stability: `convert_params_to_filter/3` must continue returning `Ecto.Query.t()` so all downstream callers (including `EctoShorts.Actions`) work without changes.

## Considered Options

* Replace `nil` source with `%EctoShorts.Source{}` struct and `:table` key (chosen).
* Keep `nil` source and add the struct as an additional option.
* Use a plain map instead of a struct.

## Decision Outcome

Chosen option: "Replace `nil` source with `%EctoShorts.Source{}` struct and `:table` key", because it makes the data-driven path explicit, hides internal source details behind a name-to-source lookup, and removes the ambiguous `nil` sentinel.

### Consequences

Good, because the API now has two clearly separated calling conventions: pass a source directly (schema module, string, tuple, query) for compile-time-known sources, or pass a `Source` struct for data-driven sources where the client provides a table name.

Good, because internal source details (schema modules, table strings, tuples) are no longer exposed to the client. The client only sees string names.

Good, because errors are clear: missing `:from`, missing `:table`, or an unknown table name all raise `ArgumentError` with descriptive messages.

Bad, because this is a breaking change for any caller using `nil` as the first argument. Those callers must switch to `%Source{}`.

Bad, because ADR 0007 ("Allow nil source in convert_params_to_filter") is now superseded.

## Validation

Run the targeted tests:

    mix test test/ecto_shorts/common_filters/core_test.exs --seed 0 --trace

Look for the describe block "convert_params_to_filter/3 with Source". All nine tests must pass:

* table resolves to a schema module
* table resolves to a table string
* from-filters merge with top-level params
* default select added for schemaless resolved source
* accepts :from as a keyword list
* raises when :from is missing
* raises when :from has no :table key
* raises when table name is not found
* raises when nil is passed as source

Run the full suite to confirm no regressions:

    mix test --seed 0 --trace

Check that the `@spec` for `convert_params_to_filter/3` includes `Source.t()` and does not include `nil`.

## Pros and Cons of the Options

### Replace nil source with Source struct and :table key

Add a new `EctoShorts.Source` struct with a `tables` field (a `%{String.t() => term()}` map). The `Source` clause pops `:from` from params, extracts the `:table` key, looks up the source in the `tables` map, rebuilds the `:from` with `:query` set to the resolved source, and delegates to the existing logic. The `nil` path is removed entirely.

Good, because the struct is self-documenting and the `:table` key is unambiguous.
Good, because the resolved source is a pure passthrough - schema-aware when the value is a schema module, schemaless when it is a string.
Bad, because callers using `nil` must migrate.

### Keep nil source and add the struct as an additional option

Keep the existing `nil` path unchanged and add `Source` as a parallel option.

Good, because no breaking change.
Bad, because two ways to do the same thing creates confusion about which to use.
Bad, because the `nil` path still exposes internal source details.

### Use a plain map instead of a struct

Accept `%{tables: %{...}}` as the first argument instead of a struct.

Good, because no new module is needed.
Bad, because a plain map is easy to confuse with other map arguments and provides no compile-time pattern matching.
Bad, because there is no clear documentation hook or typespec.

## More Information

This decision supersedes ADR 0007 ("Allow nil source in convert_params_to_filter"). The `nil` path introduced in ADR 0007 is removed by this change.

Files changed:

* `lib/ecto_shorts/schemaless_query.ex` - new struct module with `tables` and `source_key` fields
* `lib/ecto_shorts/common_filters.ex` - new `Source` clauses, `nil` rejection, helper functions (`pop_required_source_key/2`, `raise_missing_from!/1`, `resolve_table!/2`), updated `@spec`, `@doc`, and moduledoc
* `test/ecto_shorts/common_filters/core_test.exs` - replaced nil-source tests with Source tests, including custom `source_key` coverage

The `source_key` field (default `:table`) controls which atom key inside the `:from` map identifies the table name. This lets callers choose a domain-appropriate key without being locked into `:table`.

Revisit this decision if `EctoShorts.Actions` functions need to accept `Source` as a source directly.
