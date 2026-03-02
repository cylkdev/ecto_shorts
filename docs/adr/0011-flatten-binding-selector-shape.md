# Flatten binding selector params to JSON-friendly shape

---
Status: accepted
Date: 2026-03-01
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

The `convert_params_to_filter/3` function accepts a `:bind` key for targeting specific query bindings. The current shape uses three levels of nesting: `bind -> mode -> target -> filters`. For example, `%{bind: %{as: %{post: %{published: true}}}}` targets the named binding `:post` with a filter on `:published`. Positional bindings use integer map keys: `%{bind: %{at: %{1 => %{published: true}}}}`.

This structure does not serialize naturally to JSON. Integer map keys do not exist in JSON (all JSON object keys are strings). Keyword lists like `[post: %{...}, author: %{...}]` for multiple bindings have no direct JSON equivalent. The three levels of nesting make the payload verbose and hard to construct from HTTP request data. The question is: how should we reshape the binding selector params so they map cleanly to JSON while preserving the same binding semantics?

## Decision Drivers

- The payload must serialize to standard JSON without lossy conversions (no integer keys, no keyword lists as the primary shape).
- The shape should be as flat as possible to reduce nesting depth.
- Named bindings (`:as`) and positional bindings (`:at`) must remain distinguishable.
- The `:first` and `:last` shortcuts must remain available without adding new top-level keys.
- This is a library with a public API, so the change must be deliberate and documented as a breaking change.

## Considered Options

- **Option A: Flat map with `:as`/`:at` keys** - each bind entry is a flat map where `:as` or `:at` identifies the target and all other keys are filters. Multiple bindings use a list of flat maps.
- **Option B: Target as top key under bind** - the binding target is used directly as a key under `:bind`, e.g. `%{bind: %{post: %{...}}}`. Rejected because named and positional bindings become indistinguishable without a type hint.
- **Option C: Keep the old shape** - no change. Rejected because integer map keys and keyword lists do not serialize to JSON.

## Decision Outcome

Chosen option: "Option A: Flat map with `:as`/`:at` keys", because it reduces the nesting from three levels to one, eliminates integer map keys, and maps directly to JSON objects. The `:at` key accepts integers, `:first`, or `:last` to consolidate positional and shortcut modes into a single key.

### Consequences

Good, because the payload is flat and maps directly to JSON: `{"bind": {"as": "post", "published": true}}`. Good, because multiple bindings are a JSON array of objects: `{"bind": [{"as": "post", ...}, {"as": "author", ...}]}`. Good, because `:first` and `:last` are now values of `:at` instead of separate modes, reducing the API surface.

Bad, because this is a breaking change to the public API. All callers using the old nested `%{bind: %{as: %{target: params}}}` shape must update. Bad, because the `with_ties` bind payload needs a `:value` key since the boolean can no longer be the only value in a target-keyed map.

## Validation

Run the full test suite to confirm the new shape is accepted and the old shape is rejected:

    mix test --seed 0 --trace

Search the codebase for any remaining references to the old nested pattern:

    grep -rn "bind.*as:.*%{" lib/ test/ guides/
    grep -rn "bind.*at:.*%{" lib/ test/ guides/

The search should return zero matches for the old nested `%{as: %{target: ...}}` pattern.

## Pros and Cons of the Options

### Option A: Flat map with :as/:at keys

Each bind entry is a flat map. The `:as` or `:at` key identifies the binding target. All other keys are filters.

Good, because it reduces nesting from 3 levels to 1.
Good, because it eliminates integer map keys and keyword lists from the user-facing shape.
Good, because `:first`/`:last` fold into `:at` as special values.
Bad, because it is a breaking change.

### Option B: Target as top key under bind

The binding target is the key directly under `:bind`.

Good, because it is very flat.
Bad, because named bindings (atom keys) and positional bindings (integer keys) cannot be distinguished by key type alone in JSON.

### Option C: Keep the old shape

No change.

Good, because no migration effort.
Bad, because integer map keys and keyword lists do not exist in JSON.

## More Information

The internal binding selector tuples (`{:as, atom}`, `{:at, integer}`) used between functions are unchanged by this decision. Only the user-facing params shape changes. The `BindingParams` module translates the new flat shape into the same internal tuples.

Revisit this decision if the library adds support for additional binding modes beyond `:as` and `:at`, or if the `:value` key in `with_ties` causes confusion with other use cases.

Related: ADR 0010 restructured `:source`/`:query` params to `:from` shape for similar JSON-friendliness reasons.
