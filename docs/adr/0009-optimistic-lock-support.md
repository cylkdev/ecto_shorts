# Automatic optimistic locking in update/4 and find_and_update/4

---
Status: accepted
Date: 2026-03-01
Deciders: []
Consulted: []
Informed: []
---

## Context and Problem Statement

`EctoShorts.Actions.update/4` and `find_and_update/4` had no built-in support for optimistic locking. Users who needed concurrent-update safety had to call `Ecto.Changeset.optimistic_lock/3` manually inside a custom changeset function and handle `Ecto.StaleEntryError` themselves. This created boilerplate and made it easy to forget locking on schemas that require it.

The question was: how can we add opt-in optimistic locking to the existing update functions without breaking backward compatibility or adding complexity for users who do not need it?

## Decision Drivers

* Backward compatibility - existing callers must not change behavior.
* Discoverability - a schema should be able to declare its own locking requirement so callers do not need to remember.
* Override control - callers must be able to enable, disable, or change locking per call.
* Consistent error contract - stale entries must return an error tuple, not raise an exception, to match the existing `{:ok, _} | {:error, _}` contract.
* Minimal surface area - the change should be confined to `update/4` and private helpers.

## Considered Options

1. Schema callback auto-detection with option override (chosen).
2. Only support the `:optimistic_lock` option (no auto-detection).
3. Require users to handle locking in custom changeset functions (status quo).

## Decision Outcome

Chosen option: "Schema callback auto-detection with option override", because it satisfies all five decision drivers. Schemas that need locking declare it once via `optimistic_lock/0`. Callers can override or disable per call. The error contract is preserved by rescuing `Ecto.StaleEntryError`.

### Consequences

Good, because schemas that define `optimistic_lock/0` get automatic locking on every update without caller changes.

Good, because the `:optimistic_lock` option gives full runtime control, including disabling auto-detection with `false`.

Good, because stale entries return `{:error, %ErrorMessage{code: :stale}}` instead of raising, which is consistent with the existing error contract.

Bad, because `update/4` now has a `try/rescue` block, which adds a small amount of complexity to the function body.

Bad, because schemas must add a new `optimistic_lock/0` function to opt in, which is a convention specific to EctoShorts (not an Ecto standard).

## Validation

Run the optimistic locking tests:

    mix test test/ecto_shorts/actions_test.exs --seed 0 --trace

Look for the following describe blocks, all of which must pass:

* "update/4 optimistic locking via schema callback"
* "update/4 optimistic locking via option"
* "update/4 optimistic locking on schema without callback"
* "find_and_update/4 optimistic locking"

Verify backward compatibility by confirming the full suite passes:

    mix test --seed 0 --trace

## Pros and Cons of the Options

### Schema callback auto-detection with option override

The schema module exports `optimistic_lock/0` returning a field atom or `{field, incrementer}` tuple. `update/4` checks for this callback via `function_exported?/3`. The `:optimistic_lock` option in `opts` overrides or disables the callback.

Good, because schemas declare their locking requirement once.
Good, because callers can override or disable per call.
Good, because `find_and_update/4` and `find_and_upsert/4` inherit locking through `update/4`.
Bad, because it introduces a new convention (`optimistic_lock/0`) that users must learn.

### Only support the :optimistic_lock option

No auto-detection. Callers must pass `optimistic_lock: :lock_version` on every call that needs locking.

Good, because there is no magic - everything is explicit.
Bad, because it is easy to forget the option, defeating the purpose of safety.
Bad, because every call site that updates a lockable schema must remember the option.

### Require users to handle locking in custom changeset functions

Users call `Ecto.Changeset.optimistic_lock/3` inside their changeset and rescue `Ecto.StaleEntryError` at the call site.

Good, because no changes to EctoShorts are needed.
Bad, because it requires boilerplate at every call site.
Bad, because the `Ecto.StaleEntryError` exception breaks the `{:ok, _} | {:error, _}` contract unless the user wraps every call in a try/rescue.

## More Information

The implementation lives in `lib/ecto_shorts/actions.ex`:

* `update/4` struct clause (lines ~1044-1056) applies `maybe_apply_optimistic_lock/3` to the changeset and rescues `Ecto.StaleEntryError`.
* `maybe_apply_optimistic_lock/3` and `resolve_optimistic_lock/2` are private helpers at the bottom of the module.

The test schema `EctoShorts.Schema.PostWithLock` in `test/support/schema/post_with_lock.ex` demonstrates the `optimistic_lock/0` callback pattern.

Revisit this decision if Ecto adds a built-in mechanism for declaring optimistic lock fields on schemas, which would make the custom callback unnecessary.
