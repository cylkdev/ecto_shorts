# Remove Middle Man

## When to use

Use when any of the following are true:

- A module mostly delegates to another module without adding policy.
- Wrapper functions change whenever the target API changes.
- Callers are clearer with direct calls to the owning module.
- The delegating layer exists only for historical reasons.

## Problem

A module is mostly pass-through code.

```elixir
defmodule ProjectService do
  def list(user), do: Projects.list(user)
  def get(user, id), do: Projects.get(user, id)
  def archive(user, id), do: Projects.archive(user, id)
end
```

## Solution

Remove pure pass-through functions and call the target module directly.

```elixir
# before
ProjectService.archive(user, id)

# after
Projects.archive(user, id)
```

Keep delegators only when they add policy, authorization, telemetry, or compatibility behavior.

## Why Refactor

This improves code in the following ways:

- Shortens call paths.
- Reduces duplicated public APIs.
- Removes wrapper churn.
- Clarifies module ownership.

## Benefits

- Lower maintenance overhead.
- Fewer stale wrapper docs/tests.
- Cleaner dependency graph.

## Drawbacks

- Large migrations can touch many call sites.
- If the wrapper provided meaningful facade behavior, removing it may hurt readability.

## How to Refactor

1. List delegator functions and classify which add real behavior.
2. Keep only delegators with clear policy value.
3. Migrate callers of pass-through functions to direct calls.
4. Remove obsolete delegators.
5. Run formatter and tests.

## Validation

- Removed delegators had no unique behavior.
- Callers compile and tests pass with direct calls.
- Remaining delegators have explicit justification.

## Eliminates Code Smell

- `Middle Module`

## Similar Refactoring Techniques

- `Hide Delegate`
