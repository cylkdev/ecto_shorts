# Encapsulate Collection

## When to use

Use when any of the following are true:

- External modules manipulate internal lists/maps directly.
- Collection updates require invariants (no duplicates, sorted, filtered).
- You need controlled add/remove semantics.

## Problem

Callers modify a collection field directly, bypassing rules.

```elixir
%Team{member_ids: ids ++ [new_id]}
```

## Solution

Provide module functions for collection operations and enforce invariants there.

```elixir
defmodule Team do
  def add_member(%Team{member_ids: ids} = team, member_id) do
    ids = ids |> Enum.uniq() |> Kernel.++([member_id]) |> Enum.uniq()
    %{team | member_ids: ids}
  end
end
```

## Why Refactor

- Centralizes collection invariants.
- Prevents accidental invalid states.
- Reduces duplicated update logic.

## How to Refactor

1. Identify direct external collection mutation.
2. Add explicit add/remove/update functions.
3. Enforce invariants in those functions.
4. Migrate callers.
5. Run formatter and tests.

## Validation

- Collection mutations go through module API.
- Invariants are maintained in one place.
- Behavior is covered by tests.

## Eliminates Code Smell

- `Data Class`
- `Duplicate Code`
