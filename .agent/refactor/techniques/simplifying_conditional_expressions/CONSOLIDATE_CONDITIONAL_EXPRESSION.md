# Consolidate Conditional Expression

## When to use

Use when any of the following are true:

- Several conditions return the same result.
- Multiple early exits exist for one business decision.
- The same consequence is repeated across `if` branches.

## Problem

Equivalent outcomes are split across separate condition checks.

```elixir
if user.suspended do
  {:error, :inactive}
else
  if user.deleted_at !== nil do
    {:error, :inactive}
  else
    :ok
  end
end
```

## Solution

Combine related predicates into one explicit decision.

```elixir
def availability(user) do
  if inactive?(user), do: {:error, :inactive}, else: :ok
end

defp inactive?(user), do: user.suspended or user.deleted_at !== nil
```

## Why Refactor

- One decision is represented once.
- Repeated branch outcomes are removed.
- Condition intent becomes clearer.

## How to Refactor

1. Find conditions that produce the same outcome.
2. Combine them into one predicate function.
3. Replace duplicated branches with one conditional.
4. Run formatter and tests.

## Validation

- Same outcomes as before for all inputs.
- No duplicated consequence blocks remain.

## Eliminates Code Smell

- `Duplicate Code`
- `Complex Conditional`
