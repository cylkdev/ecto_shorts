# Separate Query from Modifier

## When to use

Use when any of the following are true:

- One function both changes state and returns queried data.
- Callers depend on side effects and return value at once.
- Order-dependent behaviour is hard to reason about.

## Problem

A function mixes read intent and write intent.

```elixir
def deactivate_if_inactive?(user) do
  inactive = user.last_seen_at < DateTime.add(DateTime.utc_now(), -30, :day)
  updated_user = if inactive, do: %{user | status: :inactive}, else: user
  {inactive, updated_user}
end
```

## Solution

Split into two functions: one pure query, one modifier/update operation.

```elixir
def user_active?(user), do: user.status == :active

def deactivate_user(user), do: %{user | status: :inactive}
```

## Why Refactor

- Clarifies side effects.
- Improves composability and testability.
- Reduces accidental mutation assumptions.

## How to Refactor

1. Identify mixed read/write function.
2. Extract pure query behaviour.
3. Keep modification in separate function.
4. Update callers to use correct function.
5. Run formatter and tests.

## Validation

- Query function has no side effects.
- Modifier function is explicit about state changes.
- Tests pass.

## Eliminates Code Smell

- `Command-Query Separation Violation`
