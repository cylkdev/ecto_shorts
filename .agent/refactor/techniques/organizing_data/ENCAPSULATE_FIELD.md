# Encapsulate Field

## When to use

Use when any of the following are true:

- External modules read/write struct fields directly.
- You need validation or normalization before updates.
- Field representation may change.

## Problem

Direct field access leaks internal representation across module boundaries.

## Solution

Expose explicit API functions and avoid direct external mutation patterns.

```elixir
defmodule UserProfile do
  def display_name(%UserProfile{name: name}), do: name

  def rename(%UserProfile{} = profile, new_name) do
    %{profile | name: String.trim(new_name)}
  end
end
```

## Why Refactor

- Centralizes update rules.
- Protects module invariants.
- Reduces external coupling to struct layout.

## How to Refactor

1. Identify externally accessed fields.
2. Add explicit read/update functions.
3. Migrate callers to module API.
4. Remove direct external field mutation patterns.
5. Run formatter and tests.

## Validation

- Callers use API functions instead of raw field manipulation.
- Field invariants are enforced centrally.
- Tests pass.

## Eliminates Code Smell

- `Data Class`
- `Inappropriate Intimacy`
