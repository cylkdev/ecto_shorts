# Replace Type Code with Module Variants

## When to use

Use when any of the following are true:

- A type code has a fixed closed set of variants.
- Each variant requires different behavior.
- You want explicit modules per variant.

## Problem

One module handles many variant branches with growing conditional logic.

## Solution

Create a module per variant and route behavior to the right module.

```elixir
defmodule Discount.Standard do
  def apply(amount), do: amount
end

defmodule Discount.Loyalty do
  def apply(amount), do: amount * 0.95
end
```

## Why Refactor

- Makes each variant explicit.
- Keeps variant logic isolated.
- Reduces branch-heavy functions.

## How to Refactor

1. Enumerate stable variants.
2. Create one module per variant.
3. Move behavior from `case` branches to each module.
4. Route callers to variant modules.
5. Run formatter and tests.

## Validation

- Variant-specific logic is split by module.
- Core flow no longer contains large variant `case` blocks.
- Tests cover each variant.

## Eliminates Code Smell

- `Switch Statements`
- `Long Function`
