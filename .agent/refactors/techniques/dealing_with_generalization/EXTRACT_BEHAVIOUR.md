# Extract Behavior

## When to use

Use when multiple modules provide the same function contract with different implementations.

## Problem

Modules act like interchangeable strategies but no explicit contract exists.

```elixir
defmodule Shipping.Ground do
  def quote(order), do: order.weight_grams * 2
end

defmodule Shipping.Air do
  def quote(order), do: order.weight_grams * 5
end
```

## Solution

Define a behavior to formalize the shared API.

```elixir
defmodule ShippingStrategy do
  @callback quote(map()) :: non_neg_integer()
end
```

## Why Refactor

- Makes contracts explicit.
- Improves compile-time feedback.
- Clarifies pluggable module boundaries.

## How to Refactor

1. Define behavior callbacks.
2. Add `@behaviour` to implementations.
3. Align function signatures.
4. Run formatter and tests.

## Validation

- Implementations satisfy one explicit contract.
- Missing callbacks fail fast during development.
