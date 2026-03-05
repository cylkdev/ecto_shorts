# Extract Variant Module

## When to use

Use when one module contains conditional branches for one special case that keeps growing.

## Problem

Variant-specific behavior is buried in one broad module.

```elixir
defmodule Quote do
  def fee(order) do
    case order.channel do
      :marketplace -> order.total_cents * 0.12
      _ -> order.total_cents * 0.05
    end
  end
end
```

## Solution

Extract the special-case behavior into a dedicated variant module.

```elixir
defmodule Quote.Marketplace do
  def fee(order), do: order.total_cents * 0.12
end
```

## Why Refactor

- Isolates growing variant logic.
- Reduces branch complexity in the base module.

## How to Refactor

1. Identify stable variant branch.
2. Create variant module.
3. Move variant behavior.
4. Route calls appropriately.
5. Run formatter and tests.

## Validation

- Variant logic is isolated.
- Base module is simpler.
