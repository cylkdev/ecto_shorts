# Replace Conditional with Polymorphism

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- A `case` on kind/type repeatedly selects behaviour.
- New variants require editing existing branching functions.
- Variant behaviour can be expressed through module dispatch or protocol implementations.

## Problem

Type-based branching spreads across functions.

```elixir
def shipping_quote(method, order) do
  case method do
    :ground -> order.weight_grams * 2
    :air -> order.weight_grams * 5
  end
end
```

## Solution

Move variant behaviour into dedicated modules behind a shared contract.

```elixir
defmodule ShippingMethod do
  @callback quote(map()) :: non_neg_integer()
end

defmodule ShippingMethod.Ground do
  @behaviour ShippingMethod
  def quote(order), do: order.weight_grams * 2
end

defmodule ShippingMethod.Air do
  @behaviour ShippingMethod
  def quote(order), do: order.weight_grams * 5
end
```

## Why Refactor

- Adds new variants without editing central branching.
- Keeps variant logic isolated.
- Reduces large conditional blocks.

## How to Refactor

1. Define a behaviour callback contract.
2. Implement one module per variant.
3. Replace branching with module dispatch.
4. Migrate callers gradually.
5. Run formatter and tests.

## Validation

- Variant-specific branching is minimized or removed.
- Each variant module is tested independently.
- Behaviour is preserved.

## Eliminates Code Smell

- `Switch Statements`
- `Divergent Change`
