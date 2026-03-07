# Replace Type Code with State or Strategy

## When to use

Use when any of the following are true:

- Behaviour varies by runtime state or policy.
- New variants are expected over time.
- Large conditionals select algorithms repeatedly.

## Problem

Type code drives algorithm selection inline in many places.

## Solution

Use behaviour-driven strategy modules (or explicit state modules) and dispatch through a common callback contract.

```elixir
defmodule ShippingStrategy do
  @callback quote(map()) :: non_neg_integer()
end

defmodule ShippingStrategy.Ground do
  @behaviour ShippingStrategy
  def quote(order), do: order.weight_grams * 2
end
```

## Why Refactor

- Replaces branching with polymorphic module dispatch.
- Makes extension additive (new strategy module).
- Keeps state/policy logic modular.

## How to Refactor

1. Define a behaviour callback contract.
2. Implement one module per state/strategy.
3. Replace type-code branching with strategy module calls.
4. Inject/select strategy at runtime.
5. Run formatter and tests.

## Validation

- Branching is replaced by behaviour-based dispatch.
- New strategies can be added without editing core dispatcher.
- Tests cover each strategy module.

## Eliminates Code Smell

- `Switch Statements`
- `Divergent Change`
