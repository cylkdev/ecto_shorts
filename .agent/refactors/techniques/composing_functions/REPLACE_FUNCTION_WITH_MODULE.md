# Replace Function with Module

## When to use

Use when any of the following are true:

- A function has 3 or more intermediate local bindings that are reused across later steps.

- Extracting one logical step into a private helper function would require passing 4 or more arguments.

- You want to extract sub-steps, but each extraction needs both:
  - several shared inputs, and
  - several previously computed intermediate values.
  
- The function contains multiple step groups and the same intermediate values are read in more than one group. A "step group" means a contiguous set of lines in one function clause that together perform one purpose (for example "print details", "calculate total", "build payload", "validate params").

- The function is doing one overall calculation, but local binding coordination is now the dominant complexity.

## Problem

A single function clause performs one overall operation, but the implementation depends on many intermediate bindings that must be shared across later steps.

```elixir
defmodule Order do
  def price(order) do
    primary_base_price = base_price(order, :primary)
    secondary_base_price = base_price(order, :secondary)
    tertiary_base_price = base_price(order, :tertiary)

    # Long calculation using many intertwined local bindings...
    primary_base_price + secondary_base_price + tertiary_base_price
  end

  defp base_price(_order, _kind), do: 0
end
```

## Solution

Move the calculation into a dedicated module that owns the operation state.

In Elixir, the idiomatic version of the "method object" refactoring is usually a focused module (often with a struct) that carries inputs and intermediate results across small functions.

The original function becomes a thin wrapper that initializes the state and delegates to the new module.

```elixir
defmodule Order do
  def price(order) do
    order
    |> Order.PriceCalculator.new()
    |> Order.PriceCalculator.run()
  end
end

defmodule Order.PriceCalculator do
  defstruct [
    :order,
    :primary_base_price,
    :secondary_base_price,
    :tertiary_base_price
  ]

  def new(order) do
    %__MODULE__{
      order: order,
      primary_base_price: base_price(order, :primary),
      secondary_base_price: base_price(order, :secondary),
      tertiary_base_price: base_price(order, :tertiary)
    }
  end

  def run(%__MODULE__{} = state) do
    state
    |> apply_primary_adjustments()
    |> apply_secondary_adjustments()
    |> total()
  end

  defp apply_primary_adjustments(%__MODULE__{} = state), do: state
  defp apply_secondary_adjustments(%__MODULE__{} = state), do: state

  defp total(%__MODULE__{} = state) do
    state.primary_base_price +
      state.secondary_base_price +
      state.tertiary_base_price
  end

  defp base_price(_order, _kind), do: 0
end
```

## Why Refactor

This improves code in the following ways:

- It reduces the number of shared local bindings in the original clause.

- It avoids argument-list explosion when extracting helper functions.

- It gives the calculation a focused module boundary.

- It makes intermediate values explicit as struct fields instead of hidden local rebinding coordination.

- It enables smaller private functions that each operate on the same state shape.

## Benefits

- The original module becomes simpler.

- The calculation gets a focused module.

- Tangled local bindings become explicit struct fields, which makes refactoring easier.

## Drawbacks

- You introduce another module (and often a struct), which adds some structure and indirection.

## How to Refactor

1. Identify a function or clause where local binding coordination is the dominant complexity.

2. Create a new module named for the operation (for example `Order.PriceCalculator`).

3. Define a struct for:
   - required inputs
   - intermediate values that must be reused across steps

4. Add a constructor function (for example `new/1`, `new/2`) that initializes the struct.

5. Move the original function logic into a main entry function in the new module (for example `run/1`).

6. Replace shared local bindings with struct fields.

7. Split the logic into small private functions that accept and return the state struct.

8. Update the original function to delegate to the new module.

9. Run formatter and tests after each extraction step.

## Similar Refactoring Techniques

- `Replace Data Value with Object`

## Eliminates Code Smell

- `Long Function`
