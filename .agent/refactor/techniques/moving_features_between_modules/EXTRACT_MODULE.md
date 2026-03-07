# Extract Module

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- One module has multiple responsibilities.
- Different function groups in the same module change for different reasons.
- The module keeps growing with unrelated branches and helpers.
- You can name a coherent sub-domain.

## Problem

A single module mixes concerns that should evolve independently.

```elixir
defmodule Checkout do
  def total(order), do: ...
  def tax(order), do: ...
  def render_receipt(order), do: ...
  def deliver_receipt(order), do: ...
end
```

## Solution

Create a focused module for one responsibility and move related functions there.

```elixir
defmodule Checkout do
  def total(order), do: Checkout.Pricing.total(order)
  def tax(order), do: Checkout.Pricing.tax(order)
  def render_receipt(order), do: Checkout.Receipt.render(order)
  def deliver_receipt(order), do: Checkout.Receipt.deliver(order)
end

defmodule Checkout.Pricing do
  def total(order), do: ...
  def tax(order), do: ...
end

defmodule Checkout.Receipt do
  def render(order), do: ...
  def deliver(order), do: ...
end
```

## Why Refactor

This improves code in the following ways:

- Each module has one clear purpose.
- Changes in one area stop causing churn in unrelated code.
- Module names map cleanly to domain concepts.
- Tests target smaller behaviour surfaces.

## Benefits

- Smaller modules and clearer APIs.
- Easier ownership and review boundaries.
- Fewer merge conflicts on hotspot files.

## Drawbacks

- More modules to navigate.
- Temporary delegators may exist during migration.

## How to Refactor

1. Identify a cohesive slice inside a broad module.
2. Create a new module for that slice.
3. Move public and private functions for that responsibility.
4. Move related data shaping and validations.
5. Add temporary delegators if compatibility is needed.
6. Update call sites to the extracted module.
7. Remove temporary delegators.
8. Run formatter and tests.

## Validation

- Behaviour is unchanged (tests pass).
- The extracted module has one clear responsibility.
- The original module is materially smaller and more focused.

## Eliminates Code Smell

- `Duplicate Code`
- `Large Module`
- `Divergent Change`
- `Data Clumps`
- `Primitive Obsession`
- `Temporary Field`
- `Inappropriate Intimacy`

## Similar Refactoring Techniques

- `Replace Data Value with Struct`

## Anti-Refactoring

- `Inline Module`
