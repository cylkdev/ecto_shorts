# Inline Module

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- A small module has real logic, but that logic only makes sense inside one owning module.
- The extracted module never developed an independent domain boundary.
- The module has very few call sites and no stable public contract.
- Keeping the module separate adds file/module overhead without improving clarity.

## Problem

A tiny module still exists as its own boundary, even though its behaviour is tightly coupled to one owner module.

```elixir
defmodule Checkout.FeeBreakdown do
  def compute(order) do
    subtotal = Enum.reduce(order.lines, 0, fn line, acc -> acc + line.price_cents end)
    tax = round(subtotal * 0.07)
    %{subtotal_cents: subtotal, tax_cents: tax}
  end

  def total(%{subtotal_cents: subtotal, tax_cents: tax}), do: subtotal + tax
end

defmodule Checkout.Pricing do
  def total_cents(order) do
    breakdown = Checkout.FeeBreakdown.compute(order)
    Checkout.FeeBreakdown.total(breakdown)
  end
end
```

## Solution

Move the behaviour into the owning module, keep internal helpers private, and remove the inlined module.

```elixir
defmodule Checkout.Pricing do
  def total_cents(order) do
    breakdown = compute_fee_breakdown(order)
    total_from_breakdown(breakdown)
  end

  defp compute_fee_breakdown(order) do
    subtotal = Enum.reduce(order.lines, 0, fn line, acc -> acc + line.price_cents end)
    tax = round(subtotal * 0.07)
    %{subtotal_cents: subtotal, tax_cents: tax}
  end

  defp total_from_breakdown(%{subtotal_cents: subtotal, tax_cents: tax}), do: subtotal + tax
end
```

## Why Refactor

This improves code in the following ways:

- Removes a weak boundary that does not carry independent meaning.
- Keeps tightly related behaviour in one module.
- Reduces cross-module jumps for a single workflow.
- Makes ownership of the behaviour explicit.

## Benefits

- Fewer modules to maintain.
- Easier navigation for related logic.
- Clearer ownership for future changes.

## Drawbacks

- If inlined too aggressively, the owner module can become too broad.
- You may need to re-extract later if the inlined logic grows into a separate concern.

## How to Refactor

1. Confirm the candidate module has no meaningful independent boundary.
2. Move its functions into the owning module.
3. Convert moved functions to `defp` when they are internal implementation details.
4. Update call sites inside the owner to local function calls.
5. Delete the old module and file.
6. Run formatter and tests.

## Validation

- Behaviour is unchanged (tests pass).
- The old module no longer exists.
- Related workflow logic is now coherent in one module.
- The owner module remains within acceptable complexity.

## Eliminates Code Smell

- `Lazy Module`
- `Speculative Generality`

## Similar Refactoring Techniques

- `Extract Module`
- `Move Function`

## Anti-Refactoring

- `Extract Module`
