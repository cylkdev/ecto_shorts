# Replace Parameter with Function Call

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- A passed parameter is always computed from other arguments.
- Callers repeat the same derivation before every call.
- Derivation belongs closer to the callee logic.

## Problem

Callers compute and pass derived data redundantly.

```elixir
def total_with_tax(subtotal_cents, tax_cents), do: subtotal_cents + tax_cents
```

## Solution

Move derivation into the callee via helper function.

```elixir
def total_with_tax(order), do: order.subtotal_cents + tax_cents(order)

defp tax_cents(order), do: div(order.subtotal_cents * order.tax_rate_bps, 10_000)
```

## Why Refactor

- Removes repetitive caller computations.
- Centralizes derivation logic.
- Reduces caller/callee mismatch risk.

## How to Refactor

1. Confirm parameter is derived from existing inputs.
2. Remove derived parameter from signature.
3. Compute value internally via helper function.
4. Update callers.
5. Run formatter and tests.

## Validation

- Derived parameter is removed from API.
- Derivation exists once in callee.
- Tests pass.

## Eliminates Code Smell

- `Long Parameter List`
- `Duplicate Code`
