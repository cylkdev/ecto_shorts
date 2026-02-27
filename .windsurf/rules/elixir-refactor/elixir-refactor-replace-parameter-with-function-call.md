---
trigger: model_decision
description: Callers compute a derived argument the same way before calling a function. The callee could derive it from existing inputs (tax_cents from order.subtotal). Remove the derived parameter and compute it internally in a helper.
---

# Replace Parameter with Function Call

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
