---
trigger: model_decision
description: A function signature includes an unused argument (_unused) or callers always pass the same constant/derivable value. The parameter is ignored or can be computed from other inputs. Removing it reduces arity and call-site noise.
---

# Remove Parameter

## When to use

Use when any of the following are true:

- A parameter is unused.
- A parameter can be derived reliably inside the function.
- Callers pass redundant data everywhere.

## Problem

Function signature carries unnecessary arguments.

```elixir
def total(order, currency) do
  # currency is ignored
  order.subtotal_cents
end
```

## Solution

Remove redundant parameter and update call sites.

```elixir
def total(order), do: order.subtotal_cents
```

## Why Refactor

- Simplifies function API.
- Reduces caller noise.
- Makes intent clearer.

## How to Refactor

1. Verify parameter is truly unnecessary.
2. Remove it from signature.
3. Update all call sites.
4. Run formatter and tests.

## Validation

- Signature has no redundant parameters.
- Behaviour is unchanged.
- Tests pass.

## Eliminates Code Smell

- `Long Parameter List`
