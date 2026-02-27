---
trigger: model_decision
description: Reads Application.get_env / fetch_env! inside business logic. Reads process dictionary / module attribute / global state. Uses hidden configuration instead of function arguments. Function needs data not present in its signature. Test setup requires implicit config mutation.
---

# Add Parameter

## When to use

Use when any of the following are true:

- A function needs new input to implement required behaviour.
- Hidden global/config state is being read implicitly.
- Callers need to control behaviour explicitly.

## Problem

Function logic depends on data that is not part of the function contract.

```elixir
def quote(order) do
  tax_rate_bps = Application.fetch_env!(:billing, :tax_rate_bps)
  order.subtotal_cents + div(order.subtotal_cents * tax_rate_bps, 10_000)
end
```

## Solution

Add an explicit parameter (or options keyword) and pass required value from call sites.

```elixir
def quote(order, tax_rate_bps) do
  order.subtotal_cents + div(order.subtotal_cents * tax_rate_bps, 10_000)
end
```

## Why Refactor

- Makes dependencies explicit.
- Improves testability and determinism.
- Reduces hidden coupling.

## How to Refactor

1. Add the new parameter to function signature.
2. Update direct callers.
3. Add defaults via options only when appropriate.
4. Run formatter and tests.

## Validation

- Required input is explicit in signature.
- Callers compile with updated arity.
- Tests pass.

## Eliminates Code Smell

- `Hidden Dependency`
