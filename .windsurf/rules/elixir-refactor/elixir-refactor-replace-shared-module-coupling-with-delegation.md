---
trigger: model_decision
description: A module uses a shared base (use BaseModule / __using__) mainly to reuse one collaborator or helper. Inheritance-style coupling forces the module into a hierarchy it doesn't need. Replace with explicit delegation to the needed collaborator module.
---

# Replace Shared Module Coupling with Delegation

## When to use

Use when modules are tightly coupled through forced shared structure but composition would be clearer.

## Problem

A module is forced through a shared generalization path even though it only needs to reuse one collaborator.

```elixir
defmodule Billing.SpecialQuote do
  use Billing.BaseQuote
end
```

## Solution

Replace that coupling with explicit delegation to a collaborator module.

```elixir
defmodule Billing.SpecialQuote do
  def total(order), do: Billing.QuoteCalculator.total(order)
end
```

## Why Refactor

- Reduces structural coupling.
- Makes dependencies explicit.
- Improves local flexibility.

## How to Refactor

1. Identify shared-module coupling used only for reuse.
2. Extract reusable logic to collaborator module.
3. Delegate explicitly from caller module.
4. Remove unnecessary shared coupling.
5. Run formatter and tests.

## Validation

- Module depends on explicit collaborators.
- Behaviour is unchanged.
