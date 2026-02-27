---
trigger: model_decision
description: A shared/base module defines functions that only some variants ever call. Most variants never reference the function, but still depend on the shared module. Move variant-only behaviour into the variant module to keep the base small.
---

# Push Down Function

## When to use

Use when a function in a shared module is relevant to only some variants.

## Problem

A shared module defines behaviour unused by many variants.

```elixir
defmodule Payment.Shared do
  def export_bank_file(payment), do: ...
end
```

## Solution

Move the function to the module(s) where it is actually needed.

```elixir
defmodule BankPayment do
  def export_bank_file(payment), do: ...
end
```

## Why Refactor

- Keeps shared modules minimal and cohesive.
- Reduces accidental coupling.

## How to Refactor

1. Find rarely-applicable shared functions.
2. Move each to relevant variant module.
3. Update call sites.
4. Run formatter and tests.

## Validation

- Shared module no longer holds variant-only behaviour.
- Behaviour remains correct at call sites.
