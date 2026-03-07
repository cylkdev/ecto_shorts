# Push Down Function

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

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
