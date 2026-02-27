---
trigger: model_decision
description: Two or more variant modules contain identical helper functions or pipeline segments. You see copy/pasted def bodies across modules with only names changed. A shared module would host the common behaviour once.
---

# Extract Shared Module

## When to use

Use when multiple modules duplicate a common subset of behaviour.

## Problem

Common logic is repeated across variants.

```elixir
defmodule PdfInvoice do
  def validate_currency(currency), do: currency in ["USD", "EUR"]
end

defmodule EmailInvoice do
  def validate_currency(currency), do: currency in ["USD", "EUR"]
end
```

## Solution

Extract shared behaviour into one module reused by variants.

```elixir
defmodule Invoice.Shared do
  def validate_currency(currency), do: currency in ["USD", "EUR"]
end
```

## Why Refactor

- Removes duplication.
- Centralizes common policy.

## How to Refactor

1. Extract truly common functions.
2. Create shared module.
3. Call shared module from variants.
4. Run formatter and tests.

## Validation

- Shared logic lives in one module.
- Variant modules keep only variant behaviour.
