# Extract Shared Module

## When to use

Use when multiple modules duplicate a common subset of behavior.

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

Extract shared behavior into one module reused by variants.

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
- Variant modules keep only variant behavior.
