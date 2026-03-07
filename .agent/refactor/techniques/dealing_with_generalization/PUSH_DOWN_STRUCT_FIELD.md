# Push Down Struct Field

## When to use

Use when a field exists in a shared struct but is only used by specific variants.

## Problem

A broad shared struct contains fields irrelevant to many variants.

```elixir
defmodule Payment do
  defstruct [:id, :kind, :card_last4, :bank_account_suffix]
end
```

## Solution

Move specialized fields down to variant-specific structs.

```elixir
defmodule CardPayment do
  defstruct [:id, :card_last4]
end

defmodule BankPayment do
  defstruct [:id, :bank_account_suffix]
end
```

## Why Refactor

- Removes irrelevant fields from general types.
- Improves data clarity.

## How to Refactor

1. Identify fields used by only one variant.
2. Move them to variant-specific structs.
3. Update construction and access paths.
4. Run formatter and tests.

## Validation

- Shared struct has only shared fields.
- Variant-specific data remains available where needed.
