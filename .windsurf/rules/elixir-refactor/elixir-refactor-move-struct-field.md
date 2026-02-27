---
trigger: model_decision
description: A struct/schema field is primarily read/updated by another module and its rules live elsewhere. Validation/default logic for the field is duplicated outside the struct's owner. Moving the field to the owning domain reduces cross-module coupling.
---

# Move Struct Field

## When to use

Use when any of the following are true:

- A struct field is read and updated mostly by a different module.
- The field conceptually belongs to another aggregate.
- Validation or defaulting logic for that field is duplicated across modules.
- The field exists only so another module can read it.

## Problem

A field is stored on one struct but belongs to another module's domain.

```elixir
defmodule Order do
  defstruct [:id, :customer, :preferred_currency]
end

defmodule Billing do
  def invoice_currency(%Order{preferred_currency: currency}), do: currency
end
```

## Solution

Move the field to the owning struct and migrate reads/writes.

```elixir
defmodule Customer do
  defstruct [:id, :preferred_currency]
end

defmodule Order do
  defstruct [:id, :customer]
end

defmodule Billing do
  def invoice_currency(%Order{customer: %Customer{preferred_currency: currency}}), do: currency
end
```

## Why Refactor

This improves code in the following ways:

- Data ownership matches domain ownership.
- Validation and normalization become centralized.
- Data flow is clearer across module boundaries.
- Ecto schema boundaries become easier to maintain.

## Benefits

- Better cohesion for structs and schemas.
- Fewer inconsistent writes to related data.
- Clearer write APIs.

## Drawbacks

- Persisted data may require migrations and backfills.
- Payload contracts may need coordinated updates.

## How to Refactor

1. Identify the misplaced field.
2. Add the field to the target struct or schema.
3. Migrate read paths first.
4. Migrate writes, validations, and defaults.
5. If persisted, run safe migration/backfill steps.
6. Remove the old field.
7. Run formatter and tests.

## Validation

- Behaviour is unchanged from a caller perspective.
- All reads/writes use the field in its new owner.
- No code references the old field location.

## Eliminates Code Smell

- `Feature Envy`
- `Divergent Change`

## Similar Refactoring Techniques

- `Move Function`
- `Extract Module`
