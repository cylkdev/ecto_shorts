# Introduce Local Wrapper

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- External-type helper logic appears in many modules.
- Your app needs domain-specific behaviour around a library type.
- You want a stable local API that can outlive dependency API changes.
- Multiple teams need one canonical way to work with that external type.

## Problem

External library types are used directly everywhere, and app-specific behaviour is duplicated.

## Solution

Create a local wrapper module (optionally with a wrapper struct) that owns app-specific behaviour and delegates to the external library.

```elixir
defmodule MoneyAmount do
  defstruct [:decimal, :currency]

  def new(decimal, currency), do: %__MODULE__{decimal: decimal, currency: currency}

  def display(%__MODULE__{decimal: decimal, currency: currency}) do
    "#{currency} " <> Decimal.to_string(decimal, :normal)
  end

  def positive?(%__MODULE__{decimal: decimal}) do
    Decimal.compare(decimal, Decimal.new(0)) === :gt
  end
end
```

## Why Refactor

This improves code in the following ways:

- Gives external-type customizations one local home.
- Removes duplicated helper code across modules.
- Creates a stable boundary around dependency usage.
- Makes dependency swaps easier later.

## Benefits

- Consistent naming and semantics across the codebase.
- Easier testing of domain-specific behaviour.
- Better control over dependency coupling.

## Drawbacks

- Adds one abstraction layer.
- Requires caller migration and discipline.

## How to Refactor

1. Inventory repeated external-type helper patterns.
2. Design a small wrapper API with domain names.
3. Implement wrapper module/struct.
4. Move duplicated helpers into the wrapper.
5. Migrate callers incrementally.
6. Remove old scattered helpers.
7. Run formatter and tests.

## Validation

- Callers use the wrapper API for app-specific behaviour.
- Duplicated helper logic is removed.
- Behaviour is unchanged (tests pass).
- Dependency usage is centralized.

## Eliminates Code Smell

- `Duplicate Code`

## Similar Refactoring Techniques

- `Introduce External-Type Function`
