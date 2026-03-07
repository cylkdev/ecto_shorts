# Replace New with Factory Function

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- Struct creation needs naming beyond raw `%Struct{}` literals.
- Creation paths require validation/default logic.
- Different creation modes need clear entry points.

## Problem

Callers construct structs directly and duplicate creation logic.

```elixir
def checkout_total(amount_cents) do
  %Money{amount_cents: amount_cents, currency: "USD"}
end
```

## Solution

Add explicit factory functions (commonly `new/1`, plus named constructors) and route callers through them.

```elixir
defmodule Money do
  defstruct [:amount_cents, :currency]

  def new(amount_cents, currency) do
    %__MODULE__{
      amount_cents: amount_cents,
      currency: currency
    }
  end

  def zero(currency) do
    %__MODULE__{
      amount_cents: 0,
      currency: currency
    }
  end
end
```

## Why Refactor

- Centralizes creation rules.
- Improves readability of creation intent.
- Supports multiple creation variants cleanly.

## How to Refactor

1. Identify direct struct constructions with repeated rules.
2. Introduce factory functions.
3. Migrate callers.
4. Keep `%Struct{}` construction internal where possible.
5. Run formatter and tests.

## Validation

- Creation logic is centralized in factory functions.
- Callers use factory API.
- Tests pass.

## Eliminates Code Smell

- `Duplicate Code`
- `Primitive Obsession`
