# Replace Data Value with Struct

## When to use

Use when any of the following are true:

- A primitive value carries multiple domain rules.
- The same primitive validation/formatting logic is duplicated.
- A value should enforce invariants at creation time.

## Problem

A raw value (string/integer/map) is used where a domain concept should exist.

```elixir
def normalize_phone(phone), do: String.replace(phone, ~r/\D/, "")
```

## Solution

Introduce a dedicated struct and module for the value.

```elixir
defmodule PhoneNumber do
  defstruct [:digits]

  def new(raw) do
    digits = String.replace(raw, ~r/\D/, "")
    %__MODULE__{digits: digits}
  end
end
```

## Why Refactor

- Moves value-specific rules to one module.
- Creates explicit domain meaning.
- Prevents duplicated validation logic.

## How to Refactor

1. Create a struct for the domain value.
2. Add constructor/normalization functions.
3. Migrate call sites from primitive value to struct.
4. Remove old duplicated logic.
5. Run formatter and tests.

## Validation

- Value rules live in one module.
- Call sites use the struct consistently.
- Behavior is unchanged or intentionally improved with tests.

## Eliminates Code Smell

- `Primitive Obsession`
- `Duplicate Code`
