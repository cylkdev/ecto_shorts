---
trigger: model_decision
description: A struct/map stores a type atom/string (kind/type) and code branches on it in multiple functions. Behaviour for the type is scattered across case statements. Replace type-code data with module dispatch (store/pass the module and call module.fee/1, etc).
---

# Replace Type Code with Module

## When to use

Use when any of the following are true:

- A type atom/string encodes behaviour selection.
- `case` on type appears repeatedly.
- Type-specific rules belong to separate modules.

## Problem

Type code is stored as data, but behaviour is scattered in conditionals.

```elixir
def fee(type, amount) do
  case type do
    :regular -> amount * 0.10
    :premium -> amount * 0.06
  end
end
```

## Solution

Replace type code branches with module-based dispatch.

```elixir
def fee(module, amount), do: module.fee(amount)
```

Where `module` is `Pricing.Regular` or `Pricing.Premium`.

## Why Refactor

- Moves behaviour next to the type concept.
- Removes repeated branching.
- Improves extensibility.

## How to Refactor

1. Identify repeated type-code branching.
2. Create modules per type.
3. Move type-specific behaviour into each module.
4. Replace branching with module dispatch.
5. Run formatter and tests.

## Validation

- Type behaviour is implemented in dedicated modules.
- Repeated `case` on type is reduced or removed.
- Tests pass for each type module.

## Eliminates Code Smell

- `Switch Statements`
- `Primitive Obsession`
