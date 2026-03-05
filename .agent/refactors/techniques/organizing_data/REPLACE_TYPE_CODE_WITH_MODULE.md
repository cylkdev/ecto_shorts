# Replace Type Code with Module

## When to use

Use when any of the following are true:

- A type atom/string encodes behavior selection.
- `case` on type appears repeatedly.
- Type-specific rules belong to separate modules.

## Problem

Type code is stored as data, but behavior is scattered in conditionals.

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

- Moves behavior next to the type concept.
- Removes repeated branching.
- Improves extensibility.

## How to Refactor

1. Identify repeated type-code branching.
2. Create modules per type.
3. Move type-specific behavior into each module.
4. Replace branching with module dispatch.
5. Run formatter and tests.

## Validation

- Type behavior is implemented in dedicated modules.
- Repeated `case` on type is reduced or removed.
- Tests pass for each type module.

## Eliminates Code Smell

- `Switch Statements`
- `Primitive Obsession`
