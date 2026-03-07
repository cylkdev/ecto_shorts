# Replace Specialized Modules with Fields

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- Multiple modules differ only by data values, not behaviour.
- Variant modules add file/module overhead with little value.
- Runtime branching can be expressed by fields and simple rules.

## Problem

Many specialized modules exist, but their behaviour is effectively the same.

## Solution

Collapse specialized modules into one struct/module and represent differences as fields.

```elixir
defmodule EmployeeType do
  defstruct [:kind, :bonus_bps]
end
```

## Why Refactor

- Removes unnecessary module proliferation.
- Keeps shared behaviour in one place.
- Represents differences as explicit data.

## How to Refactor

1. Confirm variant modules have near-identical behaviour.
2. Define fields that capture the differences.
3. Merge shared behaviour into one module.
4. Migrate callers from module variant to data fields.
5. Remove old specialized modules.
6. Run formatter and tests.

## Validation

- Removed modules differed only by data.
- Behaviour is preserved with field-driven logic.
- Tests cover all former variants.

## Eliminates Code Smell

- `Speculative Generality`
- `Lazy Module`
