# Remove Control Flag

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- A boolean variable controls loop/flow exit manually.
- State is toggled (`found = true`) only to stop later logic.
- Flow can be expressed with `Enum.find`, `Enum.reduce_while`, pattern matching, or early returns.

## Problem

A control flag obscures the real exit condition.

```elixir
found = false
result = nil

Enum.each(items, fn item ->
  if not found and valid?(item) do
    found = true
    result = item
  end
end)
```

## Solution

Replace control-flag flow with direct control constructs.

```elixir
Enum.find(items, fn item -> valid?(item) end)
```

Or with `Enum.reduce_while/3` when richer control is needed.

## Why Refactor

- Makes exit condition explicit.
- Removes mutable-style bookkeeping.
- Simplifies reasoning about flow.

## How to Refactor

1. Locate flags used only for flow control.
2. Replace with direct Enum/pattern-matching control.
3. Remove flag variable and related branches.
4. Run formatter and tests.

## Validation

- Control flags are removed.
- Flow is expressed directly by language/library constructs.
- Behaviour is unchanged.

## Eliminates Code Smell

- `Switch Statements`
- `Temporary Field`
