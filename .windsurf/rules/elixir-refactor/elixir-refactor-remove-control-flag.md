---
trigger: model_decision
description: Flow uses a manual flag/accumulator to stop later logic (found?, done) instead of direct Enum control. You see Enum.reduce/3 returning {flag, result} tuples or complex reduce_while candidates. Replace with Enum.find/2, Enum.any?/2, reduce_while, or early-return clauses.
---

# Remove Control Flag

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

- `Complex Conditional`
- `Temporary Field`
