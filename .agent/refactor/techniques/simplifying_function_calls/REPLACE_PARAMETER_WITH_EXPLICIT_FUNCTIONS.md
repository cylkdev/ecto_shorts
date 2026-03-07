# Replace Parameter with Explicit Functions

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- A boolean/type parameter selects fundamentally different behaviour.
- Callers pass flags like `true/false` that hide intent.
- Branches are clearer as named entry points.

## Problem

One function uses a control parameter to pick behaviour.

```elixir
def booking_fee(amount, :premium), do: amount * 0.03

def booking_fee(amount, :standard), do: amount * 0.05
```

## Solution

Expose separate explicit functions for each intent.

```elixir
def premium_booking_fee(amount), do: amount * 0.03

def standard_booking_fee(amount), do: amount * 0.05
```

## Why Refactor

- Improves call-site clarity.
- Removes branch-selection arguments.
- Prevents invalid parameter combinations.

## How to Refactor

1. Identify control parameters selecting behaviour.
2. Create explicit functions per variant.
3. Redirect callers to explicit functions.
4. Remove old selector parameter API.
5. Run formatter and tests.

## Validation

- Call sites express intent by function name.
- Selector parameter is removed.
- Tests pass.

## Eliminates Code Smell

- `Long Parameter List`
- `Switch Statements`
