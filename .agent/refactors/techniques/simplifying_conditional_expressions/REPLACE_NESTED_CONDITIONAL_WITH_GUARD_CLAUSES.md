# Replace Nested Conditional with Guard Clauses

## When to use

Use when any of the following are true:

- Nested `if`/`case` blocks create deep indentation.
- Exceptional or edge cases hide the normal flow.
- Early-return conditions are clear and independent.

## Problem

Nested conditionals obscure the main path.

```elixir
def payout(employee) do
  if employee.active do
    if employee.on_probation do
      0
    else
      employee.salary
    end
  else
    0
  end
end
```

## Solution

Use guard-style early clauses so exceptional paths return first and main flow stays flat.

```elixir
def payout(%{active: false}), do: 0

def payout(%{on_probation: true}), do: 0

def payout(employee), do: employee.salary
```

## Why Refactor

- Flattens control flow.
- Highlights normal behavior.
- Reduces indentation and branching complexity.

## How to Refactor

1. Identify edge/exception conditions.
2. Move them to early function clauses (or early returns in order).
3. Leave the happy path last.
4. Run formatter and tests.

## Validation

- Nesting depth is reduced.
- Main path is obvious.
- Behavior is unchanged.

## Eliminates Code Smell

- `Complex Conditional`
- `Long Function`
