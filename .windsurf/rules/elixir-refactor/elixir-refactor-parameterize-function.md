---
trigger: model_decision
description: Two or more functions share the same body but differ only by a literal (rate, threshold, atom). Names encode the constant (five_percent_discount/1) and logic is duplicated. Introduce a parameter for the varying value.
---

# Parameterize Function

## When to use

Use when any of the following are true:

- Several functions share identical logic except one value.
- Duplicate branches differ only by constant values.
- You want one reusable implementation.

## Problem

Near-identical functions differ only by literals.

```elixir
def five_percent_discount(total), do: total * 0.95

def ten_percent_discount(total), do: total * 0.90
```

## Solution

Create one function with a parameter for the varying part.

```elixir
def discount(total, rate), do: total * (1 - rate)
```

## Why Refactor

- Removes duplication.
- Keeps behaviour changes centralized.
- Makes variation explicit.

## How to Refactor

1. Identify duplicated logic with one varying element.
2. Introduce parameter for variation.
3. Replace duplicates with parameterized function calls.
4. Run formatter and tests.

## Validation

- Duplicate functions are removed or reduced.
- Variation is explicit in parameters.
- Tests pass.

## Eliminates Code Smell

- `Duplicate Code`
