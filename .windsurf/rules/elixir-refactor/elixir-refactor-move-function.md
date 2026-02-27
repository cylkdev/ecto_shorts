---
trigger: model_decision
description: A function in module A pattern-matches on or reads fields from module B's struct and mostly calls B's helpers. Changes to B's data shape force edits in A. The behaviour conceptually belongs next to B's data and policies.
---

# Move Function

## When to use

Use when any of the following are true:

- A function in module `A` mostly reads data owned by module `B`.
- Most edits to the function are triggered by changes in `B`'s struct shape or rules.
- The function primarily calls helpers in `B`.
- The function name expresses behaviour that belongs to `B`'s domain.

## Problem

A function is defined in one module but depends more on another module's data and policy.

```elixir
defmodule Order do
  def shipping_zone(%Order{customer: customer}) do
    case customer.country_code do
      "US" -> :domestic
      "CA" -> :near_international
      _ -> :international
    end
  end
end
```

## Solution

Move the function to the module that owns the data and business rule. Keep a temporary delegator only while call sites migrate.

```elixir
defmodule Customer do
  def shipping_zone(%Customer{country_code: country_code}) do
    case country_code do
      "US" -> :domestic
      "CA" -> :near_international
      _ -> :international
    end
  end
end

defmodule Order do
  def shipping_zone(%Order{customer: customer}), do: Customer.shipping_zone(customer)
end
```

## Why Refactor

This improves code in the following ways:

- Rules live near the struct fields they use.
- The source module loses unrelated responsibility.
- Future changes stay local to the owning module.
- Cross-module coupling is reduced.

## Benefits

- Better module cohesion.
- Lower risk of feature envy between modules.
- Simpler tests because behaviour and data are colocated.

## How to Refactor

1. Find a function that mostly depends on another module's data/rules.
2. Copy it to the target module and adapt the function head to target data shape.
3. Update internal helper calls.
4. Add a temporary delegator in the source module if needed.
5. Migrate call sites to the new function.
6. Remove the delegator after migration.
7. Run formatter and tests.

## Validation

- Behaviour is unchanged (tests pass).
- Call sites use the function in the target module.
- The source module has less knowledge of target internals.

## Eliminates Code Smell

- `Feature Envy`
- `Inappropriate Intimacy`

## Similar Refactoring Techniques

- `Move Struct Field`
- `Extract Module`

## Anti-Refactoring

- `Inline Function`
