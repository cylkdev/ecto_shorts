# Decompose Conditional

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- One `if`/`case` mixes condition checks, decision intent, and action logic.
- The conditional expression is hard to read without comments.
- The true/false branches each perform multiple steps.

## Problem

A single conditional hides intent because condition and branch logic are too dense.

```elixir
if Date.compare(today, plan.starts_on) !== :lt and Date.compare(today, plan.ends_on) !== :gt do
  amount - amount * plan.discount_rate
else
  amount
end
```

## Solution

Extract the condition and branch behaviours into named functions.

```elixir
def discounted_total(plan, amount, today) do
  if discount_period?(plan, today) do
    discounted_amount(plan, amount)
  else
    regular_amount(amount)
  end
end

defp discount_period?(plan, today) do
  Date.compare(today, plan.starts_on) !== :lt and Date.compare(today, plan.ends_on) !== :gt
end

defp discounted_amount(plan, amount) do
  amount - amount * plan.discount_rate
end

defp regular_amount(amount), do: amount
```

## Why Refactor

- Names explain intent better than raw boolean expressions.
- Branch behaviour becomes independently testable.
- Future rule changes are localized.

## How to Refactor

1. Extract the boolean condition into a named predicate function.
2. Extract each branch into a named function.
3. Replace inline logic with those calls.
4. Run formatter and tests.

## Validation

- Behaviour is unchanged (tests pass).
- Condition and branches are readable through function names.

## Eliminates Code Smell

- `Long Function`
- `Switch Statements`
