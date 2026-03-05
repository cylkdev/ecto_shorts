# Inline Temp

## When to use

Use when any of the following are true:

- The local binding is introduced once.

- The local binding is used exactly once after it is introduced.

- The assigned expression is side-effect free.

- The assigned expression is cheap to evaluate (or you have confirmed performance is not relevant here).

- The binding name does not represent a meaningful domain concept that improves the code.

## Problem

You have a local binding that stores a value used only as a direct substitution, but the binding does not reduce duplication, does not cache an expensive operation, and does not add domain meaning.

Example:

```elixir
def has_discount?(order) do
  base_price = order.quantity * order.item_price
  base_price > 1_000
end
```

## Solution

Replace the binding reference with the expression assigned to it, then delete the binding assignment. This removes a local alias that is not carrying useful domain meaning.

Example:

```elixir
def has_discount?(order) do
  order.quantity * order.item_price > 1_000
end
```

## Why Refactor

This improves code in the following ways:

- It removes a local binding that is used only once.

- It reduces unnecessary local names in the function clause.

- It can expose the real expression so other refactorings become possible (for example `Extract Variable`, `Extract Function`, or `Replace Temp with Query`).

- It prevents "fake steps" where a variable assignment exists but does not create reuse, caching, or meaning.

## Benefits

This refactoring technique offers almost no benefit in and of itself. However, if a binding is assigned the result of a function call and used once, you can marginally improve readability by removing that unnecessary binding.

## Drawbacks

Sometimes seemingly useless temporary bindings are used to cache the result of an expensive operation that is reused several times. Before applying this refactoring, make sure simplicity will not reduce performance.

## How to Refactor

1. Identify a local binding assignment.

2. Confirm the binding is assigned exactly once and used exactly once.

3. Confirm the assigned expression has no side effects.

4. Confirm the expression is safe to evaluate at the use site (same order, same conditions, same available values).

5. Replace the binding reference with the assigned expression.

6. Delete the binding assignment line.

7. Run tests.

8. If the resulting line becomes multi-step or too dense, apply `Extract Variable` (as described in `./EXTRACT_VARIABLE.md`) or `Extract Function` (as described in `./EXTRACT_FUNCTION.md`) instead.

## Validation

The refactoring is successful if all of the following are true:

- Behavior is unchanged (tests pass).

- The removed binding had exactly one use.

- No duplicate evaluation was introduced (the expression still executes the same number of times in practice for that path).

- The resulting expression remains side-effect safe.

- The function now has one fewer local binding without losing domain meaning.

## Helps Other Refactorings

- [Replace Temp with Query](./REPLACE_TEMP_WITH_QUERY.md)
- [Extract Function](./EXTRACT_FUNCTION.md)
