# Inline Function

## When to use

Use when any of the following are true:

- A function has one expression (or one simple conditional) and is called from only one call site.

- A function only delegates to another function without adding validation, transformation, branching, or policy.

- The function name does not add information beyond what the body already states.

- The function was previously useful, but after refactoring, its logic became simple or redundant.

- Inlining the function removes one call layer without increasing duplication.

A function is "simple" here if it only does one of the following:

- returns a map or struct field
- performs a single comparison
- wraps one direct call with no added behavior
- returns a literal / constant
- forwards arguments unchanged

## Problem

When a function body is clearer than the extra call boundary, inline it.

Example:

```elixir
defmodule Delivery do
  def rating(%{late_deliveries: late} = delivery) do
    if more_than_five_late_deliveries?(delivery), do: 2, else: 1
  end

  defp more_than_five_late_deliveries?(%{late_deliveries: late}) do
    late > 5
  end
end
```

## Solution

Replace calls to the function with the function body, then delete the function.
This removes unnecessary indirection and keeps the behavior at the call site where it is already clear.

Example:

```elixir
defmodule Delivery do
  def rating(%{late_deliveries: late}) do
    if late > 5, do: 2, else: 1
  end
end
```

## Why Refactor

This improves code in the following ways:

- It reduces the number of function jumps needed to follow one behavior path.

- It removes functions that add no transformation, branching, or domain meaning.

- It simplifies navigation by removing wrapper functions that no longer add intent.

- It makes later refactoring easier when the real logic is already visible in the caller.

- It can reduce "speculative" abstractions that were introduced before they were needed.

## Benefits

By reducing unnecessary function layers, the code is easier to read and follow.

## How to Refactor

1. Confirm the function is safe to inline:

  - It is not part of a public API that callers depend on.

  - It is not intentionally a dispatch boundary (for example behavior callbacks, protocol implementations, or clause-based routing you want to keep explicit).

  - It has no side effects that depend on call boundaries (for example tracing, instrumentation, or expected stack traces).

2. Find all call sites.

3. Replace each call with the function body, preserving argument values, evaluation order, and match context.

4. If the function body uses parameter names, substitute the actual call arguments correctly and preserve arity expectations.

5. Run formatter and tests after each replacement (or after a small batch of replacements).

6. Delete the original function once all calls are inlined.

7. Run tests again.

## Validation

The refactoring is successful if all of the following are true:

- Behavior is unchanged (tests pass).

- The deleted function had no remaining call sites.

- The call path now has one fewer function layer.

- No new duplication was introduced unintentionally.

- The resulting call site is still within the local complexity limit you use (for example, it did not turn a simple line into a multi-step expression that should instead use `Extract Variable` (as described in `./EXTRACT_VARIABLE.md`) or `Extract Function` (as described in `./EXTRACT_FUNCTION.md`)).

## Eliminates Code Smell

- `Speculative Generality`

## Anti-Refactoring

- `Extract Function`
