# Replace Temp with Query

Use when any of the following are true:

- A local binding is assigned from an expression result.

- The expression is side-effect free (it only computes and returns a value).

- The binding represents a derived value (for example `base_price`, `subtotal`, `tax_amount`, `active_count`).

- The same derived value is needed in multiple places (within the same clause or across functions), or extracting the surrounding logic is blocked by the temp.

- Recomputing the value is acceptable, or you intentionally keep a local binding after extraction for caching.

## Problem 

A local binding stores the result of an expression, but the value is really a reusable derived value that should be computed by a dedicated function.

Example:

```elixir
defmodule PriceCalculator do
  def calculate_total(%{quantity: quantity, item_price: item_price}) do
    base_price = quantity * item_price

    if base_price > 1_000 do
      base_price * 0.95
    else
      base_price * 0.98
    end
  end
end
```

## Solution

Move the expression into a separate function (usually `defp`) that returns the derived value.

Replace uses of the local binding with calls to that function. If needed, keep one local binding at the call site only as an explicit cache for performance.

This turns an inline derived value into a reusable query function with a stable name.

Example:

```elixir
defmodule PriceCalculator do
  def calculate_total(%{quantity: quantity, item_price: item_price} = line_item) do
    if base_price(line_item) > 1_000 do
      base_price(line_item) * 0.95
    else
      base_price(line_item) * 0.98
    end
  end

  defp base_price(%{quantity: quantity, item_price: item_price}) do
    quantity * item_price
  end
end
```

## Why Refactor

- It turns an inline derived calculation into a named function.

- It removes duplicated expressions when the same calculation is needed in multiple places.

- It makes the derived value reusable across clauses and functions in the same module.

- It can make a larger function easier to decompose because one calculation step is now isolated behind a function boundary.

- It prepares the code for later refactorings such as `Extract Function`.

## Benefits

Readable code. It is easier to understand a named derived-value function than a repeated inline expression.

Slimmer code through deduplication, especially when the expression is used in multiple functions.

## Performance

This refactoring may add extra function calls; the cost is usually negligible compared to gains in readability and reuse. If the temporary binding is intentionally caching a very expensive calculation, you may stop after extracting the expression to a function and still keep a local binding at the call site.

## How to Refactor

1. Identify a local binding that stores the result of an expression.

2. Verify the binding is assigned exactly once.

3. Verify the expression is side-effect free (it computes a value and does not perform actions).

4. Extract the expression into a new function (usually `defp`) with explicit inputs.

5. Make the new function return only the computed value.

6. Replace the local binding usage with a call to the new function.

7. Remove the original local binding if it is no longer needed.

8. Run formatter and tests.

9. If the calculation is expensive and used multiple times in the same function, consider keeping a local binding after extraction (extract first, then decide whether to cache locally).

## Validation

The refactoring is successful if all of the following are true:

- Behaviour is unchanged (tests pass).

- The original expression now exists in a dedicated function that returns a value.

- The local binding used only for storing that derived value was removed (or intentionally kept for caching).

- The new function can be called from at least the original call site without changing behaviour.

- No side effects were moved into the query/helper function.


## Eliminates Code Smell

- `Long Function`
- `Duplicate Code`

## Similar Refactoring Techniques

- `Extract Function`
