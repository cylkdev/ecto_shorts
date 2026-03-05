# Introduce External-Type Function

## When to use

Use when any of the following are true:

- You need behavior for a type from a library you do not control.
- The behavior is needed in one module or a small number of places.
- Repeating inline transformations is starting to duplicate logic.
- Creating a full wrapper module would be premature.

## Problem

You need app-specific behavior around an external type, but cannot modify the dependency.

```elixir
defmodule BillingReport do
  def line_total_string(%{amount: decimal}) do
    "$" <> Decimal.to_string(decimal, :normal)
  end
end
```

## Solution

Extract a local function that accepts the external value explicitly.

```elixir
defmodule BillingReport do
  def line_total_string(%{amount: decimal}), do: to_currency_string(decimal)

  defp to_currency_string(decimal) do
    "$" <> Decimal.to_string(decimal, :normal)
  end
end
```

## Why Refactor

This improves code in the following ways:

- Centralizes local behavior for an external type.
- Removes inline duplication.
- Keeps third-party boundaries explicit.

## Benefits

- Fast, low-risk extraction.
- Clearer function names at call sites.
- No changes to dependency source code.

## Drawbacks

- If the helper spreads across many modules, it should become a local wrapper module.

## How to Refactor

1. Identify repeated or likely-to-repeat logic for an external type.
2. Extract that logic into a local function.
3. Pass the external value as an explicit argument.
4. Replace inline expressions with the new function.
5. If usage grows broadly, move to `Introduce Local Wrapper`.
6. Run formatter and tests.

## Validation

- Behavior is unchanged (tests pass).
- External-type logic is centralized for the module.
- No attempt is made to patch dependency code.

## Eliminates Code Smell

- `Duplicate Code`
- `Incomplete External API`

## Similar Refactoring Techniques

- `Introduce Local Wrapper`
