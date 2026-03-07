# Replace Delegation with Shared Module

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when many modules delegate the same calls and now share stable behaviour.

## Problem

Repeated delegation wrappers add noise across modules.

```elixir
defmodule CsvQuote do
  def total(order), do: QuoteCore.total(order)
end

defmodule JsonQuote do
  def total(order), do: QuoteCore.total(order)
end
```

## Solution

Promote repeated delegated behaviour into one shared module API used directly.

```elixir
defmodule Quote do
  def total(order), do: QuoteCore.total(order)
end
```

## Why Refactor

- Removes repetitive wrapper modules.
- Clarifies single point of shared behaviour.

## How to Refactor

1. Find repeated delegation-only modules.
2. Create or promote one shared module boundary.
3. Migrate callers to the shared module.
4. Remove redundant wrappers.
5. Run formatter and tests.

## Validation

- Delegation wrappers are reduced.
- Shared entry point is explicit and stable.
