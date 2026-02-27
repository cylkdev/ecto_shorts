---
trigger: model_decision
description: Multiple small modules only delegate the same functions to a shared core module (CsvQuote.total -> QuoteCore.total). Wrappers differ only by name and add no behaviour. Expose one shared module API and delete redundant delegators.
---

# Replace Delegation with Shared Module

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
