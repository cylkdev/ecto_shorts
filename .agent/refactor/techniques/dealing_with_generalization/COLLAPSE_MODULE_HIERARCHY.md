# Collapse Module Hierarchy

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when separated modules no longer provide meaningful distinction.

## Problem

A split hierarchy adds indirection but little real variation.

```elixir
defmodule Report.Base do
  def title(report), do: report.title
end

defmodule Report.Standard do
  defdelegate title(report), to: Report.Base
end
```

## Solution

Merge thin layers into one module.

```elixir
defmodule Report do
  def title(report), do: report.title
end
```

## Why Refactor

- Removes unnecessary module boundaries.
- Simplifies navigation and maintenance.

## How to Refactor

1. Identify layers with no meaningful specialization.
2. Move behaviour into one target module.
3. Update call sites.
4. Remove obsolete modules.
5. Run formatter and tests.

## Validation

- Removed layers had no unique value.
- Behaviour is unchanged.
