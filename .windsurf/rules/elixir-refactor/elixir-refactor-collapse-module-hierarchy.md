---
trigger: model_decision
description: Module layers mostly just defdelegate/forward calls (do: Base.foo(...)) with no specialization. Submodules exist only to route to a base module or macro and add little domain meaning. Finding real logic requires hopping through multiple modules.
---

# Collapse Module Hierarchy

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
