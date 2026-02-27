---
trigger: model_decision
description: Several modules implement new/build functions that set the same defaults/normalization (retries: 0, status: :pending). The initialization logic is copy/pasted and risks drifting. Extract a shared constructor helper used by all variants.
---

# Pull Up Constructor Logic

## When to use

Use when multiple `new/` functions across related modules apply the same initialization steps.

## Problem

Constructor logic is duplicated in several modules.

```elixir
defmodule CsvJob do
  def new(attrs), do: %{source: attrs.source, retries: 0, status: :pending}
end

defmodule JsonJob do
  def new(attrs), do: %{source: attrs.source, retries: 0, status: :pending}
end
```

## Solution

Move shared initialization to one helper and keep variant-specific parts local.

```elixir
defmodule JobInit do
  def base(attrs) do
    %{
      source: attrs.source,
      retries: 0,
      status: :pending
    }
  end
end
```

## Why Refactor

- Avoids drift in shared defaults.
- Makes constructors consistent.

## How to Refactor

1. Extract common initialization steps.
2. Reuse shared constructor helper.
3. Keep only variant differences in each module.
4. Run formatter and tests.

## Validation

- Defaults are defined once.
- Variant constructors still return expected shapes.
