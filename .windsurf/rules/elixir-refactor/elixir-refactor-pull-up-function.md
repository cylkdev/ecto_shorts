---
trigger: model_decision
description: Sibling/variant modules contain identical function implementations (same body, same name/arity). Copy/paste changes require editing multiple files. Move the shared function to a common module and reuse it.
---

# Pull Up Function

## When to use

Use when the same function logic is duplicated across related modules.

## Problem

Sibling modules repeat identical function behaviour.

```elixir
defmodule CsvExporter do
  def normalize_name(name), do: String.trim(name) |> String.downcase()
end

defmodule JsonExporter do
  def normalize_name(name), do: String.trim(name) |> String.downcase()
end
```

## Solution

Move shared logic to a common module and call it from variants.

```elixir
defmodule Exporter.Shared do
  def normalize_name(name), do: String.trim(name) |> String.downcase()
end
```

## Why Refactor

- Removes duplicated behaviour.
- Centralizes future changes.

## How to Refactor

1. Extract identical logic.
2. Create shared module function.
3. Replace duplicate implementations with calls.
4. Run formatter and tests.

## Validation

- One canonical implementation remains.
- Callers preserve behaviour.
