---
trigger: model_decision
description: Multiple modules contain nearly identical pipeline flow (|> step1 |> step2) with only one step differing (parse_csv vs parse_json). Function bodies duplicate the same sequencing and shared steps. A template function can own the flow and accept the variant step as a callback/module.
---

# Form Template Function

## When to use

Use when modules follow the same algorithm steps but differ in one or two step implementations.

## Problem

Algorithm flow is duplicated across modules with small step differences.

```elixir
defmodule CsvImport do
  def run(file), do: file |> parse_csv() |> validate() |> persist()
end

defmodule JsonImport do
  def run(file), do: file |> parse_json() |> validate() |> persist()
end
```

## Solution

Define a shared template flow and delegate variable steps to callback functions.

```elixir
defmodule ImportTemplate do
  def run(file, parser_fun) do
    file
    |> parser_fun.()
    |> validate()
    |> persist()
  end

  defp validate(data), do: data
  defp persist(data), do: {:ok, data}
end
```

## Why Refactor

- Shares common algorithm structure.
- Isolates variant steps cleanly.

## How to Refactor

1. Identify stable algorithm steps.
2. Extract common flow into template function.
3. Pass variant steps as callbacks/modules.
4. Run formatter and tests.

## Validation

- Shared flow exists once.
- Variant step behaviour remains correct.
