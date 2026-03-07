# Form Template Function

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

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
