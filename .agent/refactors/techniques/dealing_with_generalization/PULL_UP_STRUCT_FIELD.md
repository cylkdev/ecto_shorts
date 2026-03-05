# Pull Up Struct Field

## When to use

Use when the same field exists in multiple related structs and has the same meaning.

## Problem

Sibling structs duplicate one shared field.

```elixir
defmodule Square do
  defstruct [:id, :color, :side]
end

defmodule Circle do
  defstruct [:id, :color, :radius]
end
```

## Solution

Move the shared field to a common struct or shared base data shape used by both variants.

```elixir
defmodule Shape do
  defstruct [:id, :color]
end
```

## Why Refactor

- Removes duplicated data definitions.
- Keeps shared meaning in one place.

## How to Refactor

1. Identify truly shared fields.
2. Introduce common data shape.
3. Migrate variant structs to embed/reference shared shape.
4. Run formatter and tests.

## Validation

- Shared field is defined once.
- Variant behavior is unchanged.
