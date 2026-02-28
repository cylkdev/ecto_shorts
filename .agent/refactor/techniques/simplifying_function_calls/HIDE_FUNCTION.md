# Hide Function

## When to use

Use when any of the following are true:

- A helper function should not be called from outside the module.
- Public API surface is larger than necessary.
- External callers depend on implementation details.

## Problem

Internal helper functions are public (`def`) without need.

```elixir
defmodule Checkout do
  def run(order), do: validate(order) |> persist()

  # `validate/1` is not part of the public API, but it is public here.
  def validate(order), do: order

  defp persist(order), do: {:ok, order}
end
```

## Solution

Change public helper functions to private (`defp`) and expose only stable entry points.

```elixir
def run(order), do: validate(order) |> persist()

defp validate(order), do: order

defp persist(order), do: {:ok, order}
```

## Why Refactor

- Shrinks API surface.
- Protects implementation details.
- Reduces accidental external coupling.

## How to Refactor

1. Find public functions not intended for external use.
2. Change to `defp`.
3. Ensure all external callers use public API functions.
4. Run formatter and tests.

## Validation

- Only intentional API functions remain public.
- Helpers are private.
- Tests pass.

## Eliminates Code Smell

- `Inappropriate Intimacy`
