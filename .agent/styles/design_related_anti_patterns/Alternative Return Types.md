# Alternative Return Types

**Problem**

A single function should not return fundamentally different shapes based on options like `return: :raw` vs `return: :wrapped`. It forces callers to read implementation details, makes types unclear, and leads to "guess the shape" code at call sites.

If you need multiple return shapes, they should be different functions with different names.

**Example**

One function returns a string by default, but returns an `{:ok, value} | {:error, reason}` tuple when configured:

```elixir
defmodule MyApp.Token do
  def parse(token, opts \\ []) do
    return = Keyword.get(opts, :return, :value)

    case do_parse(token) do
      {:ok, value} when return === :tuple -> {:ok, value}
      {:ok, value} -> value
      {:error, reason} when return === :tuple -> {:error, reason}
      {:error, _reason} -> nil
    end
  end

  defp do_parse(token) do
    if is_binary(token) and byte_size(token) > 0 do
      {:ok, String.trim(token)}
    else
      {:error, :invalid}
    end
  end
end
```

**Refactoring**

Expose distinct functions whose names encode the contract:

```elixir
defmodule MyApp.Token do
  def parse(token) do
    case parse_tuple(token) do
      {:ok, value} -> value
      {:error, _} -> nil
    end
  end

  def parse_tuple(token) do
    do_parse(token)
  end

  defp do_parse(token) do
    if is_binary(token) and byte_size(token) > 0 do
      {:ok, String.trim(token)}
    else
      {:error, :invalid}
    end
  end
end
```

Each function now has a single, stable return shape.
