# Unnecessary Macros

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

**Problem**

Macros should be the last resort. If you can express the same behaviour with a function, do it with a function. Macros are harder to reason about, harder to debug, and often require `require/2` at call sites.

**Example**

This macro provides no benefit over a function:

```elixir
defmodule MyMath do
  defmacro sum(a, b) do
    quote do
      unquote(a) + unquote(b)
    end
  end
end
```

**Refactoring**

Replace it with a function:

```elixir
defmodule MyMath do
  def sum(a, b), do: a + b
end
```

The caller no longer needs `require MyMath`.
