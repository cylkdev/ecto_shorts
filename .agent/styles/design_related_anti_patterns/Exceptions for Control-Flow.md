# Exceptions for Control-Flow

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

**Problem**

Using exceptions as normal control-flow (for example, calling `foo!/1` and rescuing) makes intent unclear and can obscure real failures. Exceptions are best reserved for truly exceptional situations or programmer errors.

Prefer explicit, typed returns for expected failure modes (for example, `{:ok, value} | {:error, reason}`), then handle them with pattern matching.

**Example**

```elixir
def get_required(map, key) do
  try do
    Map.fetch!(map, key)
  rescue
    KeyError -> :missing
  end
end
```

**Refactoring**

```elixir
def get_required(map, key) do
  case Map.fetch(map, key) do
    {:ok, value} -> {:ok, value}
    :error -> {:error, :missing}
  end
end
```

Now control-flow is obvious and exceptions remain for unexpected problems.
