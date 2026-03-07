# Underscore (foo)

Elixir relies on underscores in different situations.

For example, a value that is not meant to be used must be assigned to `_` or to a variable starting with underscore:

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

```elixir
{:ok, _contents} = File.read("README.md")
```

Function names may also start with an underscore. Such functions are never imported by default:

```elixir
defmodule Example do
  def _wont_be_imported do
    :oops
  end
end

import Example
_wont_be_imported()
** (CompileError) iex:1: undefined function _wont_be_imported/0
```

Due to this property, Elixir relies on functions starting with underscore to attach compile-time metadata to modules. Such functions are most often in the `__foo__` format. For example, every module in Elixir has an `__info__/1` function:

```elixir
String.__info__(:functions)
[at: 2, capitalize: 1, chunk: 2, ...]
```

Elixir also includes five special forms that follow the double underscore format: `__CALLER__/0`, `__DIR__/0`, `__ENV__/0`and `__MODULE__/0` retrieve compile-time information about the current environment, while `__STACKTRACE__/0` retrieves the stacktrace for the current exception.
