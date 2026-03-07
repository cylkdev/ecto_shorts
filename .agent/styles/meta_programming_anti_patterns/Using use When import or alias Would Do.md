# Using use When import or alias Would Do

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

**Problem**

`use` is much broader than `import` and `alias`. `use` runs `__using__/1`, which can inject arbitrary code into the caller and also propagate dependencies (for example, importing additional modules). That makes code harder to read, and can cause unexpected conflicts between imported functions and local functions.

**Example**

A library's `__using__/1` injects imports from other modules, surprising the caller and risking name conflicts:

```elixir
defmodule Library do
  defmacro __using__(_opts) do
    quote do
      import Library
      import ModuleA
    end
  end
end
```

**Refactoring**

Prefer `import`/`alias` directly when the goal is simply "make these functions available":

```elixir
defmodule ClientApp do
  import Library
end
```

When `use` is genuinely necessary (for example, extension points in OTP behaviours), document its public effects clearly, like a "nutrition facts label": what behaviours are set, which public functions are defined, and what public attributes/macros are introduced.
