---
trigger: model_decision
description: Code defines macros (defmacro, __using__, quote/unquote) and risks compile-time coupling. Macro expansion references modules/attributes at compile time (Module.put_attribute, Macro.expand_literals) or generates lots of code inside quote. Prefer runtime functions when possible.
---

## Accidental compile-time dependencies

**Problem**

Code outside of functions runs at compile time. If your macro expands into something that references another module outside of a function, you can accidentally make that other module a *compile-time dependency*, even if you only meant it to be a runtime dependency. This can balloon the recompilation graph: changing one module causes many others to recompile.

**Example**

This pattern makes `MyApp.Authentication` a compile-time dependency of the module that calls `plug/1`, even though the module is only used when requests are handled:

```elixir
defmodule MyApp.RouterMacros do
  defmacro plug(mod) do
    quote do
      @plugs unquote(mod)
    end
  end
end
```

**Refactoring**

If the macro only needs to store the module reference for later runtime use (and does not inspect the module at compile time), expand literals in the *runtime* context where they will be used. This turns a compile-time dependency into a runtime dependency:

```elixir
defmodule MyApp.RouterMacros do
  defmacro plug(mod) do
    mod = Macro.expand_literals(mod, %{__CALLER__ | function: {:call, 2}})

    quote do
      @plugs unquote(mod)
    end
  end
end
```

Rule of thumb: if you call functions on the module, read its structs, or access its metadata at compile time, you have a compile-time dependency. Sometimes that is necessary, but you should treat it as a cost and keep it intentional.

---

## Large code generation

**Problem**

Macros that generate a lot of code per invocation can make compilation slower and compiled artifacts larger. This frequently shows up in DSL-style macros that are called hundreds of times (for example, router declarations).

**Example**

If a macro includes heavy validation or logic inside `quote`, that logic is expanded and compiled at every call site:

```elixir
defmacro get(route, handler) do
  quote do
    route = unquote(route)
    handler = unquote(handler)

    if not is_binary(route), do: raise ArgumentError, "route must be a binary"
    if not is_atom(handler), do: raise ArgumentError, "handler must be a module"

    @routes {route, handler}
  end
end
```

**Refactoring**

Keep the macro expansion small and delegate the work to a normal function:

```elixir
defmacro get(route, handler) do
  quote do
    Routes.__define__(__MODULE__, unquote(route), unquote(handler))
  end
end

def __define__(module, route, handler) do
  if not is_binary(route), do: raise ArgumentError, "route must be a binary"
  if not is_atom(handler), do: raise ArgumentError, "handler must be a module"

  Module.put_attribute(module, :routes, {route, handler})
end
```

This reduces how much code is expanded/compiled repeatedly and makes the logic testable as a normal function.

---

## Unnecessary macros

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

---

## Using `use` when `import` or `alias` would do

**Problem**

`use` is much broader than `import` and `alias`. `use` runs `__using__/1`, which can inject arbitrary code into the caller and also propagate dependencies (for example, importing additional modules). That makes code harder to read, and can cause unexpected conflicts between imported functions and local functions.

**Example**

A library’s `__using__/1` injects imports from other modules, surprising the caller and risking name conflicts:

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

Prefer `import`/`alias` directly when the goal is simply “make these functions available”:

```elixir
defmodule ClientApp do
  import Library
end
```

When `use` is genuinely necessary (for example, extension points in OTP behaviours), document its public effects clearly, like a “nutrition facts label”: what behaviours are set, which public functions are defined, and what public attributes/macros are introduced.

---

## Untracked compile-time dependencies

**Problem**

Elixir’s incremental compilation relies on the compiler being able to see module references. If you generate module names programmatically (for example, with `Module.concat/2` or raw alias-atoms), you can bypass dependency tracking. This can lead to inconsistent recompiles (a dependency changes, but the caller is not recompiled).

**Example**

Avoid generating the module name itself:

```elixir
for part <- [:Foo, :Bar] do
  Module.concat(OtherModule, part).example()
end
```

Or “cheating” with atoms:

```elixir
mods = [:"Elixir.OtherModule.Foo", :"Elixir.OtherModule.Bar"]
Enum.each(mods, & &1.example())
```

**Refactoring**

Prefer explicit module names so the compiler can track them:

```elixir
mods = [OtherModule.Foo, OtherModule.Bar]
Enum.each(mods, & &1.example())
```

If you truly need to build module names dynamically, do it via macro expansion so the full alias is present at compile time:

```elixir
defmodule MyMacro do
  defmacro call_examples(parts) do
    for part <- parts do
      quote do
        OtherModule.unquote(part).example()
      end
    end
  end
end
```

When debugging dependency issues in real projects, `mix xref trace path/to/file.ex` can help show whether dependencies are compile-time, runtime, or export-related.
