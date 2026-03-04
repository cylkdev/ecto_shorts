# Accidental Compile-Time Dependencies

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
