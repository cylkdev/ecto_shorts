# Untracked Compile-Time Dependencies

**Problem**

Elixir's incremental compilation relies on the compiler being able to see module references. If you generate module names programmatically (for example, with `Module.concat/2` or raw alias-atoms), you can bypass dependency tracking. This can lead to inconsistent recompiles (a dependency changes, but the caller is not recompiled).

**Example**

Avoid generating the module name itself:

```elixir
for part <- [:Foo, :Bar] do
  Module.concat(OtherModule, part).example()
end
```

Or "cheating" with atoms:

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
