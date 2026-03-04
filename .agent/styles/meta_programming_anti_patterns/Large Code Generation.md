# Large Code Generation

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
