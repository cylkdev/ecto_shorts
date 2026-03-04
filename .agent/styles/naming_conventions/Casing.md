# Casing

Use `snake_case` when defining variables, function names, module attributes, and the like
in elixir:

```elixir
some_map = %{this_is_a_key: "and a value"}
is_map(some_map)
```

Aliases, commonly used as module names, are an exception as they must be capitalized and written in `CamelCase`, like `OptionParser`. For aliases, capital letters are kept in acronyms, like `ExUnit.CaptureIO` or `Mix.SCM`.

Atoms can be written either in `:snake_case` or `:CamelCase`, although the convention is to use the snake case version throughout Elixir.

Generally speaking, filenames follow the `snake_case` convention of the module they define. For example, `MyApp` should be defined inside the `my_app.ex` file. However, this is only a convention. At the end of the day any filename can be used as they do not affect the compiled code in any way.
