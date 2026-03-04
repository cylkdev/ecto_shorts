# Using Application Configuration for Libraries

**Problem**

Libraries should not fetch application configuration to decide how they behave (for example, `Application.fetch_env!/2` inside library functions). It hides dependencies, makes code harder to test, and creates "action at a distance" where behaviour changes based on runtime config.

This is especially problematic when multiple consumers want different behaviour at the same time.

**Example**

```elixir
defmodule SomeLib.Client do
  def base_url do
    Application.fetch_env!(:some_lib, :base_url)
  end
end
```

Every caller is now coupled to global app config to use the library.

**Refactoring**

Prefer explicit configuration passed as arguments or stored in a struct:

```elixir
defmodule SomeLib.Client do
  defstruct [:base_url]

  def new(opts) do
    %__MODULE__{base_url: Keyword.fetch!(opts, :base_url)}
  end

  def base_url(%__MODULE__{base_url: base_url}), do: base_url
end
```

Application configuration still has a place, but it should be used at the application boundary to build the client (for example, in your app's supervision tree), not inside the library's core logic.
