# Refused Bequest

## Category

Abstraction Abusers

## Description

A module uses another module (via `use`, `import`, or behavior implementation) but only utilizes a small fraction of the inherited functionality, or actively works around parts of it. In Elixir, this often appears when a module implements a behavior but ignores or stubs out callbacks, or when `use` brings in functionality that isn't needed.

## Signs and Symptoms

- A module implements a behavior but has stub implementations that raise or return dummy values.
- A module uses `use SomeModule` but only needs one or two functions from it.
- Imported functions are immediately overridden or wrapped to change behavior.
- Documentation warns "don't call these functions directly" for inherited functionality.
- A behavior callback returns `:not_implemented` or similar sentinel values.

## Causes

- Using inheritance-like patterns (`use`) when composition would be better.
- Implementing a behavior that's too broad for the use case.
- Copy-pasting module structure without understanding what's needed.
- Behaviors designed with too many required callbacks.

## Example

```elixir
defmodule MyApp.SimpleCache do
  @behaviour Cachex.Policy

  # Only care about get/set, but behavior requires all these callbacks
  @impl true
  def get(key), do: Agent.get(__MODULE__, &Map.get(&1, key))

  @impl true
  def set(key, value), do: Agent.update(__MODULE__, &Map.put(&1, key, value))

  # Don't need these but must implement them
  @impl true
  def delete(_key), do: raise "Not implemented"

  @impl true
  def clear(), do: raise "Not implemented"

  @impl true
  def stats(), do: raise "Not implemented"

  @impl true
  def ttl(_key), do: raise "Not implemented"

  @impl true
  def touch(_key), do: raise "Not implemented"
end

# Or with `use`:
defmodule MyApp.MinimalController do
  use Phoenix.Controller

  # Only need render/2 but `use` brings in dozens of functions
  # Many are overridden or ignored

  def index(conn, _params) do
    render(conn, "index.html")
  end
end
```

## Refactored

```elixir
# Option 1: Define a smaller behavior
defmodule MyApp.SimpleCache.Behavior do
  @callback get(key :: term()) :: term() | nil
  @callback set(key :: term(), value :: term()) :: :ok
end

defmodule MyApp.SimpleCache do
  @behaviour MyApp.SimpleCache.Behavior

  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @impl true
  def get(key), do: Agent.get(__MODULE__, &Map.get(&1, key))

  @impl true
  def set(key, value) do
    Agent.update(__MODULE__, &Map.put(&1, key, value))
    :ok
  end
end

# Option 2: Use composition instead of `use`
defmodule MyApp.MinimalController do
  import Phoenix.Controller, only: [render: 2, put_flash: 3]
  import Plug.Conn

  def index(conn, _params) do
    render(conn, "index.html")
  end
end

# Option 3: Delegate only what's needed
defmodule MyApp.CacheWrapper do
  defdelegate get(key), to: Cachex, as: :get
  defdelegate set(key, value), to: Cachex, as: :put

  # No other Cachex functions exposed
end
```

## Treatment

- **Extract Behavior**: Create a smaller behavior with only the callbacks you need.
- **Replace Use with Import**: Import only the specific functions needed.
- **Replace Inheritance with Delegation**: Wrap the dependency and expose only required functions.
- **Push Down Function**: If a shared module has functions not all consumers need, push them to specific modules.

## Why Refactor

- Modules only contain functionality they actually use.
- Smaller behaviors are easier to implement correctly.
- Explicit imports document dependencies clearly.
- Reduces coupling to large frameworks or libraries.
- Eliminates dead code and stub implementations.

## When to Accept

- Framework conventions require `use` (e.g., Phoenix controllers, GenServer).
- The unused functionality has no runtime cost.
- The behavior is a well-known standard that consumers expect.

## Elixir-Specific Considerations

In Elixir, "inheritance" patterns are different from OOP:

| Pattern | When to Use | When to Avoid |
|---------|-------------|---------------|
| `use` | Framework integration, DSLs | When only a few functions needed |
| `import` | Frequently called functions | Large modules with many functions |
| `@behaviour` | Defining contracts | When contract is too broad |
| `defdelegate` | Exposing subset of API | When wrapping adds no value |

## Related Smells

- `Speculative Generality`
- `Dead Code`
- `Large Module`

## Related Refactoring Techniques

- `Extract Behavior`
- `Replace Delegation with Shared Module`
- `Push Down Function`
- `Collapse Module Hierarchy`
