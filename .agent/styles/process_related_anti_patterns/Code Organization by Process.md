# Code Organization by Process

**Problem**

Do not introduce a process just to "organize code". A process should exist because you need runtime properties (serialization, isolation, backpressure, ordering, fault containment, etc.). When you wrap pure computation in a single process (for example, a GenServer), you create an unnecessary single-file bottleneck: every caller must funnel through one mailbox.

**Example**

A calculator implemented as a GenServer is usually a mistake:

```elixir
defmodule Calculator do
  use GenServer

  def start_link(init_arg), do: GenServer.start_link(__MODULE__, init_arg)

  def add(pid, a, b), do: GenServer.call(pid, {:add, a, b})

  @impl true
  def init(init_arg), do: {:ok, init_arg}

  @impl true
  def handle_call({:add, a, b}, _from, state) do
    {:reply, a + b, state}
  end
end
```

**Refactoring**

Prefer modules and functions for code organization. Keep computation pure by default and let callers decide if/how they want concurrency:

```elixir
defmodule Calculator do
  def add(a, b), do: a + b
end
```

If you truly need concurrency or serialization, add a process at the boundary where the runtime need exists (for example, to protect a shared ETS table or rate-limit an external service), not as a default wrapper around business logic.
