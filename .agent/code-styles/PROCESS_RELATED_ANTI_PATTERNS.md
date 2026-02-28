# Process-related anti-patterns

This document outlines potential anti-patterns related to processes and process-based abstractions (Agent, GenServer, Task, raw message passing). Processes are a great tool for runtime concerns (concurrency, isolation, shared resource access, and lifecycle management), but they are easy to overuse in ways that create bottlenecks, make systems harder to change, or waste CPU/memory.

---

## Code organization by process

**Problem**

Do not introduce a process just to “organize code”. A process should exist because you need runtime properties (serialization, isolation, backpressure, ordering, fault containment, etc.). When you wrap pure computation in a single process (for example, a GenServer), you create an unnecessary single-file bottleneck: every caller must funnel through one mailbox.

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

---

## Scattered process interfaces

**Problem**

When direct interactions with a process are spread across many modules (multiple call sites doing `Agent.get/update`, `GenServer.call/cast`, raw `send/2`, etc.), the system becomes harder to maintain and more bug-prone:

- duplicated “how do I talk to this process?” logic appears everywhere
- the process state shape can drift because anybody can write “whatever” into it
- it becomes difficult to change the process implementation because the interface is implicit and scattered

**Example**

Multiple modules reaching directly into a shared Agent:

```elixir
defmodule A do
  def update(agent), do: Agent.update(agent, fn _ -> 123 end)
end

defmodule B do
  def update(agent), do: Agent.update(agent, fn value -> %{a: value} end)
end
```

**Refactoring**

Centralize process interaction behind a single module that defines a narrow, explicit API, and constrain the process state to a single shape:

```elixir
defmodule Foo.Bucket do
  use Agent

  def start_link(_opts), do: Agent.start_link(fn -> %{} end)

  def get(bucket, key), do: Agent.get(bucket, &Map.get(&1, key))
  def put(bucket, key, value), do: Agent.update(bucket, &Map.put(&1, key, value))
end
```

All other modules call `Foo.Bucket.get/2` and `Foo.Bucket.put/3` instead of calling `Agent.*` directly.

---

## Sending unnecessary data

**Problem**

Message passing copies data between processes. Large messages can be CPU- and memory-expensive. This includes:

- explicit messages (`send/2`, `GenServer.call/3`, `GenServer.cast/2`, `GenServer.start_link/3` init args)
- implicit messages via closures (`spawn/1`, `Task.async/1`, `Task.async_stream/3`) where captured variables are copied into the new process

This commonly bites when you “just pass the whole struct” (for example, a `%Plug.Conn{}`) when you only need one or two fields.

**Example**

Capturing a large variable inside a spawned function:

```elixir
spawn(fn -> report_ip(conn.remote_ip) end)
```

Even though you only use `conn.remote_ip`, the closure still captures `conn` and the whole term gets copied.

**Refactoring**

Extract the minimal data first, then spawn with only that value:

```elixir
ip_address = conn.remote_ip
spawn(fn -> report_ip(ip_address) end)
```

Other options when appropriate:

- make the receiving process fetch the data it needs (instead of being sent a huge payload)
- for infrequently changing shared data, consider mechanisms designed for sharing (for example, `:persistent_term`) instead of repeatedly copying large terms

---

## Unsupervised processes

**Problem**

Starting long-running processes outside a supervision tree makes their lifecycle harder to observe and control:

- startup ordering becomes ad-hoc (“hope it starts before users call it”)
- shutdown ordering is unpredictable
- crash/restart behavior is undefined (no configured restart strategy)
- runtime visibility is worse (harder to introspect as part of the application)

This is especially problematic in libraries that hide background processes from their callers.

**Example**

A library starts a named Agent directly and expects callers to “just run it”:

```elixir
defmodule Counter do
  use Agent

  def start_link(opts \\ []) do
    initial = Keyword.get(opts, :initial_value, 0)
    name = Keyword.get(opts, :name, __MODULE__)
    Agent.start_link(fn -> initial end, name: name)
  end
end
```

**Refactoring**

Ensure long-running processes are started under supervision. Provide a `child_spec/1` (Elixir will derive one for `use GenServer/Agent`, but you still need to document how it should be supervised), and have the application put the process into its supervision tree:

```elixir
children = [
  Counter,
  Supervisor.child_spec({Counter, name: :other_counter, initial_value: 15}, id: :other_counter)
]

Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
```

Supervision trees give deterministic startup and reverse-order shutdown, plus explicit restart strategies, and integrate with runtime introspection tools.
