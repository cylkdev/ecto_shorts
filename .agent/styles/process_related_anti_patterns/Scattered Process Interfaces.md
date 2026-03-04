# Scattered Process Interfaces

**Problem**

When direct interactions with a process are spread across many modules (multiple call sites doing `Agent.get/update`, `GenServer.call/cast`, raw `send/2`, etc.), the system becomes harder to maintain and more bug-prone:

- duplicated "how do I talk to this process?" logic appears everywhere
- the process state shape can drift because anybody can write "whatever" into it
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
