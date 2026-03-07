# Unsupervised Processes

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

**Problem**

Starting long-running processes outside a supervision tree makes their lifecycle harder to observe and control:

- startup ordering becomes ad-hoc ("hope it starts before users call it")
- shutdown ordering is unpredictable
- crash/restart behaviour is undefined (no configured restart strategy)
- runtime visibility is worse (harder to introspect as part of the application)

This is especially problematic in libraries that hide background processes from their callers.

**Example**

A library starts a named Agent directly and expects callers to "just run it":

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
