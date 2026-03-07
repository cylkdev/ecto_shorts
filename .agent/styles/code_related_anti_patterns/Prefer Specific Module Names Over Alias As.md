# Prefer Specific Module Names Over alias as

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

**Problem**

Using `alias ... as: ...` for modules you control in the codebase is a sign of a naming problem in the codebase.

This usually happens when a module has a generic name like Supervisor, Manager, Helper, or Utils. The generic name forces each caller to create a local rename just to make the code readable.

The result is a readability tax. Each file must re-explain the module name, and different files may choose different local names for the same module. That makes the codebase harder to scan and harder to maintain.

**Rule**

Prefer domain-specific module names so callers can use normal `alias` without `as:`.

Use `alias ... as: ...` only when it clearly improves readability in a local file, such as resolving a real name collision. Do not rely on `as:` as a workaround for overly generic module names in the design.

**Bad Example**

In this example, the module name is too generic for the domain. Callers must rename it to make the code readable.

```elixir
defmodule Bigtable.Supervisor do
  use Supervisor

  # ...
end
```

```elixir
defmodule Bigtable.Tablet do
  alias Bigtable.Supervisor, as: PartitionSupervisor
  alias Bigtable.Tablet.Manager, as: TabletManager

  def start_tablet(tablet_id) do
    PartitionSupervisor.start_tablet(tablet_id)
  end
end
```

Bigtable.Supervisor does not communicate its domain role clearly. The caller has to rename it to PartitionSupervisor to express the intent that should have been in the module name already.

If many files do this, the project ends up with repeated aliases like PartitionSupervisor, BigtableSupervisor, or MainSupervisor for the same module.

**Good Example**

In this example, the module name is specific enough that callers do not need a local rename.

```elixir
defmodule Bigtable.Cluster.PartitionSupervisor do
  use Supervisor

  # ...
end
```

```elixir
defmodule Bigtable.Tablet do
  alias Bigtable.Cluster.PartitionSupervisor
  alias Bigtable.Tablet.Manager

  def start_tablet(tablet_id) do
    PartitionSupervisor.start_tablet(tablet_id)
  end
end
```

The domain meaning is encoded in the module name itself. Every caller sees the same name, so the codebase stays consistent and easier to read.
