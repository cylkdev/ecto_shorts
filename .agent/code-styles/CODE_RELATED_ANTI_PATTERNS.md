# Code-related anti-patterns

This document outlines potential anti-patterns related to your code and specific Elixir idioms and features. These anti-patterns should be avoided.

---

### Non-assertive pattern matching

**Problem**

Overall, Elixir systems are composed of many supervised processes, so the effects of an error are localized to a single process, and don't propagate to the entire application. A supervisor detects the failing process, reports it, and possibly restarts it. This anti-pattern arises when developers write defensive or imprecise code, capable of returning incorrect values which were not planned for, instead of applicationming in an assertive style through pattern matching and guards.

**Example**

The function `get_value/2` tries to extract a value from a specific key of a URL query string. As it is not implemented using pattern matching, `get_value/2` always returns a value, regardless of the format of the URL query string passed as a parameter in the call. Sometimes the returned value will be valid. However, if a URL query string with an unexpected format is used in the call, `get_value/2` will extract incorrect values from it:

```elixir
defmodule Extract do
  def get_value(string, desired_key) do
    parts = String.split(string, "&")

    Enum.find_value(parts, fn pair ->
      key_value = String.split(pair, "=")
      Enum.at(key_value, 0) === desired_key && Enum.at(key_value, 1)
    end)
  end
end
```

```elixir
# URL query string with the planned format - OK!
Extract.get_value("name=Lucas&university=UFMG&lab=ASERG", "lab")
"ASERG"
Extract.get_value("name=Lucas&university=UFMG&lab=ASERG", "university")
"UFMG"
# Unplanned URL query string format - Unplanned value extraction!
Extract.get_value("name=Lucas&university=institution=UFMG&lab=ASERG", "university")
"institution"   # <= why not "institution=UFMG"? or only "UFMG"?
```

**Refactoring**

To remove this anti-pattern, `get_value/2` can be refactored through the use of pattern matching. So, if an unexpected URL query string format is used, the function will crash instead of returning an invalid value. This behaviour, shown below, allows clients to decide how to handle these errors and doesn't give a false impression that the code is working correctly when unexpected values are extracted:

```elixir
defmodule Extract do
  def get_value(string, desired_key) do
    parts = String.split(string, "&")

    Enum.find_value(parts, fn pair ->
      [key, value] = String.split(pair, "=") # <= pattern matching
      key === desired_key && value
    end)
  end
end
```

```elixir
# URL query string with the planned format - OK!
Extract.get_value("name=Lucas&university=UFMG&lab=ASERG", "name")
"Lucas"
# Unplanned URL query string format - Crash explaining the problem to the client!
Extract.get_value("name=Lucas&university=institution=UFMG&lab=ASERG", "university")
** (MatchError) no match of right hand side value: ["university", "institution", "UFMG"]
  extract.ex:7: anonymous fn/2 in Extract.get_value/2 # <= left hand: [key, value] pair
Extract.get_value("name=Lucas&university&lab=ASERG", "university")
** (MatchError) no match of right hand side value: ["university"]
  extract.ex:7: anonymous fn/2 in Extract.get_value/2 # <= left hand: [key, value] pair
```

Elixir and pattern matching promote an assertive style of applicationming where you handle the known cases. Once an unexpected scenario arises, you can decide to address it accordingly based on practical examples, or conclude the scenario is indeed invalid and the exception is the desired choice.

`case/2` is another important construct in Elixir that help us write assertive code, by matching on specific patterns. For example, if a function returns `{:ok, ...}` or `{:error, ...}`, prefer to explicitly match on both patterns:

```elixir
case some_function(arg) do
  {:ok, value} -> # ...
  {:error, _} -> # ...
end
```

In particular, avoid matching solely on `_`, as shown below:

```elixir
case some_function(arg) do
  {:ok, value} -> # ...
  _ -> # ...
end
```

Matching on `_` is less clear in intent and it may hide bugs if `some_function/1` adds new return values in the future.

---

## Non-assertive truthiness

**Problem**

Elixir provides the concept of truthiness: nil and false are considered "falsy" and all other values are "truthy". Many constructs in the language, such as `&&/2`, `||/2`, and `!/1` handle truthy and falsy values. Using those operators is not an anti-pattern. However, using those operators when all operands are expected to be booleans, may be an anti-pattern.

**Example**

The simplest scenario where this anti-pattern manifests is in conditionals, such as:

```elixir
if is_binary(name) && is_integer(age) do
  # ...
else
  # ...
end
```

Given both operands of `&&/2` are booleans, the code is more generic than necessary, and potentially unclear.

**Refactoring**

To remove this anti-pattern, we can replace `&&/2`, `||/2`, and `!/1` by `and/2`, `or/2`, and `not/1` respectively. These operators assert at least their first argument is a boolean:

```elixir
if is_binary(name) and is_integer(age) do
  # ...
else
  # ...
end
```

This technique may be particularly important when working with Erlang code. Erlang does not have the concept of truthiness. It never returns nil, instead its functions may return :error or :undefined in places an Elixir developer would return nil. Therefore, to avoid accidentally interpreting :undefined or :error as a truthy value, you may prefer to use `and/2`, `or/2`, and `not/1` exclusively when interfacing with Erlang APIs.

## Structs with 32 fields or more

**Problem**

Structs in Elixir are implemented as compile-time maps, which have a predefined amount of fields. When structs have 32 or more fields, their internal representation in the Erlang Virtual Machines changes, potentially leading to bloating and higher memory usage.

**Example**

Any struct with 32 or more fields will be problematic:

defmodule MyExample do
  defstruct [
    :field1,
    :field2,
    ...,
    :field35
  ]
end

The Erlang VM has two internal representations for maps: a flat map and a hash map. A flat map is represented internally as two tuples: one tuple containing the keys and another tuple holding the values. Whenever you update a flat map, the tuple keys are shared, reducing the amount of memory used by the update. A hash map has a more complex structure, which is efficient for a large amount of keys, but it does not share the key space.

Maps of up to 32 keys are represented as flat maps. All others are hash map. Structs are maps (with a metadata field called `__struct__`) and so any struct with fewer than 32 fields is represented as a flat map. This allows us to optimize several struct operations, as we never add or remove fields to structs, we simply update them.

Furthermore, structs of the same name "instantiated" in the same module will share the same "tuple keys" at compilation times, as long as they have fewer than 32 fields. For example, in the following code:

```elixir
defmodule Example do
  def users do
    [%User{name: "John"}, %User{name: "Meg"}, ...]
  end
end
```

All user structs will point to the same tuple keys at compile-time, also reducing the memory cost of instantiating structs with `%MyStruct{...}` notation. This optimization is also not available if the struct has 32 keys or more.

**Refactoring**

Removing this anti-pattern, in a nutshell, requires ensuring your struct has fewer than 32 fields. There are a few technique you could apply:

- If the struct has "optional" fields, for example, fields which are initialized with nil, you could nest all optional fields into other field, called `:metadata`, `:optionals`, or similar. This could lead to benefits such as being able to use pattern matching to check if a field exists or not, instead of relying on nil values

- You could nest structs, by storing structs within other fields. Fields that are rarely read or written to are good candidates to be moved to a nested struct

- You could nest fields as tuples. For example, if two fields are always read or updated together, they could be moved to a tuple (or another composite data structure)

The challenge is to balance the changes above with API ergonomics, in particular, when fields may be frequently read and written to.

---

## Prefer Specific Module Names Over alias ... as:

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