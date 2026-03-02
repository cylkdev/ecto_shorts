# Code-related anti-patterns

This document outlines potential anti-patterns related to your code and specific Elixir idioms and features. These anti-patterns should be avoided.

---

## Namespace trespassing

**Problem**

This anti-pattern manifests when a package author or a library defines modules outside of its "namespace". A library should use its name as a "prefix" for all of its modules. For example, a package named `:my_lib` should define all of its modules within the `MyLib` namespace, such as `MyLib.User`, `MyLib.SubModule`, `MyLib.Application`, and `MyLib` itself.

This is important because the Erlang VM can only load one instance of a module at a time. So if there are multiple libraries that define the same module, then they are incompatible with each other due to this limitation. By always using the library name as a prefix, it avoids module name clashes due to the unique prefix.

**Example**

This problem commonly manifests when writing an extension of another library. For example, imagine you are writing a package that adds authentication to Plug called `:plug_auth`. You must avoid defining modules within the `Plug` namespace:

```elixir
defmodule Plug.Auth do
  # ...
end
```

Even if `Plug` does not currently define a `Plug.Auth` module, it may add such a module in the future, which would ultimately conflict with `plug_auth`'s definition.

**Refactoring**

Given the package is named `:plug_auth`, it must define modules inside the `PlugAuth` namespace:

```elixir
defmodule PlugAuth do
  # ...
end
```

There are few known exceptions to this anti-pattern:

Protocol implementations are, by design, defined under the `protocol` namespace

In some scenarios, the namespace owner may allow exceptions to this rule. For example, in Elixir itself, you defined custom Mix tasks by placing them under the Mix.Tasks namespace, such as Mix.Tasks.PlugAuth

If you are the maintainer for both `plug` and `plug_auth`, then you may allow `plug_auth` to define modules with the `Plug` namespace, such as `Plug.Auth`. However, you are responsible for avoiding or managing any conflicts that may arise in the future.

---

## Non-assertive map access

**Problem**

In Elixir, it is possible to access values from Maps, which are key-value data structures, either statically or dynamically.

When a key is expected to exist in a map, it must be accessed using the `map.key` notation, making it clear to developers (and the compiler) that the key must exist. If the key does not exist, an exception is raised (and in some cases also compiler warnings). This is also known as the static notation, as the key is known at the time of writing the code.

When a key is optional, the `map[:key]` notation must be used instead. This way, if the informed key does not exist, nil is returned. This is the dynamic notation, as it also supports dynamic key access, such as `map[some_var]`.

When you use `map[:key]` to access a key that always exists in the map, you are making the code less clear for developers and for the compiler, as they now need to work with the assumption the key may not be there. This mismatch may also make it harder to track certain bugs. If the key is unexpectedly missing, you will have a nil value propagate through the system, instead of raising on map access.

**Table: Comparison of map access notations**

| Access notation	| Key exists	    | Key doesn't exist	| Use case                                        |
|-------------------|-------------------|-------------------|-------------------------------------------------|
| map.key	        | Returns the value	| Raises KeyError	| Structs and maps with known atom keys           |
| map[:key]	        | Returns the value	| Returns nil	    | Any Access-based data structure, optional keys  |

**Example**

The function `plot/1` tries to draw a graphic to represent the position of a point in a Cartesian plane. This function receives a parameter of Map type with the point attributes, which can be a point of a 2D or 3D Cartesian coordinate system. This function uses dynamic access to retrieve values for the map keys:

```elixir
defmodule Graphics do
  def plot(point) do
    # Some other code...
    {point[:x], point[:y], point[:z]}
  end
end
```

```elixir
point_2d = %{x: 2, y: 3}
%{x: 2, y: 3}
point_3d = %{x: 5, y: 6, z: 7}
%{x: 5, y: 6, z: 7}
Graphics.plot(point_2d)
{2, 3, nil}
Graphics.plot(point_3d)
{5, 6, 7}
```

Given we want to plot both 2D and 3D points, the behaviour above is expected. But what happens if we forget to pass a point with either :x or :y?

```elixir
bad_point = %{y: 3, z: 4}
%{y: 3, z: 4}
Graphics.plot(bad_point)
{nil, 3, 4}
```

The behaviour above is unexpected because our function should not work with points without a :x key. This leads to subtle bugs, as we may now pass nil to another function, instead of raising early on, as shown next:

```elixir
point_without_x = %{y: 10}
%{y: 10}
{x, y, _} = Graphics.plot(point_without_x)
{nil, 10, nil}
distance_from_origin = :math.sqrt(x * x + y * y)
** (ArithmeticError) bad argument in arithmetic expression
    :erlang.*(nil, nil)
```

The error above occurs later in the code because nil (from missing :x) is invalid for arithmetic operations, making it harder to identify the original issue.

**Refactoring**

To remove this anti-pattern, we must use the dynamic map[:key] syntax and the static map.key notation according to our requirements. We expect :x and :y to always exist, but not :z. The next code illustrates the refactoring of `plot/1`, removing this anti-pattern:

```elixir
defmodule Graphics do
  def plot(point) do
    # Some other code...
    {point.x, point.y, point[:z]}
  end
end
```

```elixir
Graphics.plot(point_2d)
{2, 3, nil}
Graphics.plot(bad_point)
** (KeyError) key :x not found in: %{y: 3, z: 4}
  graphic.ex:4: Graphics.plot/1
```

This is beneficial because:

It makes your expectations clear to others reading the code
It fails fast when required data is missing
It allows the compiler to provide warnings when accessing non-existent fields, particularly in compile-time structures like structs
Overall, the usage of map.key and map[:key] encode important information about your data structure, allowing developers to be clear about their intent. The Access module documentation also provides useful reference on this topic. You can also consider the Map module when working with maps of any keys, which contains functions for fetching keys (with or without default values), updating and removing keys, traversals, and more.

An alternative to refactor this anti-pattern is to use pattern matching, defining explicit clauses for 2D vs 3D points:

```elixir
defmodule Graphics do
  # 3d
  def plot(%{x: x, y: y, z: z}) do
    # Some other code...
    {x, y, z}
  end

  # 2d
  def plot(%{x: x, y: y}) do
    # Some other code...
    {x, y}
  end
end
```

Pattern-matching is specially useful when matching over multiple keys as well as on the values themselves at once. In the example above, the code will not only extract the values but also verify that the required keys exist. If we try to call `plot/1` with a map that doesn't have the required keys, we'll get a `FunctionClauseError`:

```elixir
incomplete_point = %{x: 5}
%{x: 5}
Graphics.plot(incomplete_point)
** (FunctionClauseError) no function clause matching in Graphics.plot/1

    The following arguments were given to Graphics.plot/1:

        # 1
        %{x: 5}
```

Another option is to use structs. By default, structs only support static access to its fields. In such scenarios, you may consider defining structs for both 2D and 3D points:

```elixir
defmodule Point2D do
  @enforce_keys [:x, :y]
  defstruct [x: nil, y: nil]
end
```

Generally speaking, structs are useful when sharing data structures across modules, at the cost of adding a compile time dependency between these modules. If module A uses a struct defined in module B, A must be recompiled if the fields in the struct `B` change.

In summary, Elixir provides several ways to access map values, each with different behaviours:

    1. Static access (`map.key`): Fails fast when keys are missing, ideal for structs and maps with known atom keys

    2. Dynamic access (`map[:key]`): Works with any Access data structure, suitable for optional fields, returns nil for missing keys

    3. Pattern matching: Provides a powerful way to both extract values and ensure required map/struct keys exist in one operation

Choosing the right approach depends if the keys are known upfront or not. Static access and pattern matching are mostly equivalent (although pattern matching allows you to match on multiple keys at once, including matching on the struct name).

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