# Non-assertive map access

## Problem

In Elixir, it is possible to access values from Maps, which are key-value data structures, either statically or dynamically.

When a key is expected to exist in a map, it must be accessed using the `map.key` notation, making it clear to developers (and the compiler) that the key must exist. If the key does not exist, an exception is raised (and in some cases also compiler warnings). This is also known as the static notation, as the key is known at the time of writing the code.

When a key is optional, the `map[:key]` notation must be used instead. This way, if the informed key does not exist, nil is returned. This is the dynamic notation, as it also supports dynamic key access, such as `map[some_var]`.

When you use `map[:key]` to access a key that always exists in the map, you are making the code less clear for developers and for the compiler, as they now need to work with the assumption the key may not be there. This mismatch may also make it harder to track certain bugs. If the key is unexpectedly missing, you will have a nil value propagate through the system, instead of raising on map access.

### Table: Comparison of map access notations

| Access notation | Key exists        | Key doesn't exist | Use case                                       |
|-----------------|-------------------|-------------------|------------------------------------------------|
| `map.key`       | Returns the value | Raises `KeyError` | Structs and maps with known atom keys          |
| `map[:key]`     | Returns the value | Returns `nil`     | Any Access-based data structure, optional keys |

### Example

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

### Refactoring

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

- It makes your expectations clear to others reading the code
- It fails fast when required data is missing
- It allows the compiler to provide warnings when accessing non-existent fields, particularly in compile-time structures like structs

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
