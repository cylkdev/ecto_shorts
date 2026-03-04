# Structs with 32 Fields or More

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
