# Special Names

Some names have specific meaning in Elixir. We detail those cases below.

### length and size

When you see size in a function name, it means the operation runs in constant time (also written as "O(1) time") because the size is stored alongside the data structure.

Examples: `map_size/1`, `tuple_size/1`

When you see length, the operation runs in linear time ("O(n) time") because the entire data structure has to be traversed.

Examples: `length/1`, `String.length/1`

In other words, functions using the word "size" in its name will take the same amount of time whether the data structure is tiny or huge. Conversely, functions having "length" in its name will take more time as the data structure grows in size.

### get, fetch, fetch!

When you see the functions `get`, `fetch`, and `fetch!` for key-value data structures, you can expect the following behaviors:

- `get` returns a default value (which itself defaults to nil) if the key is not present, or returns the requested value.
- `fetch` returns :error if the key is not present, or returns {:ok, value} if it is.
- `fetch!` raises if the key is not present, or returns the requested value.

Examples: `Map.get/2`, `Map.fetch/2`, `Map.fetch!/2`, `Keyword.get/2`, `Keyword.fetch/2`, `Keyword.fetch!/2`

### compare

The function `compare/2` should return :lt if the first term is less than the second, :eq if the two terms compare as equivalent, or :gt if the first term is greater than the second.

Examples: `DateTime.compare/2`

Note that this specific convention is important due to the expectations of `Enum.sort/2`
