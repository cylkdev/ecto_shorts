# Writing Doctests

* Doctests should start with `iex>` and each subsequent line should be prefixed with `...>`.
* Write the expected output and the end of the full expression.
* Prefer explicit assertions that shows what a user would actually see. This means don't to "policy.partitions" but instead show the full struct.

Do not do this:

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

```elixir
iex> policy = Bigtable.Tablet.Policy.new()
iex> policy.partitions
2
iex> policy.max_items
10_000
iex> policy.table_type
:ordered_set
```

Do this:

```elixir
iex> policy = Bigtable.Tablet.Policy.new()
...> %Bigtable.Tablet.Policy{
...>   partitions: 2,
...>   max_items: 10_000,
...>   table_type: :ordered_set
...> }
```
