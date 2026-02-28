# Testing Code Style

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Write assertions that reflect what the caller observes.
* Assert on the full returned value, or on a small explicit structure you build from it, instead of asserting on many individual fields one by one.
* Use per-field assertions only when one field is the only important behavior you need to prove.

## How to write Doctests

* Doctests should start with `iex>` and each subsequent line should be prefixed with `...>`.
* Write the expected output and the end of the full expression.
* Prefer explicit assertions that shows what a user would actually see. This means don't to "policy.partitions" but instead show the full struct.

Do not do this:

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
