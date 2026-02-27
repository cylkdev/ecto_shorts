---
trigger: always_on
description: Code is a doctest example.
---

## When to use

Use when writing or modifying doctest examples.

## How to write doctests

- Doctests should start with `iex>` and each subsequent line should be prefixed with `...>`.
- Write the expected output and the end of the full expression.
- Prefer explicit assertions that shows what a user would actually see. This means don't to "policy.partitions" but instead show the full struct.

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
