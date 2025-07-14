---
trigger: always_on
---

Rule: Prefer is_nil/1 in Nil Checks

Check nil with is_nil(value) or not is_nil(value); avoid value == nil, value === nil, value != nil.

Valid

```elixir

is_nil(item)

```

Invalid

```elixir

item == nil

```