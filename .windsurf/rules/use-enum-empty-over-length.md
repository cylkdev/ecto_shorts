---
trigger: always_on
description: Use this rule any time you check if a list is empty: call Enum.empty?(list) instead of length(list) == 0 so intent is clear and the list isn’t fully traversed.
---

Rule: Use `Enum.empty?/1` Over `length(list) === 0`

---

Use `Enum.empty?(list)` instead of comparing the length of a list to zero. This is both more expressive and more efficient, as it avoids traversing the entire list just to check if it's empty.
  

Valid

```elixir

Enum.empty?(list)

```

Invalid

```elixir

length(list) === 0

```