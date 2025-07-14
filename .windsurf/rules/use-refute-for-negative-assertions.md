---
trigger: always_on
---

Rule: Use refute for Negative Assertions

In ExUnit tests, prefer refute expr over assert expr == false or similar, for clarity and idiomatic style.

Valid

```elixir

refute user.active?

```

Invalid

```elixir

assert user.active? == false

```