---
trigger: always_on
---

Rule: Name Predicates with “?” Suffix

Public functions that return a Boolean should end with “?” (e.g., empty?). Reserve the is_ prefix for guard helpers only.

Valid

```elixir

def admin?(user), do: user.role == :admin

```

Invalid

```elixir

def is_admin(user), do: user.role == :admin

```