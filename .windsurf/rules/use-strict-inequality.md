---
trigger: always_on
description: Use !== wherever you test inequality—in if, case, guards, filters, etc.—to guarantee type-exact comparisons and avoid coercion. Skip for pattern matching or other non-inequality operators.
---

## Rule: Use `!==` Over `!=`

---

Use the strict inequality operator `!==` instead of `!=` to ensure that type coercion is not involved in the comparison. It enforces clearer, more intentional inequality logic.

Valid

```elixir

a !== b

```

Invalid

```elixir

a != b

```