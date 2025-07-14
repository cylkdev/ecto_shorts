---
trigger: always_on
description: Apply whenever you write an equality check: replace == with === in if, case, guards, filters, etc., to enforce type-exact matches and prevent coercion. Not needed for pattern matching or non-equality operators.
---

Rule: Use `===` Over `==`

---

Use the strict equality operator `===` instead of the standard equality operator `==`. This avoids ambiguity and ensures that comparisons do not coerce values between types. It leads to more predictable and safer equality checks.

Valid

```elixir

a === b

```

Invalid

```elixir

a == b

```