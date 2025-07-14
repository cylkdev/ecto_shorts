---
trigger: always_on
description: Apply whenever you consider adding a comment: omit it unless it explains a non-obvious reason or deviation; rely on clear names and structure instead of stating the obvious.
---

Rule: Avoid Unnecessary Comments

---

Do not write comments unless they are absolutely necessary for explaining why the code exists or why it deviates from standard expectations. Favor clean, expressive code over explanatory comments. When a comment is truly needed, make it concise and clear.

Preferred

Write self-documenting code with meaningful function and variable names.

Discouraged

```elixir

# Increment the counter by one
counter = counter + 1

```