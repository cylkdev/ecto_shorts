# Consolidate Duplicate Conditional Fragments

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- The same expression appears in multiple branches of one conditional.
- Setup/cleanup logic is duplicated above or below branch-specific code.
- Branches differ only by a small segment.

## Problem

Duplicated code appears in all conditional paths.

```elixir
if vip?(user) do
  total = subtotal * 0.90
  log_checkout(user)
  total
else
  total = subtotal
  log_checkout(user)
  total
end
```

## Solution

Move shared fragments outside the conditional and keep only differing logic inside.

```elixir
total = if vip?(user), do: subtotal * 0.90, else: subtotal
log_checkout(user)
total
```

## Why Refactor

- Removes branch duplication.
- Clarifies what truly differs.
- Lowers risk of inconsistent edits.

## How to Refactor

1. Identify code duplicated in all branches.
2. Move duplicated fragment before/after the conditional where safe.
3. Keep only branch-specific logic in conditional blocks.
4. Run formatter and tests.

## Validation

- Shared fragment exists once.
- Branches contain only unique behaviour.
- Tests pass.

## Eliminates Code Smell

- `Duplicate Code`
- `Long Function`
