# Change Bidirectional Association to Unidirectional

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- One direction is rarely used.
- Maintaining both sides causes consistency bugs.
- The reverse link increases coupling without clear value.

## Problem

Two-way association exists, but one side is unnecessary and expensive to keep consistent.

## Solution

Remove the weak/unused direction and keep one clear ownership path.

```elixir
# Keep only Member -> Team association if Team -> members is rarely required.
```

## Why Refactor

- Reduces coupling and maintenance burden.
- Simplifies update semantics.
- Lowers risk of stale reverse links.

## How to Refactor

1. Confirm which direction is truly needed.
2. Remove reverse association and update queries.
3. Simplify write/update logic.
4. Add tests for remaining traversal path.

## Validation

- Required navigation still works.
- Reverse-link maintenance code is gone.
- Tests pass with simpler association model.

## Eliminates Code Smell

- `Inappropriate Intimacy`
- `Middle Man`
