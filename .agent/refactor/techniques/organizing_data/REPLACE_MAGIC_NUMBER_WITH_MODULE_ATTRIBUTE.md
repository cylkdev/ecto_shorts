# Replace Magic Number with Module Attribute

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- Numeric/string literals encode business meaning.
- The same literal appears in multiple clauses.
- Readers cannot infer intent from raw values.

## Problem

Unlabeled literals hide domain intent.

```elixir
if retries > 3, do: :fail, else: :retry
```

## Solution

Replace magic literals with named module attributes (or named zero-arity functions when runtime config is needed).

```elixir
@max_retries 3

if retries > @max_retries, do: :fail, else: :retry
```

## Why Refactor

- Makes intent explicit.
- Centralizes important constants.
- Simplifies safe updates.

## How to Refactor

1. Identify repeated or unclear literals.
2. Introduce named module attributes.
3. Replace inline literals.
4. Run formatter and tests.

## Validation

- No business-critical literals remain unnamed.
- Constant names describe domain intent.
- Behaviour is unchanged (tests pass).

## Eliminates Code Smell

- `Primitive Obsession`
- `Duplicate Code`
