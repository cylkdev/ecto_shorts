---
trigger: model_decision
description: Tuples/lists are passed as domain data and accessed by position (elem/2, hd/tl, pattern {a, b, c}) rather than by name. Call sites rely on ordering and comments to remember meaning. Introduce a struct with named fields and migrate pattern matches.
---

# Replace Positional Data with Struct

## When to use

Use when any of the following are true:

- Lists/tuples/maps with implicit positions are passed around.
- Callers rely on index positions instead of names.
- Meaning of each element is unclear without comments.

## Problem

Positional data hides intent and is easy to misuse.

```elixir
{"Ada", "Lovelace", "ada@example.com"}
```

## Solution

Introduce a named struct with explicit fields.

```elixir
defmodule Contact do
  defstruct [:first_name, :last_name, :email]
end
```

## Why Refactor

- Makes data self-describing.
- Reduces index/order mistakes.
- Enables pattern matching on named fields.

## How to Refactor

1. Define a struct for the positional payload.
2. Add conversion from old shape to new shape.
3. Migrate call sites to named fields.
4. Remove old positional representation.
5. Run formatter and tests.

## Validation

- Callers no longer depend on positional indexes.
- Pattern matches use named fields.
- Behaviour is unchanged (tests pass).

## Eliminates Code Smell

- `Primitive Obsession`
- `Data Clumps`
