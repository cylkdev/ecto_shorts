---
trigger: model_decision
description: Inside one module, a field is read/written directly in many functions (user.first_name, %{user | first_name: ...}). You anticipate adding defaulting/derived access logic for that field. Add private getter/setter helpers and route internal access through them.
---

# Self Encapsulate Field

## When to use

Use when any of the following are true:

- A struct field is accessed directly in many functions inside the same module.
- You need one place to enforce derived/default access rules.
- Field access logic is likely to change soon.

## Problem

Direct field reads/writes are spread across a module, so changes to access rules require broad edits.

```elixir
def full_name(%User{first_name: first, last_name: last}), do: first <> " " <> last
```

## Solution

Introduce private getter/setter-style helpers inside the same module and route field access through them.

```elixir
def full_name(user), do: first_name(user) <> " " <> last_name(user)

defp first_name(%User{first_name: value}), do: value

defp last_name(%User{last_name: value}), do: value
```

## Why Refactor

- Centralizes field access policy.
- Makes later field representation changes cheaper.
- Reduces accidental coupling to raw struct layout.

## How to Refactor

1. Identify repeated direct access to one field.
2. Add internal helper functions for read/write behaviour.
3. Replace direct accesses incrementally.
4. Run formatter and tests.

## Validation

- Behaviour is unchanged (tests pass).
- Field access logic is centralized in helper functions.

## Eliminates Code Smell

- `Shotgun Surgery`
- `Divergent Change`
