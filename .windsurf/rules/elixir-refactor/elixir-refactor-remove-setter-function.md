---
trigger: model_decision
description: Public APIs like set_status/2 or put_field/3 allow arbitrary field mutation and enable invalid intermediate states. Callers can bypass invariants and state-transition rules. Replace generic setters with intention-revealing domain operations.
---

# Remove Setter Function

## When to use

Use when any of the following are true:

- Public setter-style functions expose internal mutation policy.
- Arbitrary field updates bypass invariants.
- A value should be set only at creation time.

## Problem

Setter-like APIs allow invalid intermediate states.

```elixir
def set_status(order, status), do: %{order | status: status}

# Any caller can set impossible transitions:
# set_status(order, :refunded) even when order is still :pending
```

## Solution

Remove generic setter functions and expose intention-revealing domain operations.

```elixir
# instead of set_status(order, status)
def cancel_order(order), do: %{order | status: :cancelled}
```

## Why Refactor

- Preserves invariants.
- Prevents arbitrary state changes.
- Improves domain clarity.

## How to Refactor

1. Identify public setter-like functions.
2. Replace with explicit domain operations.
3. Restrict direct mutation entry points.
4. Update callers.
5. Run formatter and tests.

## Validation

- Generic setter API is removed.
- State changes happen through explicit domain functions.
- Tests pass.

## Eliminates Code Smell

- `Data Class`
- `Inappropriate Intimacy`
