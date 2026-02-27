# Duplicate Observed Data

## When to use

Use when any of the following are true:

- UI state and domain state must both exist and stay in sync.
- One side is optimized for rendering, the other for business rules.
- Changes in one model should propagate to the other predictably.

## Problem

Two representations of the same data exist (for example LiveView assigns and domain structs), but synchronization is ad hoc.

## Solution

Keep a clear source of truth and define explicit synchronization points/events.

```elixir
def handle_event("email_changed", %{"email" => email}, socket) do
  changeset = Accounts.change_user_email(socket.assigns.user, email)
  {:noreply, assign(socket, :changeset, changeset)}
end
```

## Why Refactor

- Makes sync behaviour explicit.
- Prevents silent divergence between representations.
- Keeps UI-specific and domain-specific models separate but coordinated.

## How to Refactor

1. Identify the source of truth.
2. Define explicit update flow from source to observer.
3. Remove implicit/two-way hidden sync.
4. Add tests for synchronization scenarios.

## Validation

- Changes propagate correctly in defined directions.
- No stale observed data remains after updates.
- Tests cover source-to-observer sync.

## Eliminates Code Smell

- `Divergent Change`
- `Shotgun Surgery`
