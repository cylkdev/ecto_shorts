# Introduce Assertion

## When to use

Use when any of the following are true:

- A code path should be unreachable if invariants hold.
- Defensive conditionals hide programmer assumptions.
- Invalid state should fail fast instead of silently continuing.

## Problem

Code tolerates impossible states, making bugs harder to detect.

```elixir
case status do
  :pending -> ...
  :paid -> ...
  _ -> :ok
end
```

## Solution

Replace impossible fallbacks with explicit assertions or matches.

```elixir
def handle_status(status) when status in [:pending, :paid] do
  case status do
    :pending -> ...
    :paid -> ...
  end
end
```

Or use direct pattern matching / `raise` for invalid programmer-state inputs.

## Why Refactor

- Makes invariants explicit.
- Fails fast on invalid states.
- Prevents hidden bug propagation.

## How to Refactor

1. Identify branches that should never happen.
2. Replace with guards, pattern matches, or explicit raises.
3. Keep user-input validation separate from programmer assertions.
4. Run formatter and tests.

## Validation

- Impossible states fail loudly.
- Valid inputs still behave as before.
- Tests cover assertion behaviour.

## Eliminates Code Smell

- `Obscure Intent`
- `Complex Conditional`
