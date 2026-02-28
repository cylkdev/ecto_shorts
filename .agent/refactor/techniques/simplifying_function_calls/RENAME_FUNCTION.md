# Rename Function

## When to use

Use when any of the following are true:

- A function name does not reflect current behaviour.
- Callers need comments to understand what the function does.
- Domain language changed, but function names did not.

## Problem

A vague or misleading name obscures intent.

```elixir
def process(user), do: Accounts.deactivate(user)
```

## Solution

Rename the function to describe its real behaviour.

```elixir
def deactivate_user(user), do: Accounts.deactivate(user)
```

## Why Refactor

- Makes call sites self-explanatory.
- Reduces semantic drift.
- Lowers onboarding cost.

## How to Refactor

1. Choose a domain-accurate name.
2. Rename function and update all call sites.
3. Keep temporary compatibility wrapper only if needed.
4. Run formatter and tests.

## Validation

- Function name matches behaviour.
- No stale call sites remain.
- Tests pass.

## Eliminates Code Smell

- `Obscure Intent`
