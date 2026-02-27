---
trigger: model_decision
description: Code uses try/rescue around Map.fetch!/2, File.read!/1, Repo.get!/2, etc for expected absence/invalid input. Exceptions are used for normal branching. Replace with predicate checks (Map.has_key?, File.exists?) and explicit conditionals.
---

# Replace Exception with Test

## When to use

Use when any of the following are true:

- A condition is expected and can be checked cheaply.
- Exceptions are being used for normal control flow.
- Preventive checks improve readability and performance.

## Problem

Code raises/rescues for predictable situations.

```elixir
try do
  {:ok, Map.fetch!(params, "email")}
rescue
  KeyError -> {:error, :missing_email}
end
```

## Solution

Check condition first and use normal branching.

```elixir
if Map.has_key?(params, "email") do
  {:ok, params["email"]}
else
  {:error, :missing_email}
end
```

## Why Refactor

- Keeps exceptions for exceptional states.
- Improves flow clarity.
- Avoids try/rescue overhead for expected cases.

## How to Refactor

1. Identify exception-driven normal branches.
2. Introduce explicit predicate checks.
3. Replace `try/rescue` with normal control flow.
4. Run formatter and tests.

## Validation

- Expected conditions use explicit checks.
- Exception paths are reserved for unexpected failures.
- Tests pass.

## Eliminates Code Smell

- `Exception-Driven Flow`
