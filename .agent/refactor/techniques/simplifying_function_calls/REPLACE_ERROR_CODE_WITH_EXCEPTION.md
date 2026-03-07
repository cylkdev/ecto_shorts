# Replace Error Code with Exception

## When to use

Use when any of the following are true:

- A failure is truly exceptional and should halt normal flow.
- Sentinel error codes obscure failure causes.
- Deep callers repeatedly branch on the same rare failure code.

## Problem

Rare exceptional failures are encoded as generic return codes.

```elixir
case parse_config(path) do
  {:ok, config} -> config
  {:error, :corrupt} -> :abort
end
```

## Solution

Raise a specific exception for truly exceptional paths, while keeping expected domain failures as `{:error, reason}` when appropriate.

```elixir
def load_config!(path) do
  case parse_config(path) do
    {:ok, config} -> config
    {:error, :corrupt} -> raise "corrupt config: #{path}"
  end
end
```

## Why Refactor

- Separates exceptional failure from expected domain errors.
- Simplifies normal-path code.
- Makes catastrophic failures explicit.

## How to Refactor

1. Identify truly exceptional error-code paths.
2. Replace sentinel code handling with explicit exceptions.
3. Keep expected errors as tagged tuples.
4. Update callers (`try/rescue` or bang/non-bang API pairs).
5. Run formatter and tests.

## Validation

- Exceptional failures raise clearly.
- Expected failures still use idiomatic tagged tuples where appropriate.
- Tests cover both paths.

## Eliminates Code Smell

- `Error Code Obscurity`
