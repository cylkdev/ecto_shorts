# Introduce Parameter Struct

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## When to use

Use when any of the following are true:

- Multiple parameters travel together frequently.
- Function signatures are long and error-prone.
- Parameter sets need validation or defaults.

## Problem

Large argument lists hide meaning and invite ordering bugs.

```elixir
def quote(subtotal_cents, tax_rate_bps, discount_bps, shipping_cents, handling_cents) do
  ...
end
```

## Solution

Group related parameters into a struct (or explicit map schema) and pass one argument.

```elixir
defmodule QuoteParams do
  defstruct [:subtotal_cents, :tax_rate_bps, :discount_bps]
end

def quote(%QuoteParams{} = params), do: ...
```

## Why Refactor

- Shortens signatures.
- Names the parameter concept.
- Centralizes defaults and validation.

## How to Refactor

1. Identify recurring parameter groups.
2. Create parameter struct/module.
3. Migrate function signatures to accept the struct.
4. Update callers and constructors.
5. Run formatter and tests.

## Validation

- Related parameters are grouped in one type.
- Signature complexity is reduced.
- Tests pass.

## Eliminates Code Smell

- `Long Parameter List`
- `Data Clumps`
