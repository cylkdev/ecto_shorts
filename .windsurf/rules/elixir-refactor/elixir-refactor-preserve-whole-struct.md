---
trigger: model_decision
description: Call sites pass multiple fields from the same struct/map (address.city, address.postal_code, address.country_code) into a function. The callee treats them as one concept and may need more fields later. Accept the whole struct/map to reduce signature noise.
---

# Preserve Whole Struct

## When to use

Use when any of the following are true:

- Callers pass several fields from the same struct.
- Function parameters repeatedly unpack one data source.
- The called function may need additional fields later.

## Problem

Field-by-field parameter passing creates noisy signatures.

```elixir
def delivery_window(city, postal_code, country_code), do: ...
```

## Solution

Pass the whole struct/map and extract needed fields inside.

```elixir
def delivery_window(address), do: ...
```

## Why Refactor

- Shortens parameter lists.
- Reduces call-site field plumbing.
- Makes future field needs easier to support.

## How to Refactor

1. Identify parameters that come from one struct.
2. Change signature to receive the whole struct/map.
3. Update call sites.
4. Extract fields inside callee.
5. Run formatter and tests.

## Validation

- Signature is simpler.
- Callers stop passing many sibling fields.
- Tests pass.

## Eliminates Code Smell

- `Long Parameter List`
- `Data Clumps`
