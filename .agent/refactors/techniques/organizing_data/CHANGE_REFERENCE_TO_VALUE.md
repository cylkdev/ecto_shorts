# Change Reference to Value

## When to use

Use when any of the following are true:

- A referenced entity has no independent lifecycle.
- Identity adds complexity but no domain benefit.
- Data should be immutable snapshot data at write time.

## Problem

A reference is maintained for data that should just be copied as a value.

```elixir
%Invoice{tax_rule_id: tax_rule.id}
```

## Solution

Replace the reference with embedded value fields (or embedded schema) that capture needed data.

```elixir
%Invoice{tax_rate_bps: tax_rule.rate_bps}
```

## Why Refactor

- Removes unnecessary joins/lookups.
- Captures historical snapshots correctly.
- Simplifies persistence and queries.

## How to Refactor

1. Confirm identity is not meaningful for this relation.
2. Add explicit value fields for required data.
3. Populate values from referenced entity.
4. Migrate read paths.
5. Remove reference field/association.
6. Run formatter and tests.

## Validation

- Behavior matches required snapshot semantics.
- No unnecessary reference lookups remain.
- Tests cover historical correctness.

## Eliminates Code Smell

- `Speculative Generality`
- `Middle Man`
