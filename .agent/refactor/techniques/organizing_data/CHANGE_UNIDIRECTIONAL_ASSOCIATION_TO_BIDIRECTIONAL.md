# Change Unidirectional Association to Bidirectional

## When to use

Use when any of the following are true:

- You frequently navigate relationship in both directions.
- Reverse lookup is repeated or expensive.
- Both sides are meaningful in the domain model.

## Problem

Only one side of an association is represented, causing repeated reverse queries or manual plumbing.

## Solution

Model both sides of the association explicitly.

```elixir
# Example with Ecto schemas and associations:
# Team has_many :members
# Member belongs_to :team
```

## Why Refactor

- Makes navigation explicit in both directions.
- Reduces repeated reverse lookup logic.
- Clarifies domain model relationships.

## How to Refactor

1. Add reverse association field/schema relation.
2. Update preload/query paths.
3. Ensure write paths maintain both sides correctly.
4. Add tests for forward and reverse traversal.

## Validation

- Both navigation paths are available and correct.
- No duplicated reverse lookup logic remains.
- Tests verify consistency.

## Eliminates Code Smell

- `Message Chains`
- `Duplicate Code`
