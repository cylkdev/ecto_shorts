# Change Value to Reference

## When to use

Use when any of the following are true:

- Multiple records should point to one shared canonical entity.
- Updating one entity should be visible to all related records.
- Duplicated copies of the same data are causing drift.

## Problem

Data is copied by value in many places when it should be shared by identity.

```elixir
%Order{customer_name: "Ada", customer_email: "ada@example.com"}
```

## Solution

Store a reference (such as `customer_id`) and resolve through the owning module/schema.

```elixir
%Order{customer_id: customer.id}
```

## Why Refactor

- Reduces duplicated mutable business data.
- Makes updates consistent across records.
- Clarifies ownership boundaries.

## How to Refactor

1. Identify copied value data that represents one entity.
2. Add reference fields/associations.
3. Migrate reads to resolve through the reference.
4. Backfill existing data.
5. Remove duplicated value fields.
6. Run formatter and tests.

## Validation

- Shared entities are referenced, not copied.
- Updates to canonical data propagate naturally.
- Tests cover migration and lookups.

## Eliminates Code Smell

- `Duplicate Code`
- `Data Clumps`
