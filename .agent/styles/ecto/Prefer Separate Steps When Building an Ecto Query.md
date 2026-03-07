# Prefer Separate Steps When Building an `Ecto.Query`

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose

An `Ecto.Query` should be written in a way that makes the sequence of query expressions easy for a complete beginner to understand at a glance.

Use this rule to avoid splitting one query across a partial `from/2` expression and later pipeline steps. When both forms are used together, the reader has to inspect the `from/2` expression, then return to the pipeline, and then reconstruct the full query mentally.

Write a query in one shape only:

  - Write the whole query in a single `from/2` expression, or
  - Build the query step by step by piping through query macros

## Problem

When a query is built incrementally, each query expression should appear as its own step in the pipeline.

Do not put some query expressions inside `from/2` and then continue building that same query with more pipeline steps afterward.

This makes the query harder to read because the construction is split across two shapes. Part of the query is nested inside `from/2`. The rest is added outside of it. The reader has to move in and out of the `from/2` expression to understand how the final query is assembled.

In Ecto, macros such as `where`, `order_by`, `select`, `join`, and `prepend_order_by` are all query expressions. When a query is being built in stages, those expressions should remain visible as separate steps. That makes the order of construction clear and keeps the query easier to read, review, and change.

## Example

    from(p in EctoShorts.TestPost, order_by: [desc: p.id], select: p.title)
    |> prepend_order_by([p], desc: p.title)

In this example, the query starts with `from/2`, but that `from/2` expression already contains `order_by` and `select`. The query is then extended with `prepend_order_by/3`.

This splits one query across two construction forms. Some expressions are inside `from/2`. Another expression is added in the pipeline. The full sequence is no longer visible in one consistent shape.

## Solution

When a query is built incrementally, put each query expression in its own pipeline step.

Start from the queryable, then apply each query macro one step at a time in the order the query is built. This keeps the construction visible and makes the query read as one clear sequence.

    EctoShorts.TestPost
    |> order_by([p], desc: p.id)
    |> select([p], p.title)
    |> prepend_order_by([p], desc: p.title)

In this version, each query expression appears as its own step. The reader can follow the query from top to bottom without switching between a nested `from/2` expression and later pipeline calls.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

- Use one query construction form per query. Start the query either with `from/2` or by piping from the queryable. Do not use both forms in the same query.
- Use `from/2` only when the whole query can be expressed inside that one `from/2` expression.

Prefer separate query macro calls when building an `Ecto.Query` incrementally.

## Validation Checklist

Use this checklist to verify that the rule is applied correctly.

- [ ] The query starts from a queryable or from a variable that already holds a query.
- [ ] The query uses one construction form only: either a `from/2` expression or a step-by-step pipeline.
- [ ] Each incremental query expression appears as its own pipeline step.
- [ ] No later pipeline step extends a query that already includes `where`, `order_by`, `select`, `join`, or similar expressions inside `from/2`.
- [ ] If `from/2` is used, the full query is expressed inside that single `from/2` expression.
- [ ] The same query does not combine a partial `from/2` expression with later pipeline steps that add more query expressions.
- [ ] A reader can identify the full order of query construction by reading the code from top to bottom.
- [ ] No query expression is hidden inside `from/2` when later query expressions for that same query appear in the pipeline.
- [ ] Each query expression added after the query starts is visible as its own macro call.
- [ ] Removing the pipeline would not reveal additional hidden query expressions inside `from/2`.
- [ ] A reviewer can point to one consistent composition shape for the whole query: either a single `from/2` expression or a step-by-step pipeline.
