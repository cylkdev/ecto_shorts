# Move Logger.warning from Specs to Dynamics

This RefactorPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

The `nil_specs` function in both `array_expr/specs.ex` and `scalar_expr/specs.ex` contained `EctoShorts.Logger.warning` calls inside compile-time-generated code. Specs are low-level expression builders that should be side-effect-free - they return `nil` when they cannot build an expression, and the caller decides what to do. The warning was moved to `dynamics.ex`, the module with semantic context (field name, source, expression), where it is consistent with how other warnings are logged (e.g., unknown field warnings).

## Progress

- [x] (2026-02-28) Removed Logger.warning from `array_expr/specs.ex` nil_specs catch-all.
- [x] (2026-02-28) Removed Logger.warning from `scalar_expr/specs.ex` nil_specs catch-all.
- [x] (2026-02-28) Added warnings in `dynamics.ex` at all three `build_dynamic` nil-return sites.
- [ ] Run focused tests and full test suite; record evidence and finalize retrospective.

## Surprises & Discoveries

None so far.

## Decision Log

- Decision: Remove Logger.warning from specs, add it in dynamics.ex for all nil returns from build_dynamic.
  Rationale: Specs are compiled into function clauses at build time and should not have logging side effects. The dynamics module is the right abstraction level - it has the field name, source, and expression context needed for a meaningful warning message.
  Date/Author: 2026-02-28

## Outcomes & Retrospective

Pending test verification. The warning message now includes the field name and full expression, making it more informative than the original which only mentioned the operator. The specs files are now side-effect-free.

## Context and Orientation

The dynamic expression system has this call chain:

1. `EctoShorts.CommonFilters.Filter` calls `EctoShorts.Dynamics.convert_to_dynamic/4`
2. `Dynamics` calls `dynamic_adapter.build_dynamic/4` (via the configured adapter)
3. `EctoShorts.Dynamics.Postgres.build_dynamic/4` dispatches to one of:
   - `CommonExpr.compose/3` for operator keys (`:ids`, `:before`, etc.)
   - `ArrayExpr.compose/3` for array-typed fields
   - `ScalarExpr.compose/3` for all other fields
4. These `compose` functions are generated at compile time by `EctoShorts.Compiler` from specs modules (`ArrayExpr.Specs`, `ScalarExpr.Specs`, etc.)

Specs return `nil` when they cannot build a dynamic expression. The caller in `dynamics.ex` previously silently skipped nil results. Now it logs a warning before skipping.

## Behavior Boundary (Must Remain Unchanged)

The public `EctoShorts.Dynamics.convert_to_dynamic/4` function must return identical dynamic expressions for all valid inputs. For unsupported operator+nil combinations (e.g., `%{field: %{>: nil}}`), the behavior changes from "log warning deep in specs + return nil + skip" to "return nil from specs + log warning in dynamics + skip" - same observable query result, different log message location.

## Code Smell Identified

The smell is a variant of `Feature Envy` - the specs module was performing a responsibility (logging) that belongs to its caller. Specs are data-driven expression builders; they should not have side effects. The logging belonged in `dynamics.ex`, which has the semantic context to produce a meaningful warning.

## Refactoring Technique Selected

`Inline Function` (from `.agent/refactor/techniques/composing_functions/INLINE_FUNCTION.md`) applied in reverse: instead of inlining, we extracted the side effect (logging) out of the callee and moved it to the caller where it has proper context.

## Plan of Work

1. In both `array_expr/specs.ex` and `scalar_expr/specs.ex`, replace the catch-all `_` branch in `nil_specs` from `Logger.warning(...) + nil` to just `nil`.
2. In `dynamics.ex`, add `Logger.warning` calls at all three sites where `build_dynamic` returns nil: `build_field_predicates` (two sites) and `build_operator_predicates` (one site).

## Concrete Steps

From the repository root:

    mix format
    mix test
    mix credo

## Validation and Acceptance

Run `mix test` from the repository root. All existing tests must pass. The warning messages will now originate from `EctoShorts.Dynamics` instead of `EctoShorts.Dynamics` (same prefix, different location). Any tests that capture log output for nil-operator warnings should still pass since the log prefix is unchanged.

## Idempotence and Recovery

Safe to apply multiple times. Reverting the three files restores original behavior.

## Artifacts and Notes

Before (in specs):

    _ ->
      EctoShorts.Logger.warning(
        "EctoShorts.Dynamics",
        "Expected the operator to be one of [:eq, :==, :!=] for nil comparison, got: ..."
      )
      nil

After (in specs):

    _ ->
      nil

After (in dynamics.ex, three sites):

    nil ->
      Logger.warning(
        @logger_prefix,
        "No dynamic expression generated for field #{inspect(key)} with expression: #{inspect(expr)}"
      )
      acc

## Interfaces and Dependencies

No public API changes. The `build_dynamic/4` adapter callback contract is unchanged - it still returns `Ecto.Query.dynamic_expr() | nil`.

## Milestones

### Milestone 1: Move warning from specs to dynamics

Goal: Specs become side-effect-free; dynamics.ex logs warnings with full context.

Files changed:
- `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs.ex`
- `lib/ecto_shorts/dynamics/adapters/postgres/scalar_expr/specs.ex`
- `lib/ecto_shorts/dynamics.ex`

Commands:

    mix format
    mix test

Expected outcome: All tests pass. Warning messages now include field name and expression.
