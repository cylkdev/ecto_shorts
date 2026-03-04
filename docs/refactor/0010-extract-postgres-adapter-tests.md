# Extract Postgres Adapter Tests into Dedicated Module

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

Reference: `.agent/REFACTOR_PLANS.md`

## Purpose / Big Picture

The test file `test/ecto_shorts/dynamics_test.exs` currently mixes tests for two different modules: `EctoShorts.Dynamics` (boolean operator unwrapping, helper expression preprocessing, schema-less fallback) and `EctoShorts.Dynamics.Postgres` (operator routing, scalar expressions, array expressions, common expressions). This makes it hard to tell which module a test is exercising and where to add new tests when either module changes.

After this refactor, each module has its own test file. The Postgres adapter tests call `Postgres.build_dynamic/4` directly, making the test-to-code relationship explicit. The `dynamics_test.exs` file retains only tests that exercise `Dynamics`-level preprocessing logic.

Behaviour that must remain unchanged: every dynamic expression currently asserted in `dynamics_test.exs` must still be asserted somewhere after the refactor. No production code changes.

Verification: `mix test --seed 0 --trace` passes with the same number of assertions and 0 failures.

## Progress

- [x] (2026-03-01 00:31Z) Identified behaviour boundary and test groupings.
- [x] (2026-03-01 00:31Z) Created RefactorPlan document.
- [x] (2026-03-01 00:45Z) Created `test/ecto_shorts/dynamics/adapters/postgres_test.exs` with 117 tests calling `Postgres.build_dynamic/4` directly. All green.
- [x] (2026-03-01 00:50Z) Removed moved tests from `dynamics_test.exs`. 20 tests remain, all green.
- [x] (2026-03-01 00:52Z) Full suite: 9 doctests, 1014 tests, 0 failures.

## Surprises & Discoveries

- Observation: The adapter does not process `{:datetime, {:add, ...}}` or `{:date, {:add, ...}}` tuples. These are preprocessed by `Dynamics.apply_helper_expressions` before reaching `build_dynamic/4`. Initial attempt to test them directly in the adapter file failed with mismatched ASTs.
  Evidence: Tests passed `{:>=, {:datetime, {:add, %{...}}}}` to `build_dynamic/4` and received the raw tuple back instead of a `datetime_add` expression. Removed these 2 tests from `postgres_test.exs` and confirmed they remain in `dynamics_test.exs`.

## Decision Log

- Decision: Rewrite moved tests to call `Postgres.build_dynamic/4` directly instead of keeping them calling `Dynamics.convert_to_dynamic/3`.
  Rationale: User chose Option B. One clean set of tests targeting the adapter's own public function, no duplication.
  Date/Author: 2026-03-01 / agent

- Decision: Keep datetime/date helper tests, query-builder payload tests, and boolean operator tests in `dynamics_test.exs`.
  Rationale: These test `Dynamics`-level preprocessing (helper expression application, boolean unwrapping, subquery building), not the adapter's expression generation.
  Date/Author: 2026-03-01 / agent

- Decision: Common operator tests for `:ids`, `:after`, `:before`, `:start_date`, `:end_date` move to the adapter test because the adapter's `CommonExpr` is the module that generates the dynamic expression for these keys.
  Rationale: The `Dynamics` module just passes the value through to `build_dynamic/4` for operator keys.
  Date/Author: 2026-03-01 / agent

## Outcomes & Retrospective

The refactor is complete. `dynamics_test.exs` was reduced from ~1513 lines to ~338 lines, containing only tests for `Dynamics`-level preprocessing: boolean operators, struct value preservation, query-builder payload handling for all/any/exists, datetime/date helper expressions, and keyword params.

The new `postgres_test.exs` contains 117 tests that directly exercise `Postgres.build_dynamic/4` covering: `operators/0`, `operator?/1`, common operators, scalar expressions (equality, comparison, nil, like/ilike, lower/upper, aggregates, all/any with raw subqueries, arithmetic), array expressions (equality, comparison, nil, like/ilike, lower/upper, :in/:all), and schema-less fallback.

Full suite: 9 doctests, 1014 tests, 0 failures.

## Context and Orientation

`EctoShorts.Dynamics` (in `lib/ecto_shorts/dynamics.ex`) is a preprocessing layer. It receives filter parameter maps, unwraps boolean operators (`:and`, `:or`), applies helper expressions (`:datetime`, `:date`, subquery builders), normalizes expression params into `{operator, value}` tuples, and delegates each tuple to the configured adapter's `build_dynamic/4`.

`EctoShorts.Dynamics.Postgres` (in `lib/ecto_shorts/dynamics/adapters/postgres.ex`) is the adapter. Its `build_dynamic/4` receives a normalized source, a binding selector, a field key, and an expression (either a raw value for operator keys, or a `{operator, value}` tuple for field keys). It routes to three sub-modules: `CommonExpr` (operator keys), `ArrayExpr` (array fields), and `ScalarExpr` (scalar fields).

The current test file `test/ecto_shorts/dynamics_test.exs` contains ~150 tests. About 22 test `Dynamics`-level logic. The rest exercise Postgres adapter expression generation through the `Dynamics.convert_to_dynamic/3` indirection.

The expression shape difference is critical. `Dynamics.convert_to_dynamic` normalizes `%{title: %{like: "foo"}}` into a call like `build_dynamic(source, binding, :title, {:like, "foo"})`. The direct adapter tests must pass the already-normalized tuple.

## Behaviour Boundary (Must Remain Unchanged)

No production code changes. Every dynamic expression currently asserted must still be asserted in exactly one test file after the refactor.

Observable verification: `mix test --seed 0 --trace` passes with 0 failures and no test count decrease.

## Code Smell Identified

Large Module (test file variant) from `.agent/refactor/code_smells/bloaters/LARGE_MODULE.md`. The test file `test/ecto_shorts/dynamics_test.exs` at 1513 lines mixes concerns for two distinct modules, making it difficult to locate and maintain tests for either module independently.

## Refactoring Technique Selected

Extract Function (adapted to test extraction) from `.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`. The technique is to identify the subset of tests that belong to the Postgres adapter's responsibility, extract them into a new test module, and rewrite their entry point from the indirect `Dynamics.convert_to_dynamic/3` to the direct `Postgres.build_dynamic/4`.

## Plan of Work

The work proceeds in two phases. First, create the new test file with all Postgres adapter tests rewritten to call `build_dynamic/4` directly. Second, remove those tests from the old file and verify both files independently.

The new file mirrors the directory structure of the source: `test/ecto_shorts/dynamics/adapters/postgres_test.exs` maps to `lib/ecto_shorts/dynamics/adapters/postgres.ex`.

For common operator tests (`:ids`, `:after`, `:before`, `:start_date`, `:end_date`), the adapter receives the raw value directly. For exists tests with raw subqueries, the adapter receives the subquery or `{:not, subquery}`. For scalar and array field tests, the adapter receives `{operator, value}` tuples.

## Concrete Steps

All commands run from `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

1. Create `test/ecto_shorts/dynamics/adapters/postgres_test.exs`.
2. Run: `mix test test/ecto_shorts/dynamics/adapters/postgres_test.exs --seed 0 --trace`
3. Edit `test/ecto_shorts/dynamics_test.exs` to remove moved tests.
4. Run: `mix test test/ecto_shorts/dynamics_test.exs --seed 0 --trace`
5. Run: `mix test --seed 0 --trace`

## Validation and Acceptance

Run `mix test --seed 0 --trace` and expect 0 failures. The total test count should be equal to or greater than the current count (currently ~814 tests plus 9 doctests based on recent memory). The new `postgres_test.exs` file should contain tests for `operators/0`, `operator?/1`, and `build_dynamic/4` covering common operators, scalar expressions, and array expressions.

## Idempotence and Recovery

All steps are safe to repeat. The new test file is additive. Removing tests from the old file only happens after the new file is verified green. If something goes wrong, reverting the old file restores all tests.

## Artifacts and Notes

(To be filled during implementation.)

## Interfaces and Dependencies

In `lib/ecto_shorts/dynamics/adapters/postgres.ex`, the public API under test:

    @callback operators() :: list(atom())
    @callback operator?(key :: atom()) :: boolean()
    @callback build_dynamic(source, binding_selector, key, expr) :: Ecto.Query.dynamic_expr()

No changes to this interface. Tests only exercise these three functions.

## Milestones

### Milestone 1: New test file green

Create `test/ecto_shorts/dynamics/adapters/postgres_test.exs` with all Postgres adapter tests rewritten to call `build_dynamic/4` directly. Run `mix test test/ecto_shorts/dynamics/adapters/postgres_test.exs --seed 0 --trace` and expect all tests to pass.

### Milestone 2: Old test file trimmed and green

Remove moved tests from `test/ecto_shorts/dynamics_test.exs`. Run `mix test test/ecto_shorts/dynamics_test.exs --seed 0 --trace` and expect remaining tests to pass.

### Milestone 3: Full suite green

Run `mix test --seed 0 --trace` and expect 0 failures with no test count decrease.
