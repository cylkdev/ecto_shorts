# Fix Association Filter Routing, Validation Separation, and RULES.md

This plan governs four coordinated changes: new tests written before any code change, a targeted fix to `apply_filters/6` in `lib/ecto_shorts/common_filters.ex`, an audit of sibling builder modules for the same anti-pattern, and four new rules added to `.agent/RULES.md`. When implementation begins, this file must be moved to `./plans/` at the repository root and maintained as the governing ExecPlan per `.agent/PLANS.md`.

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

## Purpose / Big Picture

Today, when a caller passes a non-container value for an association key (e.g., `%{author: "bad value"}` where `:author` is a `belongs_to` on `Post`), the routing condition `container?(term) and association_key?(source, key)` evaluates false and execution silently falls through to later `cond` clauses or the catch-all. The caller receives no feedback. The query may be silently modified or left unchanged in a way that does not match intent, and there is no way to know why.

After this change, the routing clause dispatches on key identity alone (`association_key?(source, key)`). The term-shape check (`container?`) moves inside the branch. Invalid terms produce a `Logger.warning` and return the query unchanged, consistent with how every other builder module handles invalid input. New tests prove both paths before and after the code change. Four new RULES.md entries prevent this class of failure from recurring.

## In Scope

- Create `test/ecto_shorts/common_filters/common_filters_association_filter_test.exs` with tests proving the current happy-path behavior AND the new invalid-input behavior — written before the code change
- Fix the association routing clause in `apply_filters/6` in `lib/ecto_shorts/common_filters.ex`
- Audit all files in `lib/ecto_shorts/common_filters/` for routing+validation mixing and fix any found instances
- Add four new rules to `.agent/RULES.md`

## Out of Scope

- Changing any other behavior in `apply_filters/6` beyond the association branch
- Adding new association filter features or changing the public API
- Modifying `container?/1` or `association_key?/2` definitions
- Modifying any existing test files

## Progress

- [x] (2026-03-15) Write `test/ecto_shorts/common_filters/common_filters_association_filter_test.exs` (happy path + invalid-input tests) before any code change
- [x] (2026-03-15) Fix `apply_filters/6` routing clause in `lib/ecto_shorts/common_filters.ex`
- [x] (2026-03-15) Complete audit of all 21 `lib/ecto_shorts/common_filters/*.ex` files; no other instances found
- [x] (2026-03-15) Add four new rules to `.agent/RULES.md`; renumbered steps 26-29 to 27-30
- [x] (2026-03-15) Run full test suite: 821 tests, 0 failures

## Milestones

### Milestone 1: Tests First

Before touching any implementation file, create `test/ecto_shorts/common_filters/common_filters_association_filter_test.exs`. This file proves what the association filter shorthand is supposed to do, and establishes the expected post-fix behavior for the invalid-input path.

The happy-path tests must pass both before and after the code change. The invalid-input test will fail before Milestone 2 (because the warning does not exist yet) and must pass after.

Run: `mix test test/ecto_shorts/common_filters/common_filters_association_filter_test.exs`

Acceptance: happy-path tests pass; invalid-input test is present and fails with the right reason (no warning emitted yet).

### Milestone 2: Fix apply_filters/6 Routing

Edit lines 218–221 of `lib/ecto_shorts/common_filters.ex`.

Current (wrong — routing mixed with validation):

    container?(term) and association_key?(source, key) ->
      query
      |> ensure_association_binding(source, key, opts)
      |> reduce_association_filters(filter, source, key, term, opts)

Correct (routing on key alone; validation inside branch with explicit feedback):

    association_key?(source, key) ->
      if container?(term) do
        query
        |> ensure_association_binding(source, key, opts)
        |> reduce_association_filters(filter, source, key, term, opts)
      else
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected association filter value to be a map or keyword list, got: #{inspect(term)}"
        )

        query
      end

`@logger_prefix` is `"EctoShorts.CommonFilters"` (defined at line 141 of `lib/ecto_shorts/common_filters.ex`).

Run: `mix test test/ecto_shorts/common_filters/common_filters_association_filter_test.exs`

Acceptance: all three tests pass.

Run: `mix test test/ecto_shorts/common_filters/`

Acceptance: no regressions in the broader common_filters suite.

### Milestone 3: Audit Builder Modules

Read each file in `lib/ecto_shorts/common_filters/` and inspect every `cond` block or multi-clause function dispatch. Identify any clause where a key-identity check AND a term-shape check are combined in a single routing condition. Fix any found instance using the same pattern: route on key identity, validate term inside the branch, emit Logger.warning on failure.

From initial reading: `join.ex` (`apply_join_op/5`) checks `key in @join_types` then `key in associations` — both are key-identity checks, not term-shape checks. Clean. `windows.ex`, `with_named_binding.ex`, `order_by.ex` all validate term shape inside the handler. Clean. Remaining files not yet read: `distinct.ex`, `exclude.ex`, `filter_helpers.ex`, `group_by.ex`, `having.ex`, `last.ex`, `limit.ex`, `lock.ex`, `offset.ex`, `preload.ex`, `put_query_prefix.ex`, `recursive_ctes.ex`, `select.ex`, `set_comparison.ex`, `set_operation.ex`, `sub_query.ex`, `update.ex`, `where.ex`, `with_cte.ex`, `with_ties.ex`.

Run: `mix test` (full suite)

Acceptance: no regressions; any newly identified instances are fixed and covered by tests.

### Milestone 4: RULES.md Additions

Add four new rules to `.agent/RULES.md`.

**Rule 1 — Dispatch and validation are separate layers** (add under `Critical Evaluation And Verification`, after the existing "When splitting a multi-subject predicate..." rule):

> Before writing a condition that determines which code path executes, identify which layer of responsibility that condition belongs to. Dispatch answers "which path owns this input?" — it depends on the input's identity, type, or membership. Validation answers "is this input acceptable for that path?" — it depends on the input's shape or content. These are different questions about different things and they belong at different layers. When a single dispatch condition spans both layers, a valid-dispatch/invalid-value input is silently absorbed into the dispatch result: the system cannot distinguish between "this input doesn't belong here" and "this input belongs here but is malformed," and the caller receives no signal about which situation occurred.

**Rule 2 — Every code path has a failure contract, not just a success contract** (add immediately after Rule 1):

> Every code path has both a success contract and a failure contract. The success contract is naturally defined because the path must implement it to do anything useful. The failure contract is commonly left undesigned because the author stops thinking once the success path is complete. An undesigned failure contract is not neutral: the input goes to the wrong path, produces a silent wrong result, or disappears without trace. Before considering any code path complete, ask: for each way the input can be wrong, what does the caller observe? If the answer is not explicit and intentional, the path is incomplete. Define both contracts.

**Rule 3 — A test written before a change is a specification; a test written after is a description** (add under `Coding Guidelines`, before the TDD loop step 25):

> A test written after a code change describes what the code does. A test written before a change specifies what the code should do. If you cannot write the test before the change, it means the behavior you are about to implement is not yet specified clearly enough — that is the signal the inability is sending. Do not treat the inability to write the test first as a reason to skip it. Treat it as a sign that the task boundary, the expected behavior, or the failure modes are still unresolved, and resolve them before writing code.

**Rule 4 — Both contracts must be designed** (add to the step 28 design quality checklist after the existing "Can I delete this function..." checklist item):

> - Does each code path have both a success contract and a failure contract? For each way an input can be wrong, can the caller observe that something went wrong and why? If the failure contract is undefined, implicit, or silent, the path is incomplete regardless of whether the success path works.

Run: review `.agent/RULES.md` for placement and non-overlap with existing rules.

## Context and Orientation

`EctoShorts.CommonFilters` lives at `lib/ecto_shorts/common_filters.ex`. Its public entry is `convert_params_to_filter/3`, which normalizes params to a keyword list, sorts them (`:where` first, `:or_where` last), then reduces them through `apply_filters/6`.

`apply_filters/6` is a private `cond` router. Each clause answers "which builder owns this `{key, term}` pair?" The clauses are ordered by priority.

The association filter shorthand is a public API shape: instead of an explicit `:join` key, the caller writes the association name directly with a map or keyword value. For example, given `Post` (which has `belongs_to :author, User` and `has_many :comments, Comment`), calling `convert_params_to_filter(Post, %{author: %{age: 25}}, [])` should automatically join `:author` and apply `age == 25` as a filter on that binding.

`association_key?/2` at line 312 checks `key in (CommonSchema.get_schema_reflection(source, :associations) || [])`.
`container?/1` at line 316 checks `(is_map(term) and not is_struct(term)) or Keyword.keyword?(term)`.
`@logger_prefix` is `"EctoShorts.CommonFilters"` (line 141).

`Post` associations: `:author` (belongs_to `User`), `:authors` (many_to_many `User`), `:comments` (has_many `Comment`), `:comments_authors`, `:composite_primary_keys`.
`User` fields: `first_name`, `last_name`, `age`, `email`.
`Comment` fields: `body`, `published`, `published_at`, `replies`, `tags`.

The established invalid-input pattern throughout the codebase (see `with_named_binding.ex`, `windows.ex`, `join.ex`) is: `EctoShorts.Logger.warning(@logger_prefix, "Expected ... got: #{inspect(term)}")` followed by returning the unchanged query.

## Internal Boundary Contracts

**`apply_filters/6` cond — association branch**

Upstream caller: `apply_filters/6` reduce loop (called for each `{key, term}` pair).
This boundary: `association_key?(source, key)` routing clause.
Accepted input: any `{key, term}` where `key` is in the source schema's associations list.
Valid-term path (`container?(term)` true): delegates to `ensure_association_binding/4` then `reduce_association_filters/6`.
Invalid-term path (`container?(term)` false): logs warning, returns query unchanged.
Not the routing clause's concern: whether the term is a valid shape — that is checked inside after routing.

**`reduce_association_filters/6`**

Accepts `(query, filter, source, key, term, opts)` where `term` is guaranteed by the caller to be a container. Does not re-validate term shape.

## Internal Structure Walkthrough

Happy path — `convert_params_to_filter(Post, %{author: %{age: 25}}, [])`:

1. `convert_params_to_filter/3` normalizes to `[{:author, %{age: 25}}]`, sorts, reduces.
2. `apply_filters(:where, Post, query, {:as, nil}, {:author, %{age: 25}}, [])` called.
3. `cond` evaluates: `:author` not in `@binding_operator`, not in predicate group, not in post-aggregate group.
4. `association_key?(Post, :author)` → `true`.
5. `container?(%{age: 25})` → `true` → enters success branch.
6. `ensure_association_binding` creates `join: a in assoc(p, :author), as: :author` if not already present.
7. `reduce_association_filters` recurses with binding `{:as, :author}` and filter `{:age, 25}`.

Invalid-term path — `convert_params_to_filter(Post, %{author: "bad value"}, [])`:

Steps 1–4 identical.
5. `container?("bad value")` → `false` → else branch.
6. `EctoShorts.Logger.warning("EctoShorts.CommonFilters", "Expected association filter value to be a map or keyword list, got: \"bad value\"")`.
7. Returns query unchanged.

## Example Mappings

### Story: Association filter shorthand routing

A caller filters across an association by passing the association name as a key with a nested map or keyword list. The system joins the association automatically and applies the nested filters on that binding.

#### Rules:

- When the key is a known association on the source AND the value is a map or keyword list, the association is joined and nested filters are applied on the joined binding
- When the key is a known association AND the value is NOT a map or keyword list, a Logger warning is emitted and the query is returned unchanged
- Routing dispatches on key identity alone; term validation is never part of the routing condition

#### Examples:

    # Valid — map value for belongs_to association
    CommonFilters.convert_params_to_filter(Post, %{author: %{age: 25}}, [])
    # => query with join on :author, where a.age == 25

    # Valid — keyword list value
    CommonFilters.convert_params_to_filter(Post, [author: [age: 25]], [])
    # => same query as above

    # Invalid — scalar value for a known association key
    CommonFilters.convert_params_to_filter(Post, %{author: "bad value"}, [])
    # => query unchanged
    # => Logger warning: "Expected association filter value to be a map or keyword list, got: \"bad value\""

#### Open Questions:

- **Q:** Should invalid-term behavior raise instead of warn? **A:** Logger.warning + return query unchanged (confirmed).

## Behaviour Specifications

### Feature: Association filter shorthand routing

Scenario: Valid association key with container value
  Given `Post` has a known association `:author`
  When `convert_params_to_filter/3` is called with `%{author: %{age: 25}}`
  Then the returned query includes a join on `:author` and a where clause applying `age == 25` to that binding

Scenario: Valid association key with non-container value
  Given `Post` has a known association `:author`
  When `convert_params_to_filter/3` is called with `%{author: "bad value"}`
  Then the returned query is unchanged
  And a Logger warning is emitted containing "Expected association filter value to be a map or keyword list"

Scenario: Existing non-association field key (unchanged behavior)
  Given `Post` has a field `:published` that is not an association
  When `convert_params_to_filter/3` is called with `%{published: true}`
  Then this path is not taken; the field filter path handles it normally

## Executable Tests

File: `test/ecto_shorts/common_filters/common_filters_association_filter_test.exs`

    defmodule EctoShorts.CommonFilters.AssociationFilterTest do
      use ExUnit.Case, async: true
      use EctoShorts.Testing

      alias EctoShorts.CommonFilters
      alias EctoShorts.Schema.Post

      import Ecto.Query
      import ExUnit.CaptureLog

      describe "convert_params_to_filter/3 association filter shorthand" do
        test "routes a map value for a known association key through the association handler" do
          expected =
            from(p in Post,
              join: a in assoc(p, :author),
              as: :author,
              where: a.age == ^25
            )

          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{author: %{age: 25}},
              []
            )

          assert_query(expected, actual)
        end

        test "routes a keyword list value for a known association key through the association handler" do
          expected =
            from(p in Post,
              join: a in assoc(p, :author),
              as: :author,
              where: a.age == ^25
            )

          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              [author: [age: 25]],
              []
            )

          assert_query(expected, actual)
        end

        test "returns query unchanged and logs a warning when a known association key receives a scalar value" do
          expected = from(p in Post)

          log =
            capture_log(fn ->
              actual =
                CommonFilters.convert_params_to_filter(
                  Post,
                  %{author: "bad value"},
                  []
                )

              assert_query(expected, actual)
            end)

          assert log =~ "Expected association filter value to be a map or keyword list"
        end
      end
    end

## Concrete Steps

All commands run from `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

1. Create `test/ecto_shorts/common_filters/common_filters_association_filter_test.exs` with the content above.
2. Run `mix test test/ecto_shorts/common_filters/common_filters_association_filter_test.exs` — happy-path tests should pass; invalid-input test should fail (no warning emitted yet).
3. Edit `lib/ecto_shorts/common_filters.ex` lines 218–221 as described in Milestone 2.
4. Run `mix test test/ecto_shorts/common_filters/common_filters_association_filter_test.exs` — all three tests must pass.
5. Run `mix test test/ecto_shorts/common_filters/` — no regressions.
6. Read remaining `lib/ecto_shorts/common_filters/*.ex` files; fix any routing+validation mixing found.
7. Run `mix test` — full suite passes.
8. Add four rules to `.agent/RULES.md` as described in Milestone 4.

## Validation and Acceptance

Run `mix test`. All tests pass. The new test file contains three passing tests. Running with `--seed 0` or any seed produces no failures. The `capture_log` assertion in the invalid-input test matches the warning message produced by the fixed code.

## Idempotence and Recovery

All steps are additive except the edit to `lib/ecto_shorts/common_filters.ex`. That edit is a single clause change in a `cond` block. Reverting means restoring lines 218–221 to `container?(term) and association_key?(source, key) ->` with the original pipe body.

## Surprises & Discoveries

- Initial audit of `join.ex`: `apply_join_op/5` uses nested `if` to check `key in @join_types` then `key in associations` — both checks are key-identity checks, not term-shape checks. This is not the routing+validation anti-pattern.
- Full audit of all 21 builder modules confirmed only one instance of routing+validation mixing existed: the association branch in `apply_filters/6`. Every other module validates term shape inside its handler with explicit Logger.warning.
- Pre-fix behavior of the invalid-input path was worse than expected: `%{author: "bad value"}` did not silently return the query unchanged — it added `where: p0.author == ^"bad value"` to the query, actively corrupting it.

## Decision Log

- Decision: Logger.warning + return query unchanged on invalid association term.
  Rationale: Consistent with the established pattern in `with_named_binding.ex`, `windows.ex`, `join.ex`. Confirmed by user.
  Date/Author: 2026-03-15

## Outcomes & Retrospective

All four milestones complete. Three new tests prove the association filter shorthand contract: two happy-path tests pass before and after the code change; the invalid-input test proved the pre-fix behavior was actively wrong (corrupt where clause added), not merely silent. The fix isolates routing to `association_key?` and surfaces the failure contract via `Logger.warning` + unchanged query. Full test suite passes: 821 tests, 0 failures. Audit of all 21 builder modules found no other instances of the routing+validation mixing anti-pattern. Four new rules added to `.agent/RULES.md` covering dispatch/validation separation, failure contract design, test-before-change, and both-contracts checklist item.
