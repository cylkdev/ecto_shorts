# Refactor `ScalarExprBuilder` to compile faster without changing scalar filter behavior

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

After this change, callers will still be able to use `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5` and higher-level filter entry points exactly as they do today, but the repository should spend less time compiling the generated scalar expression modules. The user-visible proof is not a new API. The proof is that the existing scalar expression tests and higher-level query tests keep passing while the `ScalarExpr` generation path measurably compiles faster.

The concrete outcome is a smaller and less repetitive generated comparison module. Today, the generated comparison file is large enough to create substantial work in three places: while `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` builds quoted Elixir syntax trees, while `lib/ecto_shorts/generator.ex` renders those syntax trees back into source text, and while the generated source is compiled into BEAM bytecode by `Kernel.ParallelCompiler`. This ExecPlan reduces that work without changing query semantics.

## In Scope

The work in this plan is limited to the internal scalar-expression generation pipeline. That includes refactoring `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex`, adjusting internal dispatch in `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex` if needed to support smaller generated modules, and changing closely related generator internals such as `lib/ecto_shorts/generator.ex`, `lib/ecto_shorts/generator/builder.ex`, `lib/ecto_shorts/compiler.ex`, and `lib/ecto_shorts/dynamics/helpers.ex` when those edits are necessary to reduce compile cost.

The plan also includes adding or updating observable tests that prove scalar-expression behavior stays unchanged at the public boundary. The main focused test boundary is `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`, and the neighboring integration boundaries are `test/ecto_shorts/dynamics/postgres_test.exs` and `test/ecto_shorts/common_filters_scalar_filter_test.exs`.

The plan includes establishing one reproducible compile-time measurement workflow using existing tooling only. No new dependency is allowed for benchmarking or profiling.

## Out of Scope

This plan must not change the public shape of scalar filters, the meaning of existing operators, or the caller-facing contract of `EctoShorts.CommonFilters.convert_params_to_filter/3`. It must not redesign unrelated filter families such as array or common expression handling unless a narrowly-scoped change is required to keep the scalar refactor correct.

This plan must not add runtime-only features, cosmetic cleanup, or unrelated generator refactors that do not materially support the compile-time goal. It must not add external dependencies, long-lived custom profiling frameworks, or behavior changes that require new user documentation.

## Progress

- [x] (2026-03-13 22:55Z) Read `.agent/RULES.md`, re-read the updated rule requiring an `ExecPlan`, and read `.agent/PLANS.md`.
- [x] (2026-03-13 22:57Z) Researched the scalar-expression compile path from `lib/ecto_shorts/dynamics/postgres.ex` into `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`, `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex`, `lib/ecto_shorts/generator.ex`, and `lib/ecto_shorts/compiler.ex`.
- [x] (2026-03-13 23:00Z) Verified the current public validation boundaries in `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`, `test/ecto_shorts/dynamics/postgres_test.exs`, and `test/ecto_shorts/common_filters_scalar_filter_test.exs`.
- [x] (2026-03-13 23:05Z) Measured the current generated comparison file size by inspection: `priv/generated/dynamics/postgres/scalar_expr_builder/comparison.ex` is approximately `4,024,318` bytes and about `91k` lines.
- [x] (2026-03-13 23:08Z) Rewrote the prior planning note into this `ExecPlan` so the task now has a living source of truth.
- [x] (2026-03-13 23:39Z) Chose the official acceptance benchmark: a clean `mix compile` from the repository root is the authoritative compile-time measurement for this refactor.
- [ ] Capture a reproducible before-change compile-time baseline for the scalar generation path using the official clean `mix compile` benchmark.
- [ ] Refactor one comparison slice in `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` to prove a lower-clause-count generation pattern while preserving observable behavior.
- [ ] Extend the proven reduction pattern across the remaining scalar comparison families and update the plan as discoveries occur.
- [ ] Reassess `lib/ecto_shorts/generator.ex` rendering and formatting overhead after clause reduction, then implement the narrowest justified change.
- [ ] Re-run compile and test validation, record the evidence in this plan, and complete the retrospective.

## Milestones

### Milestone 1: Establish a reliable baseline and a proof boundary

The first milestone makes the problem measurable and pins the public behavior that must not change. At the end of this milestone, a novice should be able to run one repeatable compile workflow from the repository root, know which generated file is the main outlier, and know which tests must continue to pass after every refactor step.

The work for this milestone happens entirely in observation and validation. The implementer should confirm that `lib/ecto_shorts/dynamics/postgres.ex` calls `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5` for non-array, non-common scalar fields, and that `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex` routes work into four generated modules: comparison, membership, string, and string-transform. The implementer should also confirm that `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` directly exercises `ScalarExpr.dynamic_expr/5` across equality, inequality, membership, string matching, transform wrappers, aggregate helpers, quantified terms, named bindings, and positional bindings. That focused test file is the main behavior anchor for the refactor.

The compile-time baseline must use existing repository tooling only. A clean `mix compile` from `/Users/kurthogarth/Documents/GitHub/ecto_shorts` is the official acceptance benchmark because it exercises the existing code generation and compilation flow without new scripts and the user selected it as the benchmark of record. A more targeted repeatable command may still be recorded as supporting evidence during implementation, but it does not replace the official benchmark unless this plan is explicitly revised. At a minimum, record wall-clock compile time and the generated file size of `priv/generated/dynamics/postgres/scalar_expr_builder/comparison.ex`.

Acceptance for this milestone is that a future implementer can follow the plan alone, run the documented compile command, identify the files most relevant to the issue, and run the focused scalar tests before making any change.

### Milestone 2: Replace clause explosion with smaller shared generation patterns

The second milestone is the core design change. At the end of it, `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` should generate materially less code for comparison handling while still producing the same public outcomes. The intent is not to remove the generated-module approach outright. The intent is to stop emitting one large inline clause body for every operator, negation, wrapper, interval, and binding permutation when those permutations can be handled by smaller dispatch layers plus shared helper functions.

Today, `comparison_conditions/5` in `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` multiplies the clause count by combining comparison operators, negated forms, quantified forms, `nil` cases, arithmetic wrappers, datetime wrappers, date wrappers, and aggregate helpers inside the builder itself. The preferred direction is to keep generated module entry points stable, especially `dynamic_expr/4` inside the generated modules, while moving repeated expression construction into ordinary internal functions that take explicit resolved terms. This follows Elixir’s macro guidance to keep quoted content short and avoids paying compile cost for repeated quoted bodies that differ only slightly.

The first implementation step inside this milestone should refactor only one semantic slice to prove the pattern. A good first slice is plain comparison operators plus their negated and `nil` variants, because that slice is heavily repeated and well covered in `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`. If that first slice shows a meaningful file-size reduction and keeps tests green, the same pattern can be extended to arithmetic wrappers, aggregate comparisons, and date or datetime wrappers in separate small batches. Each batch must update this plan’s `Progress`, `Surprises & Discoveries`, and `Decision Log` sections before moving on.

Acceptance for this milestone is observable, not stylistic. The generated comparison module should become materially smaller than before, the scalar-expression tests should still pass, and the plan should clearly explain which duplication pattern was removed and how the replacement works.

### Milestone 3: Remove avoidable generator overhead after clause reduction

The third milestone happens only after the generated comparison module is meaningfully smaller. At the end of it, the compile-time path should avoid any source rendering or formatting work that no longer pulls its weight. The reason for waiting is that it will be hard to tell whether a generator change matters if the plan has not yet reduced the size of the underlying generated syntax trees.

The relevant files are `lib/ecto_shorts/generator.ex` and `lib/ecto_shorts/compiler.ex`. `lib/ecto_shorts/generator.ex` currently builds clauses, converts each quoted clause into an algebra document with `Code.quoted_to_algebra/1`, joins the text, and then runs `Code.format_string!/2` on the whole generated module. That means the current implementation pays both syntax-tree generation cost and pretty-printing cost before the source is compiled. Once clause reduction is in place, reassess whether full formatting is still required for generated files. If skipping or simplifying formatting yields a measurable win without making generated compile errors unusable, it is in scope. If the formatting step remains necessary for diagnosis or determinism, record that decision explicitly and leave it in place.

The generated-module topology may also be revisited here, but only if clause reduction alone is not enough. If a single comparison module remains too large, split it by stable semantic families behind the same public dispatcher in `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`. Any split must be driven by measured compile benefit, not aesthetics, and the public entry point `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5` must remain unchanged.

Acceptance for this milestone is that the compile-time measurement improves further or at least remains improved after any generator-path change, and that the plan captures why the chosen generator adjustments were worth keeping.

### Milestone 4: Validate behavior end to end and close the plan

The final milestone turns the refactor into a complete, demonstrable result. At the end of it, a novice should be able to run the documented compile command, run the focused and neighboring tests, compare the before-and-after evidence captured in this plan, and conclude that the scalar-expression path still behaves the same while compiling faster.

Validation should begin with the focused boundary in `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`, because that file directly exercises the public scalar expression entry point. Then run `test/ecto_shorts/dynamics/postgres_test.exs` to prove higher-level dispatcher behavior through `EctoShorts.Adapters.Postgres.build_dynamic/4`, and run the scalar-focused portion of `test/ecto_shorts/common_filters_scalar_filter_test.exs` to prove that `EctoShorts.CommonFilters.convert_params_to_filter/3` still sees unchanged scalar behavior. If other nearby tests fail and the failure is genuinely caused by the refactor, update the plan and incorporate the additional boundary. If failures are ambiguous, stop and resolve the ambiguity before changing tests or code.

Acceptance for this milestone is that the plan contains the concrete compile results, the exact test commands, the observed passing outcomes, and a short retrospective on what delivered the compile-time win.

## Surprises & Discoveries

- Observation: the generated comparison module is dramatically larger than the other scalar generated modules.
  Evidence: `priv/generated/dynamics/postgres/scalar_expr_builder/comparison.ex` is about `4,024,318` bytes, while `membership.ex` is about `17,420` bytes and `string_upper_lower.ex` is about `72,174` bytes.

- Observation: compile cost is likely not only in BEAM compilation. The current path also pays for syntax-tree rendering and full formatting before compilation.
  Evidence: `lib/ecto_shorts/generator.ex` converts clauses with `Code.quoted_to_algebra/1` and then formats the resulting module with `Code.format_string!/2`.

- Observation: the most authoritative public behavior boundary already exists and is broad.
  Evidence: `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` covers plain scalar values, `nil`, operator aliases, list-membership coercion, quantified values, aggregate helpers, string transforms, named bindings, and positional bindings.

- Observation: the plan must treat generated-module attributes carefully.
  Evidence: prior repository work established that builder module attributes do not automatically exist in generated modules unless their literal values are expanded into generated code.

## Decision Log

- Decision: keep the public entry point `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5` unchanged and treat the refactor as internal-only unless later evidence proves that a public change is necessary.
  Rationale: the user asked for compile-time improvement, not a contract redesign, and the existing test suite already provides strong public-boundary coverage for the current interface.
  Date/Author: 2026-03-13 / Cascade

- Decision: use `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` as the primary behavioral anchor and `test/ecto_shorts/dynamics/postgres_test.exs` plus `test/ecto_shorts/common_filters_scalar_filter_test.exs` as neighboring integration boundaries.
  Rationale: these files collectively prove the direct scalar boundary, the adapter dispatch boundary, and a higher-level filter boundary without asserting on private `Ecto.Query` internals.
  Date/Author: 2026-03-13 / Cascade

- Decision: reduce generated clause explosion before attempting generator rendering changes.
  Rationale: the current comparison file size strongly suggests that code volume is the dominant cost driver, so measuring formatter-only tweaks before shrinking the file would risk optimizing the wrong layer first.
  Date/Author: 2026-03-13 / Cascade

- Decision: rewrite the prior markdown plan into a true `ExecPlan`.
  Rationale: `.agent/RULES.md` now requires an `ExecPlan` for planning, implementing, and refactoring behavior-bearing code, and `.agent/PLANS.md` defines the required living-document structure.
  Date/Author: 2026-03-13 / Cascade

- Decision: treat a clean `mix compile` from `/Users/kurthogarth/Documents/GitHub/ecto_shorts` as the official acceptance benchmark for compile-time improvement.
  Rationale: this is the simplest end-to-end measurement of the existing generation and compilation path, and the user explicitly selected it as the acceptance target.
  Date/Author: 2026-03-13 / Cascade

## Outcomes & Retrospective

The task is not implemented yet, so there is no finished outcome to report. The current outcome is that the repository now has a self-contained `ExecPlan` that documents the public behavior boundary, the most likely compile hot spots, the intended implementation order, and the proof expectations for each milestone.

The main lesson from the research phase is that this task is not only about Elixir macros in the abstract. In this repository, the compile cost is created by a specific pipeline: large quoted Elixir syntax trees are built in `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex`, turned back into source text in `lib/ecto_shorts/generator.ex`, written to `priv/generated`, and then compiled. The plan therefore prioritizes reducing generated code volume before tuning downstream compilation steps.

## Context and Orientation

The scalar filter path begins in `lib/ecto_shorts/dynamics/postgres.ex`. Inside `build_expr/5`, when the selected field is neither a common operator nor an array field, the module routes the request into `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5`. That module lives in `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`. It normalizes plain scalar values into `{:==, value}`, chooses one generated module based on the operator and term shape, and then calls `dynamic_expr/4` on the selected generated module.

The generated modules are declared through `use EctoShorts.Compiler` in `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`. The compiler hook calls `lib/ecto_shorts/generator.ex`, which builds clauses through `lib/ecto_shorts/generator/builder.ex`, writes source files under `priv/generated`, and compiles them with `Kernel.ParallelCompiler` in `lib/ecto_shorts/compiler.ex`. The builder responsible for the large comparison file is `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex`.

In this repository, a “generated module” means an Elixir source file created at compile time and placed under `priv/generated`. The generated modules are then compiled like ordinary Elixir source files. A “quoted syntax tree” means Elixir code represented as data. `ScalarExprBuilder` constructs those trees with `quote`, and `Generator` later turns them back into source text. That means compile-time work can be reduced either by building fewer trees, building smaller trees, or doing less rendering and formatting work before compile.

The most important test helper is `EctoShorts.Testing`, defined in `lib/ecto_shorts/testing.ex`. It provides public-behavior assertions such as `assert_dynamic/2`, `assert_sql/3`, and `assert_query/2`. The repository rule for this task is to validate observable behavior through those helpers and not by asserting on internal `Ecto.Query` struct fields.

## Plan of Work

Start by capturing a compile baseline from the repository root at `/Users/kurthogarth/Documents/GitHub/ecto_shorts`. Record the exact command and wall-clock time in this plan. Also record the size of `priv/generated/dynamics/postgres/scalar_expr_builder/comparison.ex`. This baseline is required before any behavioral refactor so the plan can later prove that the changes achieved the intended outcome.

Next, change `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` in the smallest behavior-preserving slice that reduces clause multiplication. The first change should target a clearly-bounded part of `comparison_conditions/5`, not the whole file at once. The preferred first slice is plain comparisons and their negated or `nil` forms. Introduce regular helper functions where they reduce repeated `quote do ... end` blocks. Keep helper inputs explicit so the generated modules receive resolved terms instead of needing builder-only context.

After that first slice, update or add focused tests only if the current coverage does not already prove the preserved behavior. Use `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` first. If the refactor changes dispatch shape in `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`, run `test/ecto_shorts/dynamics/postgres_test.exs` as the next boundary. If a higher-level scalar filter behavior is potentially affected, run the relevant tests in `test/ecto_shorts/common_filters_scalar_filter_test.exs`.

Once the new pattern is proven and adopted across the remaining comparison branches, re-open `lib/ecto_shorts/generator.ex` and assess whether the current `Code.quoted_to_algebra/1` plus `Code.format_string!/2` pipeline is still justified. If a change there produces a compile-time win without harming diagnosability or determinism, keep it and record the evidence in this plan. If it does not, revert or avoid that change and record why.

Finally, if the comparison module still remains too large after deduplication, split the generated comparison responsibilities in `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex` into smaller internal modules behind the same public dispatcher. Any such split must preserve the public caller contract and be supported by measurable benefit.

## Example Mappings

### Story: Scalar expressions keep their current meaning while compilation gets faster

The refactor changes how scalar-expression code is generated, not what scalar filters mean. A caller using the public scalar expression boundary or the higher-level filter boundary should still receive the same dynamic expressions and SQL behavior after the refactor.

#### Rules:

- A plain scalar value passed to `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5` must still behave as equality by default.
- Omitting an explicit operator must still preserve current behavior by normalizing the term to `{:==, value}`.
- Invalid or unsupported scalar term shapes must still return `nil` through the generated-module fallback path unless the current public behavior proves otherwise.
- Existing behavior for membership, string operators, string transforms, aggregates, quantified values, named bindings, and positional bindings must remain unchanged.

#### Examples:

- `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr({:as, nil}, :id, nil, 1, [])`
  returns a dynamic equivalent to `dynamic([q], field(q, :id) == ^1)`.
- `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr({:as, nil}, :published_at, nil, nil, [])`
  returns a dynamic equivalent to `dynamic([q], is_nil(field(q, :published_at)))`.
- `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr({:as, nil}, :published, nil, {:!=, [true, false]}, [])`
  returns a dynamic equivalent to `dynamic([q], is_nil(field(q, :published)) or field(q, :published) not in ^[true, false])`.
- `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr({:as, nil}, :id, nil, {:unsupported, 1}, [])`
  returns `nil`.

#### Open Questions:

- **Q:** What compile command should be treated as the official baseline for this task? **A:** Use clean `mix compile` from the repository root as the official acceptance benchmark. A targeted repeatable compile command may be recorded as supporting evidence, but not as the primary benchmark unless this plan is explicitly revised.
- **Q:** Should the refactor keep one comparison generated module or allow multiple smaller comparison generated modules? **A:** Allow multiple smaller internal modules only if clause reduction alone is insufficient and the public dispatcher in `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5` stays unchanged.
- **Q:** Should generator formatting be removed immediately? **A:** No. Reassess formatting only after code-volume reduction is in place so the measurement reflects the real dominant cost.

## Behaviour Specifications

### Feature: Direct scalar expression generation remains unchanged

Scenario: A caller passes a plain scalar term to the public scalar expression entry point
  Given `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5`
  When the caller passes `{:as, nil}`, key `:id`, `nil` negation, and term `1`
  Then the function returns a dynamic equivalent to `field(q, :id) == ^1`

Scenario: A caller omits an explicit operator by passing `nil`
  Given `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5`
  When the caller passes `{:as, nil}`, key `:published_at`, `nil` negation, and term `nil`
  Then the function returns a dynamic equivalent to `is_nil(field(q, :published_at))`

Scenario: A caller uses a supported wrapped scalar term
  Given `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5`
  When the caller passes a supported comparison, membership, string, transform, aggregate, or quantified term currently covered by `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`
  Then the function returns the same observable dynamic or SQL behavior as before the refactor

Scenario: A caller passes an unsupported term shape
  Given the generated scalar comparison modules
  When `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5` routes to a generated module that does not match the term
  Then the generated module returns `nil`

### Feature: Higher-level scalar filter routing remains unchanged

Scenario: The Postgres adapter routes scalar terms through the same public path
  Given `EctoShorts.Adapters.Postgres.build_dynamic/4`
  When a scalar field term is neither a common expression nor an array expression
  Then the returned dynamic matches the same observable SQL or dynamic behavior as before the refactor

Scenario: CommonFilters still build the same scalar SQL
  Given `EctoShorts.CommonFilters.convert_params_to_filter/3`
  When the caller passes scalar comparison parameters already covered in `test/ecto_shorts/common_filters_scalar_filter_test.exs`
  Then the returned query matches the same observable SQL as before the refactor

### Feature: Compilation becomes measurably faster

Scenario: The scalar generation path is recompiled after the refactor
  Given the documented compile command in this plan
  When the command is run from the repository root before and after the refactor
  Then the after-change measurement is meaningfully lower than the before-change measurement
  And the plan records both measurements and the environment assumptions used to collect them

## Executable Tests

The first proof boundary is `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`. Treat that file as authoritative for direct scalar behavior. Do not replace those tests with assertions against internal `Ecto.Query` fields. Extend the file only if the refactor introduces a behavior distinction that is not already covered there.

The second proof boundary is `test/ecto_shorts/dynamics/postgres_test.exs`. Use it to verify that `EctoShorts.Adapters.Postgres.build_dynamic/4` still routes nested scalar wrappers and quantified payloads correctly through the scalar-expression path after any dispatch refactor.

The third proof boundary is `test/ecto_shorts/common_filters_scalar_filter_test.exs`. Use it to prove unchanged higher-level scalar query behavior through `EctoShorts.CommonFilters.convert_params_to_filter/3`. If the scalar refactor affects only code generation and not high-level behavior, focused test selection within that file is acceptable as long as the chosen tests cover the operator families touched by the refactor.

Whenever a test compares SQL or dynamic expressions, use `assert_dynamic/2`, `assert_sql/3`, or `assert_query/2` from `EctoShorts.Testing`. Never assert against internal `Ecto.Query` struct fields such as `hd(actual.wheres).expr`.

## Concrete Steps

Run commands from `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

Capture the baseline compile measurement first.

    mix compile

Record the elapsed wall-clock time and the size of `priv/generated/dynamics/postgres/scalar_expr_builder/comparison.ex` after the compile.

Run the direct scalar-expression tests before and after each small refactor batch.

    mix test test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs

Run the adapter dispatch tests after any change to `lib/ecto_shorts/dynamics/postgres.ex` or `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`.

    mix test test/ecto_shorts/dynamics/postgres_test.exs

Run the scalar-filter integration tests after any change that could alter higher-level query semantics.

    mix test test/ecto_shorts/common_filters_scalar_filter_test.exs

If the compile measurement command changes during implementation, replace the command in this section and explain the reason in the `Decision Log`.

## Validation and Acceptance

Acceptance is satisfied only when three things are true at the same time. First, the documented compile measurement shows a meaningful improvement for the scalar generation path. Second, `mix test test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` passes and proves that direct scalar behavior is unchanged. Third, the neighboring adapter and scalar-filter tests that cover the touched behavior pass as well.

The human-readable evidence for success should include the before-and-after compile measurements, the before-and-after generated comparison file size if it changed materially, and short notes confirming that the direct scalar tests and relevant integration tests passed. The user-visible meaning of “faster” must be backed by a recorded measurement, not intuition.

## Idempotence and Recovery

This plan is intended to be executed in small reversible batches. The baseline measurement command can be rerun safely. The focused test commands can be rerun safely. When refactoring `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex`, keep each batch small enough that a failed attempt can be reverted or adjusted without losing track of the working state.

If a refactor batch creates ambiguous failures, stop and determine whether the code or the test expectation is wrong before continuing. If the failure does not prove which side is wrong, resolve the ambiguity before changing either. Record the resolution in the `Decision Log` and, if it materially changes the understanding of the task, update the relevant specification sections in this plan before proceeding.

## Artifacts and Notes

The most important artifact today is the current generated comparison file.

    priv/generated/dynamics/postgres/scalar_expr_builder/comparison.ex
    size: approximately 4,024,318 bytes
    relative significance: far larger than the other scalar generated modules

The current scalar public routing path is:

    lib/ecto_shorts/dynamics/postgres.ex
      -> lib/ecto_shorts/dynamics/postgres/scalar_expr.ex
      -> generated modules under priv/generated/dynamics/postgres/scalar_expr_builder/

The current focused proof files are:

    test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs
    test/ecto_shorts/dynamics/postgres_test.exs
    test/ecto_shorts/common_filters_scalar_filter_test.exs

## Interfaces and Dependencies

The public interface that must remain stable is `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5`. It currently accepts a selected binding such as `{:as, alias}` or `{:at, position}`, a field key atom, a negation marker of `:not` or `nil`, a scalar term, and options. It normalizes the term and delegates to generated modules that each expose `dynamic_expr/4`.

The surrounding public interfaces that must continue to observe unchanged behavior are `EctoShorts.Adapters.Postgres.build_dynamic/4` in `lib/ecto_shorts/dynamics/postgres.ex` and `EctoShorts.CommonFilters.convert_params_to_filter/3` in the CommonFilters path exercised by `test/ecto_shorts/common_filters_scalar_filter_test.exs`.

The internal collaborators that may change are `EctoShorts.Dynamics.Postgres.ScalarExprBuilder`, `EctoShorts.Generator`, `EctoShorts.Generator.Builder`, `EctoShorts.Compiler`, and `EctoShorts.Dynamics.Helpers`. Any new helper functions introduced by the refactor should prefer explicit arguments over hidden compile-time state. If literals such as operator sets or datetime intervals are needed in generated code, expand them explicitly into generated output rather than assuming builder module attributes will be available inside compiled generated modules.

Revision note: On 2026-03-13, Cascade rewrote the earlier planning note into a full `ExecPlan` because `.agent/RULES.md` now requires an `ExecPlan` for planning and refactoring behavior-bearing code, and the previous file did not satisfy the required living-document structure from `.agent/PLANS.md`.
