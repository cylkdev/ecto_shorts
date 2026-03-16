# Document `EctoShorts.DynamicExpressions.Postgres`

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the task it describes. Maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

`EctoShorts.DynamicExpressions.Postgres` is listed as public documentation surface in `mix.exs`, but the module currently has no `@moduledoc` and its public `build_dynamic/4` entry point has no `@doc` or `@spec`. After this change, a caller reading generated docs or in-editor docs will be able to understand when to use this module directly, when to prefer `EctoShorts.DynamicExpressions`, which binding selectors and filter-entry shapes `build_dynamic/4` accepts, what it returns, and which behavior is a precondition rather than a documented failure case.

The result should be visible by running `mix docs` from the repository root and opening the generated documentation for `EctoShorts.DynamicExpressions.Postgres`, or by reading the module in an editor that shows Elixir docs. No runtime behavior should change.

## In Scope

1. Add a complete `@moduledoc` to `lib/ecto_shorts/dynamics/postgres.ex` that explains the responsibility and boundary of `EctoShorts.DynamicExpressions.Postgres`, the single public entry point `build_dynamic/4`, the shared binding-selector contract, the accepted high-level filter-entry families, and the relationship to `EctoShorts.DynamicExpressions` plus the Postgres-specific expression builders.
2. Add public function documentation for `build_dynamic/4`, including its default `opts \\ []`, accepted argument families, return shape, preconditions, and examples.
3. Add a public `@spec` for `build_dynamic/4` if needed to satisfy the function specification standard for this repo.
4. Keep the documentation behavior-preserving: do not change runtime logic, private helpers, routing, or operator semantics while adding the docs.
5. Validate that the edited file compiles and that generated docs succeed without introducing warnings or test regressions.

## Out of Scope

- Any runtime behavior change in `lib/ecto_shorts/dynamics/postgres.ex` or its helper modules.
- Renaming modules, moving files, or reorganizing the dynamics tree.
- Correcting adjacent contract drift in `lib/ecto_shorts/adapter/dynamic.ex` or `lib/ecto_shorts/dynamics.ex` unless a doc edit there becomes strictly necessary for consistency and receives explicit approval.
- Adding or changing ExUnit tests unless the documentation change introduces a compile or doctest issue that must be repaired.
- Documenting private helpers in `EctoShorts.DynamicExpressions.Postgres` individually.

## Progress

- [x] (2026-03-15 21:38Z) Re-read `.agent/RULES.md`, `.agent/PLANS.md`, and the module/function specification guides before planning.
- [x] (2026-03-15 21:40Z) Confirmed there is no existing governing `ExecPlan` for this specific documentation task.
- [x] (2026-03-15 21:44Z) Established the public-boundary context from `lib/ecto_shorts/adapter/dynamic.ex`, `lib/ecto_shorts/dynamics.ex`, and `lib/ecto_shorts/dynamics/postgres.ex`.
- [x] (2026-03-15 21:47Z) Wrote the governing `ExecPlan` with the required module and function specifications folded into this tracked artifact.
- [x] (2026-03-15 21:58Z) Added `@moduledoc` and `@doc`/`@spec` to `lib/ecto_shorts/dynamics/postgres.ex` in line with the specifications below.
- [x] (2026-03-15 21:59Z) Ran `mix test` and `mix docs`, then adjusted the new docs to avoid introducing new hidden-module reference warnings from `EctoShorts.DynamicExpressions.Postgres`.
- [x] (2026-03-15 22:00Z) Re-ran `mix test` and `mix docs` and recorded the final validation outcomes in this plan.

## Milestones

### Milestone 1 - Settle the public documentation contract

Capture the module-level and function-level contract for `EctoShorts.DynamicExpressions.Postgres` before touching the source file. The end state of this milestone is a complete description of what the module is for, when callers should use it, which public entry point matters, what `build_dynamic/4` accepts, what it returns, and which shapes are outside the documented contract.

Acceptance for this milestone is this ExecPlan itself: the `Module Specification`, `Function Specification`, `Example Mappings`, and boundary sections below must be specific enough that a new implementer could add the docs without reverse-engineering the code again.

### Milestone 2 - Apply the documentation to the source file

Edit `lib/ecto_shorts/dynamics/postgres.ex` to add the agreed `@moduledoc` and `build_dynamic/4` documentation. Keep the edit narrow. The runtime clauses, guards, helper ownership, and operator routing must remain byte-for-byte behavior-compatible unless a compile fix is required for the docs themselves.

Acceptance for this milestone is that `EctoShorts.DynamicExpressions.Postgres` exposes clear generated docs, `build_dynamic/4` has a public `@spec` and `@doc`, and no runtime behavior has changed.

### Milestone 3 - Prove the documentation integrates cleanly

Run the narrowest useful verification commands from the repo root. `mix test` should remain green. `mix docs` should generate the documentation successfully so the new module/function docs appear in the public docs set already declared in `mix.exs`.

Acceptance for this milestone is recorded command output showing successful verification and an updated `Outcomes & Retrospective` section.

## Surprises & Discoveries

- Observation: Tracked documentation examples under `/doc` are not available as authoritative repo artifacts because `/doc/` is gitignored in this repository.
  Evidence: `.gitignore` ignores `/doc/`, and direct reads of `doc/*.md` are prohibited by the workspace tooling.

- Observation: The live binding-selector contract for `EctoShorts.DynamicExpressions.Postgres` is broader than the current `EctoShorts.Adapter.DynamicExpression.selected_binding` type.
  Evidence: `lib/ecto_shorts/dynamics/postgres.ex` accepts `{:as, nil}`, `{:as, atom()}`, and `{:at, position}` through `binding_selector?/1`, while `lib/ecto_shorts/dynamics.ex` already documents `{:as, nil}` as the default binding selector.

- Observation: Naming `Ecto.Query.DynamicExpr` directly in prose produces ExDoc hidden-module warnings in this repo's current documentation configuration.
  Evidence: The first `mix docs` run warned on `lib/ecto_shorts/dynamics/postgres.ex:62` and `:93`. Rewording the new docs to say \"dynamic expression\" removed the warnings from `EctoShorts.DynamicExpressions.Postgres` while `mix docs` continued to succeed.

- Observation: `mix docs` still emits pre-existing hidden-module warnings outside this task's scope.
  Evidence: The final `mix docs` run still warned about `Ecto.Query.DynamicExpr` references in `lib/ecto_shorts/dynamics.ex` and `lib/ecto_shorts/adapter/dynamic.ex`, and `EctoShorts.CommonFilters.API` references in `lib/ecto_shorts/common_filters.ex`.

## Decision Log

- Decision: Use this governing `ExecPlan` as the tracked artifact for the required module and function specifications instead of creating separate spec files.
  Rationale: The repo rules require one governing `ExecPlan`, and the tracked `doc/` area is gitignored. Folding the module/function specification into the governing plan keeps the contract in a tracked, authoritative artifact without creating competing planning documents.
  Date/Author: 2026-03-15 / Cascade

- Decision: Document the live `{:as, nil}` binding-selector shape in the `EctoShorts.DynamicExpressions.Postgres` docs.
  Rationale: The module itself accepts that selector today, and `EctoShorts.DynamicExpressions` already presents it as the default-binding form. The docs for this module should reflect the live owner code rather than the narrower older type alias in the behaviour module.
  Date/Author: 2026-03-15 / Cascade

- Decision: Treat invalid `selected_binding` and invalid filter-entry shapes as preconditions rather than defining new failure behavior in the docs.
  Rationale: The user asked for documentation, not a behavior change. The current public function does not expose a stable error tuple or explicit exception contract for invalid shapes, so the docs should describe the valid call contract without inventing a stronger failure guarantee.
  Date/Author: 2026-03-15 / Cascade

- Decision: Avoid naming hidden modules directly in the new Postgres adapter docs.
  Rationale: `mix docs` succeeded with warnings when the new prose mentioned `Ecto.Query.DynamicExpr` directly. Rewording the new docs kept the same caller-facing meaning while avoiding new warnings introduced by this task.
  Date/Author: 2026-03-15 / Cascade

## Outcomes & Retrospective

Completed. `lib/ecto_shorts/dynamics/postgres.ex` now has a module doc plus `build_dynamic/4` documentation and a public `@spec`. The new docs describe when to call the Postgres adapter directly, the accepted binding-selector families, the two documented entry families, the default `opts`, and the valid-call precondition model without changing runtime behavior.

Validation passed. `mix test` finished with `11 doctests, 1281 tests, 0 failures`. `mix docs` generated the docs successfully. During validation, the first version of the new prose introduced hidden-module reference warnings for `Ecto.Query.DynamicExpr` from `EctoShorts.DynamicExpressions.Postgres`; a follow-up wording adjustment removed those new warnings. Remaining hidden-module warnings in other files were already present elsewhere in the repo and remain out of scope for this task.

## Context and Orientation

The relevant public modules are:

- `lib/ecto_shorts/dynamics.ex` - `EctoShorts.DynamicExpressions`, the adapter-agnostic public entry point that resolves the active dynamic adapter.
- `lib/ecto_shorts/adapter/dynamic.ex` - `EctoShorts.Adapter.DynamicExpression`, the behaviour implemented by dynamic adapters.
- `lib/ecto_shorts/dynamics/postgres.ex` - `EctoShorts.DynamicExpressions.Postgres`, the concrete Postgres adapter that translates one filter entry into an `Ecto.Query.DynamicExpr`.

`EctoShorts.DynamicExpressions.Postgres` is public enough to appear in the ExDoc grouping in `mix.exs`, but it is not the usual first module a caller should reach for. A caller that wants adapter-independent dynamic building should start at `EctoShorts.DynamicExpressions.build_dynamic/4`. A caller should use `EctoShorts.DynamicExpressions.Postgres.build_dynamic/4` directly only when they intentionally want the Postgres implementation or are working on adapter-facing integration.

The source file currently contains one public function and several private helpers. The documentation task only touches the public module boundary and the public `build_dynamic/4` boundary. The helpers matter only because they establish what the public module owns: top-level parameter normalization, routing across common operators versus array-like fields versus scalar fields, and the limited quantified-query rewrite for `:all` and `:any` payloads.

## Module Specification

`EctoShorts.DynamicExpressions.Postgres` builds Postgres-specific `Ecto.Query.DynamicExpr` values for one filter entry at a selected query binding.

Use this module when the caller already knows they want the Postgres adapter. Do not use it when the caller wants adapter resolution or adapter-agnostic code; use `EctoShorts.DynamicExpressions.build_dynamic/4` for that.

Start with `build_dynamic/4`. This is the module's only public entry point.

The `@moduledoc` added to the file must communicate these shared rules:

1. The module is the concrete Postgres implementation of the `EctoShorts.Adapter.DynamicExpression` contract.
2. The module accepts valid binding selectors in the live shapes `{:as, nil}`, `{:as, atom()}`, and `{:at, pos_integer()}`.
3. The module accepts two high-level filter-entry families:
   - ordinary entries shaped like `{field_or_operator, term}`
   - top-level quantified groups shaped like `{:all, params}` and `{:any, params}`
4. Successful calls return an `Ecto.Query.DynamicExpr` suitable for Ecto query macros such as `where`, `or_where`, and `having`.
5. The module owns Postgres-specific translation of scalar comparisons, array/map-backed field predicates, common operators, negation handling, and quantified subquery rewriting, but callers should not rely on the current private helper layout.
6. Invalid binding selectors and unsupported filter-entry shapes are outside the documented call contract. The docs must not invent new error tuples or promise a raised exception shape.
7. The examples should show at least one ordinary field example and one top-level quantified example, even if the examples use illustrative placeholder modules such as `Post` and `MyApp.Repo`.

The module docs should also explain the handoff to nearby public modules in caller terms: `EctoShorts.DynamicExpressions` chooses the adapter, while `EctoShorts.DynamicExpressions.Postgres` performs the Postgres-specific translation once the adapter has already been chosen.

## Function Specification

### `EctoShorts.DynamicExpressions.Postgres.build_dynamic/4`

Caller-facing purpose: build a Postgres `Ecto.Query.DynamicExpr` for a single filter entry against one selected query binding.

The function specification in the source file must state all of the following:

1. The function accepts four arguments:
   - `source` - the schema module, queryable source, or query value used for field reflection and quantified-query construction.
   - `selected_binding` - one of `{:as, nil}`, `{:as, atom()}`, or `{:at, pos_integer()}`.
   - `args` - a filter entry. The public documented families are `{field_or_operator, term}` and top-level quantifier groups `{:all, params}` or `{:any, params}`.
   - `opts` - keyword options, defaulting to `[]`, forwarded through the dynamic-building pipeline.
2. The function returns an `Ecto.Query.DynamicExpr` on successful calls.
3. The docs must describe the top-level quantifier behavior in caller terms: when the entry itself is `{:all, params}` or `{:any, params}`, the module merges the normalized child expressions with that quantifier and returns the resulting dynamic expression.
4. The docs must describe ordinary entry behavior in caller terms: when the entry is `{key, params}`, the module normalizes nested forms and builds the corresponding Postgres dynamic expression for the selected binding.
5. The docs must not claim stable failure behavior for invalid binding selectors, invalid entry shapes, or unsupported operators. Those are preconditions unless the existing implementation already documents something stronger elsewhere, which it does not today.
6. The docs must include examples. At minimum include:
   - one scalar or comparison example
   - one multi-predicate example using keyword or map operator syntax
   - one top-level quantifier example

The `@spec` should be strong enough to expose the public call shapes without pretending to enumerate every internal term form. A suitable shape is an open `term()`-based input with a precise binding-selector union, defaulted `keyword()` opts, and `%Ecto.Query.DynamicExpr{}` return.

## Internal Boundary Contracts

This task does not change internal behavior, but the documentation depends on understanding which responsibilities belong to the public module boundary versus its private helpers.

Upstream caller: `EctoShorts.DynamicExpressions.build_dynamic/4` or direct callers inside the filter pipeline.

Public boundary: `EctoShorts.DynamicExpressions.Postgres.build_dynamic/4`.

Accepted input at that public boundary:

- `source`: queryable or schema value that can be used by reflection and expression builders.
- `selected_binding`: `{:as, nil}` | `{:as, atom()}` | `{:at, pos_integer()}`.
- `args`: either `{:all, params}`, `{:any, params}`, or `{key, params}`.
- `opts`: keyword list.

Produced output at that public boundary:

- an `Ecto.Query.DynamicExpr` when the call is within the valid documented contract.

Internal ownership that the docs may summarize but must not expose as contract:

- `Normalizer.normalize_params/3` expands the incoming payload into entries ready for reduction.
- `expr_entry/2` determines whether a nested entry carries `:and` or `:or` merge intent.
- `build_expr/5` decides which Postgres expression family owns the final translation.
- `normalize_negation_term/1` and `normalize_quantified_term/3` rewrite supported wrapper forms before the final expression builder is called.
- `array_field?/2` and `binding_selector?/1` are private classification helpers, not public API.

Forbidden promotion to public contract:

- The exact helper names.
- The exact intermediate normalized list shape produced by `Normalizer`.
- The exact order of private helper calls beyond the stable caller-visible promise that one filter entry becomes one returned dynamic expression.

## Internal Structure Walkthrough

The main success path starts at `build_dynamic/4` in `lib/ecto_shorts/dynamics/postgres.ex`.

When `args` is `{:all, params}` or `{:any, params}`, the function normalizes `params`, converts each normalized entry into a dynamic child expression through `apply_expr/4`, merges those children with the top-level quantifier, and then wraps the result in an outer `:and` merge so the return shape is a standard dynamic expression value.

When `args` is `{key, params}`, the function also normalizes `params`, derives per-entry merge operators through `expr_entry/2`, converts each entry through `apply_expr/4`, and merges the child expressions into the returned dynamic expression.

`apply_expr/4` owns only one public-facing idea: nested map and keyword-list operator forms are flattened into child entries before final expression generation. It does not expose that flattening as a separate public API.

`build_expr/5` owns the last routing step. After negation normalization and quantified-query normalization, it routes common operators to `CommonExpr`, array-like fields to `ArrayExpr`, and everything else to `ScalarExpr`. The docs for `EctoShorts.DynamicExpressions.Postgres` may mention those three expression families as nearby modules, but they should not describe the exact branching logic as something callers may depend on.

## Example Mappings

### Story: Direct Postgres dynamic generation

A caller who intentionally chooses the Postgres adapter can build one `Ecto.Query.DynamicExpr` directly from a selected binding and a filter entry.

#### Rules:

- Valid binding selectors are `{:as, nil}`, `{:as, atom()}`, and `{:at, pos_integer()}`.
- Ordinary entries shaped as `{key, term}` produce a Postgres dynamic expression for that field or operator.
- Top-level quantified entries shaped as `{:all, params}` or `{:any, params}` produce a dynamic expression that merges the normalized child predicates with that quantifier.
- `opts` defaults to `[]` when omitted.
- Invalid binding selectors and unsupported entry shapes are outside the documented guarantee; the docs should present them as preconditions rather than promising a particular error.

#### Examples:

`EctoShorts.DynamicExpressions.Postgres.build_dynamic(Post, {:as, nil}, {:views, 5})`
`#Ecto.Query.DynamicExpr<...>`

`EctoShorts.DynamicExpressions.Postgres.build_dynamic(Post, {:as, nil}, {:views, [>: 1, <: 10]})`
`#Ecto.Query.DynamicExpr<...>`

`EctoShorts.DynamicExpressions.Postgres.build_dynamic(Post, {:as, nil}, {:all, [published: true, archived: false]})`
`#Ecto.Query.DynamicExpr<...>`

`EctoShorts.DynamicExpressions.Postgres.build_dynamic(Post, {:as, nil}, {:views, 5}, [])`
`#Ecto.Query.DynamicExpr<...>`

#### Open Questions:

- **Q:** Should the new docs promise behavior for invalid `selected_binding` values or malformed entry shapes? **A:** No. This task documents the valid contract only; no stable failure contract is defined today.
- **Q:** Should the new docs enumerate every forwarded option accepted by lower-level builders? **A:** No. Document `opts` as forwarded keyword options and keep the examples focused on the common direct-call shape.

## Behaviour Specifications

### Feature: Generated docs describe the Postgres dynamic adapter clearly

Scenario: Reading the module documentation
  Given a reader opens the generated docs for `EctoShorts.DynamicExpressions.Postgres`
  When they read the module page
  Then they can tell that the module is the concrete Postgres implementation of `EctoShorts.Adapter.DynamicExpression`
  And they can tell that `EctoShorts.DynamicExpressions` is the adapter-agnostic entry point
  And they can identify `build_dynamic/4` as the public entry point to start with

Scenario: Reading the function documentation
  Given a reader opens the documentation for `EctoShorts.DynamicExpressions.Postgres.build_dynamic/4`
  When they read the function docs
  Then they can see the accepted binding-selector families
  And they can see the documented ordinary-entry and top-level-quantifier entry families
  And they can see that the return value is an `Ecto.Query.DynamicExpr`
  And they can see that `opts` defaults to `[]`

Scenario: Reading invalid-input guidance
  Given a reader inspects the same function docs
  When they look for invalid-input behavior
  Then they do not see invented error tuples or exception guarantees that the current implementation does not define
  And they do see that invalid call shapes are outside the documented contract

## Executable Tests

This documentation task does not require new ExUnit assertions if the runtime behavior remains unchanged. The proof obligation is:

1. The code compiles with the new docs and any added `@spec`.
2. Existing tests continue to pass.
3. `mix docs` succeeds and includes `EctoShorts.DynamicExpressions.Postgres` in the generated documentation set.

If an added doctest-style example would require a real runnable example, prefer a non-doctest illustrative example in the docs rather than adding new test fixtures or broadening the scope.

## Plan of Work

First, edit `lib/ecto_shorts/dynamics/postgres.ex` at the module header to add `@moduledoc`. The module docs should begin with one sentence naming the module as the Postgres dynamic adapter, then explain when to use it directly and when to use `EctoShorts.DynamicExpressions` instead. Include the binding-selector contract, the two high-level entry families, the return value, and a short example block.

Second, add a public `@spec` and `@doc` for `build_dynamic/4` immediately above the function head. The function docs should describe the four arguments, the default `opts`, the successful return value, the valid input families, and the precondition model for invalid input. Keep the examples concrete but not over-specific about internal helper behavior.

Third, leave private helpers untouched. Do not add private helper docs, comments, or refactors while performing this task.

Fourth, run the validation commands listed below and record the outcomes back into this plan.

## Concrete Steps

From the repository root `/Users/kurthogarth/Documents/GitHub/ecto_shorts`:

1. Edit `lib/ecto_shorts/dynamics/postgres.ex` to add the planned docs and spec.
2. Run `mix test`.
3. Run `mix docs`.
4. Update this ExecPlan's `Progress`, `Surprises & Discoveries`, and `Outcomes & Retrospective` sections with the results.

Expected proof snippets after implementation:

    $ mix test
    ...
    0 failures

    $ mix docs
    ...
    Generated docs/

Observed proof snippets:

    $ mix test
    Finished in 1.9 seconds (1.9s async, 0.00s sync)
    11 doctests, 1281 tests, 0 failures

    $ mix docs
    Generating docs...
    View html docs at "doc/index.html"

## Validation and Acceptance

The change is complete when all of the following are true:

- `lib/ecto_shorts/dynamics/postgres.ex` has a module doc that a beginner can use to understand the module boundary without opening private helpers.
- `build_dynamic/4` has a public doc and `@spec` that state the accepted binding-selector families, the documented input families, the default `opts`, and the return value.
- The docs do not promise failure behavior that the current implementation does not stably define.
- `mix test` passes from the repo root.
- `mix docs` succeeds and includes `EctoShorts.DynamicExpressions.Postgres` in the generated docs set already declared in `mix.exs`.
- The task does not introduce new documentation warnings from `lib/ecto_shorts/dynamics/postgres.ex`.

## Idempotence and Recovery

Re-applying the same documentation text should be harmless. If `mix docs` fails because the new docs include invalid Markdown or typespec syntax, fix only the documentation text or spec shape and rerun the command. If `mix test` fails after the doc edit, treat that as evidence that the documentation change accidentally altered code shape and revert the runtime portion of the edit before proceeding.

## Artifacts and Notes

Authoritative evidence used for this plan:

- `lib/ecto_shorts/dynamics/postgres.ex`
- `lib/ecto_shorts/dynamics.ex`
- `lib/ecto_shorts/adapter/dynamic.ex`
- `mix.exs`
- `.agent/RULES.md`
- `.agent/PLANS.md`
- `.agent/guides/MODULE_SPECIFICATIONS.md`
- `.agent/guides/FUNCTION_SPECIFICATIONS.md`

## Interfaces and Dependencies

The edited public module is `EctoShorts.DynamicExpressions.Postgres` in `lib/ecto_shorts/dynamics/postgres.ex`.

The function to document is `EctoShorts.DynamicExpressions.Postgres.build_dynamic/4` with the existing runtime signature `build_dynamic(source, selected_binding, args, opts \\ [])`.

Relevant adjacent interfaces that shape the docs but are not themselves in scope for editing are:

- `EctoShorts.DynamicExpressions.build_dynamic/4` in `lib/ecto_shorts/dynamics.ex`
- `EctoShorts.Adapter.DynamicExpression` in `lib/ecto_shorts/adapter/dynamic.ex`
- `%Ecto.Query.DynamicExpr{}` from Ecto

Plan revision note: Initial governing ExecPlan created on 2026-03-15 to support a documentation-only task for `EctoShorts.DynamicExpressions.Postgres`, with the required module/function specifications embedded because the repo-tracked `doc/` directory is gitignored.

Plan revision note: Updated on 2026-03-15 after implementation and verification to record the completed doc edit, the `mix test` / `mix docs` results, and the follow-up wording fix that removed new hidden-module warnings introduced by the first draft of the new docs.
