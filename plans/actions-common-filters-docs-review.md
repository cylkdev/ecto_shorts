# Review and expand `EctoShorts.Actions` and `EctoShorts.CommonFilters` public documentation

This ExecPlan reviews `lib/ecto_shorts/actions.ex` and `lib/ecto_shorts/common_filters.ex` against the live code and test suites, then updates module docs, function docs, public types, and examples so a caller can understand the supported API surface without reading the implementation.

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the task it describes. If the task later narrows to a specific boundary, test, or contract question, that later reasoning must still be recorded here unless the task is explicitly split into a separate ExecPlan.

This plan must be maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

After this work, a caller reading `EctoShorts.Actions` or `EctoShorts.CommonFilters` should be able to choose the right public entry point, understand the accepted inputs and return shapes, see the important shared rules and options, and find examples that match live supported behavior. The outcome is documentation clarity, not runtime change: the public docs should describe what the library already does today, with live tests treated as the primary evidence for that contract.

## In Scope

This plan covers a documentation-focused review of `lib/ecto_shorts/actions.ex` and `lib/ecto_shorts/common_filters.ex` only.

It includes a full audit of each module’s `@moduledoc`, public types, public `@spec`s as they relate to caller-visible shapes, and each public function’s `@doc` content.

It includes adding or refining examples where the current docs are too abstract and where the examples can be grounded in live tests.

It includes reconciling the docs with the live proof surface under `test/ecto_shorts/actions/` and `test/ecto_shorts/common_filters/`.

It includes documenting shared module-level rules once at the right boundary instead of repeating them inconsistently across function docs.

## Out of Scope

This plan does not authorize runtime behavior changes, query semantics changes, or refactors in the implementation bodies.

This plan does not authorize adding new filters, new action helpers, or new test-only behavior.

This plan does not expand into a repo-wide documentation sweep outside `lib/ecto_shorts/actions.ex` and `lib/ecto_shorts/common_filters.ex`.

This plan does not add or revise documentation for private helpers or internal builder modules unless a brief public-boundary orientation sentence is needed to explain where responsibility lives.

## Progress

- [x] (2026-03-15 21:37Z) Loaded `.agent/RULES.md`, `.agent/PLANS.md`, `.agent/guides/MODULE_SPECIFICATIONS.md`, `.agent/guides/FUNCTION_SPECIFICATIONS.md`, and `.agent/guides/TESTING_PRINCIPLES.md` to establish the planning and documentation standard for this task.
- [x] (2026-03-15 21:41Z) Surveyed `lib/ecto_shorts/actions.ex` and `lib/ecto_shorts/common_filters.ex` to inventory their public entry points, shared options, and current documentation shape.
- [x] (2026-03-15 21:46Z) Surveyed the primary proof surface in `test/ecto_shorts/actions/` and `test/ecto_shorts/common_filters/` to identify the live supported behavior families the docs must cover.
- [x] (2026-03-15 21:58Z) Synchronized the approved draft ExecPlan into the repo `plans/` directory so implementation can proceed under a repo-root governing artifact.
- [x] (2026-03-15 22:16Z) Revised `lib/ecto_shorts/actions.ex` module docs, public typedocs, and the overloaded function docs that needed clearer caller-facing contracts.
- [x] (2026-03-15 22:22Z) Revised `lib/ecto_shorts/common_filters.ex` module docs and public function docs so the filter-language surface is explained at the behavior-family level with live routing examples.
- [x] (2026-03-15 22:27Z) Ran `mix test test/ecto_shorts/actions test/ecto_shorts/common_filters`; the targeted suites passed with 690 tests and 0 failures.
- [x] (2026-03-15 22:31Z) Read `https://hexdocs.pm/ecto/Ecto.html` to capture the reference writing style, then started rewriting the public docs in `actions.ex` and `common_filters.ex` to match that tone while preserving the live contract.
- [x] (2026-03-15 22:36Z) Completed the Ecto-style rewrite pass across the public docs in `lib/ecto_shorts/actions.ex` and `lib/ecto_shorts/common_filters.ex`, while restoring the prose `Filter Keys` section in `common_filters.ex` for readability.
- [x] (2026-03-15 22:38Z) Reran `mix test test/ecto_shorts/actions test/ecto_shorts/common_filters` after the Ecto-style pass; the targeted suites again passed with 690 tests and 0 failures.

## Milestones

### Milestone 1: Lock the live public contract to test-backed evidence

The first milestone is a documentation audit, not a code change. The goal is to produce a behavior inventory for both modules that answers two questions before any doc text is edited: what each public boundary already promises today, and which of those promises are already visible in the docs versus only visible in the tests. Completion means the plan can name the main caller-facing behavior families for `EctoShorts.Actions` and `EctoShorts.CommonFilters` and can trace each family to concrete test files.

The proof sources for `EctoShorts.Actions` are the existing action test modules: `test/ecto_shorts/actions/actions_crud_test.exs`, `actions_bulk_test.exs`, `actions_batch_test.exs`, `actions_multi_test.exs`, and `actions_transaction_test.exs`. The proof sources for `EctoShorts.CommonFilters` are the common-filter test modules covering routing, joins, ordering, selection, preloads, locking, set operations, CTEs, windows, and expression/operator families.

### Milestone 2: Make `EctoShorts.Actions` self-explanatory at the public boundary

The second milestone updates only `lib/ecto_shorts/actions.ex`. The goal is that a beginner can understand the role of the module, the difference between CRUD, Bulk, Multi, Batch, and Transaction families, the shared option model, the important error and return shapes, and the accepted call shapes for every public function without needing to open the helper modules it delegates to.

Completion means the module-level docs clearly name where `EctoShorts.Actions` owns behavior and where it delegates to adjacent modules, the public types used in specs are documented clearly enough to help a caller, and each public function doc explains its accepted inputs, options, default behavior, return shapes, and important edge or failure behavior. The updated examples should be drawn from live, deterministic behaviors already exercised by tests.

### Milestone 3: Make `EctoShorts.CommonFilters` explain the full live filter language at a usable level

The third milestone updates only `lib/ecto_shorts/common_filters.ex`. The goal is that a caller who has a schema or queryable and a map or keyword list of filter params can understand how to use `convert_params_to_filter/3`, when `build_query/6` matters, what the top-level routing keys mean, how binding selectors work, what evaluation order is guaranteed, and which broad families of filter terms are supported today.

Completion means the module docs no longer stop at the current top-level routing explanation; they also orient the caller to the deeper, test-backed filter families such as comparison operators, aggregate operators, arithmetic expressions, string matching and transformations, negation, selection and selection merging, joins, preloads, cardinality and pagination, locking, set operations, subqueries, CTEs, windows, and named-binding support. The docs do not need to restate every internal builder implementation, but they must not hide major supported public shapes that a caller would otherwise only discover by reading tests.

## Surprises & Discoveries

- Observation: `EctoShorts.Actions` already has a large `@moduledoc`, but a large doc is not the same as a complete contract. Several public entry points still need to be checked against live overloaded call shapes and option behavior rather than assumed to be covered by the family-level overview.
  Evidence: `actions_crud_test.exs` exercises `all/1-3`, keyword-vs-map behavior in `all/2`, `:preload` behavior across multiple functions, binding-selector-driven reads, and optimistic locking behavior that deserves explicit function-level documentation.

- Observation: `EctoShorts.CommonFilters` currently explains top-level routing well, but the live test surface is much broader than the current module-level filter key reference.
  Evidence: dedicated test files exist for comparison operators, aggregate operators, arithmetic, string matching, string transformations, negation, join variants, selection variants, locking, set operations, recursive CTEs, with-CTE support, windows, and named-binding behavior.

- Observation: binding-selector support is a first-class public shape in both modules and should be documented in that exact public form.
  Evidence: `actions_crud_test.exs`, `common_filters_select_test.exs`, `common_filters_select_extended_test.exs`, and `common_filters_order_modifier_test.exs` exercise top-level `%{as: %{binding_name => %{...}}}` and `%{at: %{position => %{...}}}` shapes, including `:first` and `:last` aliases inside `at:`.

- Observation: `EctoShorts.Actions` exposes several important function overloads that must be explained where callers will see them, not left to spec reading alone.
  Evidence: `all/2` distinguishes map input from keyword input, `delete/1-3` accepts structs, changesets, lists, and ids, `update/4` accepts either a record id or a struct, and `transact/2` accepts both `Ecto.Multi` values and functions.

- Observation: at least one bulk-insert lookup option name is not cleanly proven by the current tests.
  Evidence: `EctoShorts.Actions.insert_all/3` still documents and implements a `:batch_find` preprocessing option, while the bulk test coverage currently refers to `batch_preload:` in the call site without proving that the lookup preprocessing actually happened. For the documentation pass, the public docs were kept aligned to the live `Actions.insert_all/3` implementation rather than that weaker test signal.

## Decision Log

- Decision: Treat live code and live tests as the primary authority for this task.
  Rationale: The task is a documentation review of existing public behavior, and the user explicitly asked to use tests as proofs of what is possible. Supporting research artifacts may help cross-check terminology later, but they must not define the contract when the live API and tests already do.
  Date/Author: 2026-03-15 / Cascade

- Decision: Treat this as behavior-preserving documentation work only.
  Rationale: The task is to explain the existing functionality, not to refactor or expand it. If review uncovers a contract conflict between docs, tests, and implementation, that conflict should be surfaced before any runtime change is considered.
  Date/Author: 2026-03-15 / Cascade

- Decision: Document shared family rules at the module level and reserve function docs for function-specific promises.
  Rationale: Both `.agent/guides/MODULE_SPECIFICATIONS.md` and `.agent/guides/FUNCTION_SPECIFICATIONS.md` require keeping module-level rules and per-function contracts separate. Repeating shared rules in every doc block would make drift more likely.
  Date/Author: 2026-03-15 / Cascade

- Decision: Sync the draft plan into repo `plans/` before implementation.
  Rationale: Repo rules require the governing ExecPlan for repo-tracked work to live in `./plans`, while the planning tool only allowed the draft to be authored under `.windsurf/plans`.
  Date/Author: 2026-03-15 / Cascade

## Outcomes & Retrospective

The implementation pass completed as documentation-only work.

`lib/ecto_shorts/actions.ex` now explains the module boundary more directly, documents the shared caller-visible conventions, adds public typedoc guidance for the main spec terms, and sharpens the docs for the overloaded public helpers where the live behavior was easiest to miss from the previous prose alone. A follow-up rewrite then shifted the public prose toward the shorter, reference-style tone used by the upstream `Ecto` docs without dropping the caller-visible contract details.

`lib/ecto_shorts/common_filters.ex` now explains the public filter language at the behavior-family level, makes the binding-selector and join-routing shapes clearer, corrects the public lock-key description to match the current live code and tests, and expands the public function docs for `convert_params_to_filter/3` and `build_query/6`. The final style pass kept the prose more compact and `Ecto`-like while restoring the prose `Filter Keys` section in place of the table because that format was easier to read in this module.

The main deferred issue discovered during the review is the `insert_all/3` batch lookup option naming mismatch noted in `Surprises & Discoveries`. That mismatch was recorded but not changed because this task was scoped to behavior-preserving documentation only.

## Context and Orientation

`lib/ecto_shorts/actions.ex` is the main public data-operation boundary of the library. It exposes caller-facing CRUD operations, bulk operations, transactional multi-record operations, batch lookups, and transaction wrappers. It delegates query construction to `EctoShorts.CommonFilters`, changeset creation to schema-oriented helpers such as `EctoShorts.CommonSchema`, and family-specific behavior to adjacent modules such as `EctoShorts.Actions.Bulk`, `EctoShorts.Actions.Multi`, `EctoShorts.Actions.Batch`, and `EctoShorts.Actions.Transaction`. The documentation work must keep that boundary clear: callers should understand what `EctoShorts.Actions` guarantees without having to inspect those helpers.

`lib/ecto_shorts/common_filters.ex` is the main public data-driven query boundary. A caller gives it a schema, queryable, or `{source, schema}` tuple plus filter params, and it returns an `Ecto.Query`. The module also implements the `EctoShorts.Adapter.QueryBuilder` callback boundary so custom builders can delegate back to the default dispatch. The documentation work must therefore explain both the public filter language that ordinary callers use and the narrower `build_query/6` contract that adapter authors can rely on.

For this task, the most authoritative evidence is the live test suite.

For `EctoShorts.Actions`, the key proof files are:

- `test/ecto_shorts/actions/actions_crud_test.exs`
- `test/ecto_shorts/actions/actions_bulk_test.exs`
- `test/ecto_shorts/actions/actions_batch_test.exs`
- `test/ecto_shorts/actions/actions_multi_test.exs`
- `test/ecto_shorts/actions/actions_transaction_test.exs`

For `EctoShorts.CommonFilters`, the proof surface is spread across specialized test files. The important behavior families include:

- routing and composition: `common_filters_association_filter_test.exs`, `common_filters_boolean_composition_test.exs`
- ordering, grouping, and cardinality: `common_filters_order_by_extended_test.exs`, `common_filters_order_modifier_test.exs`, `common_filters_group_by_test.exs`, `common_filters_having_test.exs`, `common_filters_limit_test.exs`, `common_filters_offset_test.exs`, `common_filters_first_test.exs`, `common_filters_last_test.exs`, `common_filters_distinct_test.exs`, `common_filters_exclude_test.exs`
- joins and binding selectors: `common_filters_join_test.exs`, `common_filters_join_extended_test.exs`, `common_filters_with_named_binding_extended_test.exs`, plus binding-selector scenarios in select and order tests
- projection and eager loading: `common_filters_select_test.exs`, `common_filters_select_extended_test.exs`, `common_filters_select_merge_test.exs`, `common_filters_preload_test.exs`, `common_filters_update_test.exs`
- advanced query forms: `common_filters_subquery_test.exs`, `common_filters_set_operation_test.exs`, `common_filters_recursive_ctes_test.exs`, `common_filters_with_cte_extended_test.exs`, `common_filters_windows_test.exs`, `common_filters_with_ties_test.exs`, `common_filters_put_query_prefix_test.exs`, `common_filters_lock_test.exs`
- operator and expression families: `common_filters_comparison_operators_test.exs`, `common_filters_aggregate_operators_test.exs`, `common_filters_arithmetic_extended_test.exs`, `common_filters_string_matching_test.exs`, `common_filters_string_transformations_test.exs`, `common_filters_negation_test.exs`, `common_filters_negation_extended_test.exs`, `common_filters_date_wrappers_test.exs`, `common_filters_datetime_wrappers_test.exs`, `common_filters_datetime_extended_test.exs`, `common_filters_scalar_expr_extended_test.exs`

The documentation review should assume that if a behavior is only visible in tests and not documented in either module, that is a potential documentation gap. It should not assume that every internal builder detail belongs in public docs; the requirement is to explain the caller-visible language and function contracts well enough that the tests stop being the only discoverable source.

## Module Specifications

### `EctoShorts.Actions`

`EctoShorts.Actions` is the caller-facing module for data operations that should feel uniform whether the caller is reading, creating, updating, deleting, batching, or wrapping work in a transaction. A caller should start here when they want a data-driven API over `Ecto.Repo` operations and do not want to build every query or changeset manually.

The module docs should clearly say when to use `EctoShorts.Actions` and when to drop down to adjacent modules instead. The nearby boundaries matter here: `EctoShorts.CommonFilters` owns the data-driven query language, `EctoShorts.CommonSchema` and related helpers own schema and changeset preparation, and the `EctoShorts.Actions.*` submodules own family-specific implementation details. `EctoShorts.Actions` owns the public action interface that ties those pieces together.

The module-level docs should make the shared rules legible: which functions read through `:replica` versus write through `:repo`, how `:preload` is shared across many public functions, how filter-bearing reads use the `EctoShorts.CommonFilters` language, how Bulk differs from Multi, and what the broad return-shape families are.

The public types in `actions.ex` should be reviewed as part of the module specification. If callers are expected to understand `queryable`, `params`, `opts`, `id`, and `cardinality` from specs, then those types need clear public explanation through `@typedoc` or equivalent surrounding doc context.

### `EctoShorts.CommonFilters`

`EctoShorts.CommonFilters` is the caller-facing boundary for turning a data structure into an `Ecto.Query`. A caller should start with `convert_params_to_filter/3` when they want to express query behavior as maps or keyword lists rather than Ecto macros.

The module docs should clearly distinguish the ordinary caller path from the adapter-author path. Ordinary callers need to understand the filter language. Adapter authors and advanced integrations need to understand `build_query/6` and the `:query_builder` option.

The module-level docs should make the shared rules legible: accepted top-level source shapes, map-vs-keyword behavior, duplicate-key preservation through keyword lists, evaluation order, binding selector forms, association shorthand, and the major filter families supported today. The docs do not need to restate each internal builder module, but they do need to stop short of hiding major live capabilities that are only visible in tests today.

The plan should also assess whether `common_filters.ex` needs public type names for source values, filter params, or builder options. If the current specs are readable enough without named types, the work can leave them alone. If the public contract remains hard to read without named type terms, then adding a small number of public type names is in scope.

## Function Specifications

### `EctoShorts.Actions` function families

The doc review for `EctoShorts.Actions` should treat each public function family as a caller-facing contract that must be legible on its own.

For read helpers such as `preload/3`, `exists?/3`, `all/1-3`, `get/3`, `find/3`, `stream/3`, and `aggregate/5`, the docs should state the accepted queryable shapes, how params are interpreted, which options are merged or forwarded, the exact return family, and any important preconditions or edge behavior. In live code and tests, examples that must be explainable include keyword-vs-map behavior in `all/2`, empty-map not-found behavior in `find/3`, transaction-only consumption for `stream/3`, and `:preload` behavior across read functions.

For single-record write helpers such as `create/3`, `update/4`, and `delete/1-3`, the docs should state the accepted input shapes, changeset behavior, return shapes, failure behavior, and shared options. In live code and tests, examples that must be explainable include `update/4` by id versus struct, optimistic locking by schema callback or option, stale-entry error mapping, `delete/1-3` support for changesets and lists, and list deletion stopping on the first failure.

For wrapper helpers such as `find_and_create/4`, `find_and_update/4`, `find_and_upsert/4`, `find_and_delete/3`, and `find_or_create/3`, the docs should explain both branches of the wrapper behavior rather than only the successful branch. In live code and tests, the docs should make clear when an existing record is returned, when a new record is created, how `:preload` is applied, and what errors or changesets can still come back.

For transaction helpers `transaction/2` and `transact/2`, the docs should explain the accepted shapes (`Ecto.Multi` or function), the difference between raw transaction wrapping and normalized transaction behavior, and the effect of `:strict`. The docs should not make a caller infer normalization behavior from the implementation.

For batch helpers `batch/5` and `batch_find/4`, the docs should explain empty-input behavior, cardinality, composite keys, merge behavior, and failure rules such as `ArgumentError` when `:one` cardinality finds multiple records.

For bulk helpers `insert_all/3`, `update_all/4`, and `delete_all/3`, the docs should explain the parts that callers may rely on today: validation behavior, supported update operation shapes, conflict options, batch-find preprocessing, and return shapes.

For multi helpers `create_many/3`, `find_many/3`, `update_many/3`, `delete_many/3`, `find_or_create_many/3`, and `find_and_upsert_many/3`, the docs should explain accepted entry shapes, rollback behavior, result shapes, and any important shape restrictions proven by tests.

### `EctoShorts.CommonFilters` public functions

`convert_params_to_filter/3` is the main public contract. Its docs should explain accepted source shapes, accepted param container shapes, keyword-list duplicate-key behavior, evaluation ordering, top-level routing keys, binding selectors, association shorthand, options such as `:sorter` and `:query_builder`, and the broad families of live supported filter terms. The docs should help a caller find the right input shape without reading the specialized test files.

`build_query/6` is narrower but still public. Its docs should explain that it is the default query-builder callback implementation for `EctoShorts.CommonFilters`, when a custom `:query_builder` module receives control, what happens when the configured module does not export `build_query/6`, and when invalid option shapes raise versus merely log and return the query unchanged.

## Example Mappings

### Story: `EctoShorts.Actions` docs match the live action API

The documentation for `EctoShorts.Actions` should let a caller choose the correct public function and understand the accepted call shape and result without reading helper modules or scanning tests.

#### Rules:

- The module docs must explain the responsibility of the module, its public families, and the shared rules that apply across those families.
- Each public function must document accepted inputs, important options, return shapes, and important edge or failure behavior.
- Shared behaviors such as `:preload`, `:repo`/`:replica`, CommonFilters-backed reads, and Bulk-vs-Multi distinctions should be documented once at the right level and cross-referenced where needed.
- Examples must be grounded in live deterministic behavior already exercised by tests.

#### Examples:

`EctoShorts.Actions.all(Post, [published: true, limit: 10])`
Returns a list of matching `Post` structs, with keyword keys other than recognized runtime options treated as filter params.

`EctoShorts.Actions.find(Post, %{})`
Returns `{:error, %EctoShorts.Actions.ErrorMessage{code: :not_found}}` immediately when called with an empty map and a non-query source.

`EctoShorts.Actions.update(PostWithLock, post, %{title: "Updated"})`
Returns `{:ok, %PostWithLock{}}` on success and may return `{:error, %EctoShorts.Actions.ErrorMessage{code: :stale}}` when optimistic locking detects a concurrent modification.

`EctoShorts.Actions.transact(multi)`
Accepts an `Ecto.Multi` and normalizes the transaction result into `{:ok, value}` or `{:error, reason}`.

#### Open Questions:

- **Q:** Must every public `EctoShorts.Actions` function receive its own explicit example? **A:** The plan assumes every public function needs complete prose, while examples can be denser on representative functions when sibling functions clearly cross-reference shared behavior.
- **Q:** Should docs promise behavior not exercised by live tests in this repo? **A:** No. The review should document only behavior supported by the live code and proof surface.

### Story: `EctoShorts.CommonFilters` docs describe the live filter language at a usable level

The documentation for `EctoShorts.CommonFilters` should let a caller build supported query filters from maps or keyword lists without having to infer the language from implementation details or scattered test modules.

#### Rules:

- The module docs must explain the main public entry point, accepted source and param container shapes, evaluation order, binding selectors, association shorthand, and the major supported filter families.
- The docs must distinguish ordinary caller usage of `convert_params_to_filter/3` from adapter-author usage of `build_query/6`.
- The docs must not omit broad public behavior families that are already supported and exercised by tests.
- The docs should avoid promising internal builder details that callers do not need.

#### Examples:

`EctoShorts.CommonFilters.convert_params_to_filter(Post, %{published: true}, [])`
Returns an `Ecto.Query` that filters posts where `published == true`.

`EctoShorts.CommonFilters.convert_params_to_filter(Post, [where: %{published: true}, or_where: %{title: "Draft"}], [])`
Returns an `Ecto.Query` whose `where` clause is applied before the `or_where` clause.

`EctoShorts.CommonFilters.convert_params_to_filter(source, %{as: %{author: %{select: :first_name}}}, [])`
Supports top-level named-binding selection when `source` already includes a named binding `:author`.

`EctoShorts.CommonFilters.build_query(filter, source, query, selected_binding, term, query_builder: MyBuilder)`
Delegates to `MyBuilder.build_query/6` when the custom module exports that callback.

#### Open Questions:

- **Q:** Should the module docs enumerate every operator token individually? **A:** Only when the token itself is part of the caller-facing language and the module would otherwise remain unclear. The default bias is to document behavior families clearly and use focused examples rather than exhaustively mirror every test.
- **Q:** Should `build_query/6` be documented as a primary entry point for ordinary callers? **A:** No. It should be documented as the callback/delegation boundary for advanced integrations, not the first function a typical caller starts with.

## Executable Tests

This is documentation-only work, so no new runtime behavior tests are planned by default. The existing ExUnit suites are the proof source for the public behavior being documented, and they remain part of validation because doc updates must not misstate the exercised behavior.

For `EctoShorts.Actions`, the review and any later implementation should read the following suites as the contract inventory:

- `mix test test/ecto_shorts/actions/actions_crud_test.exs`
- `mix test test/ecto_shorts/actions/actions_bulk_test.exs`
- `mix test test/ecto_shorts/actions/actions_batch_test.exs`
- `mix test test/ecto_shorts/actions/actions_multi_test.exs`
- `mix test test/ecto_shorts/actions/actions_transaction_test.exs`

For `EctoShorts.CommonFilters`, the review and any later implementation should rely on the targeted common-filter suites that prove the behaviors being documented. The exact command set can be narrowed during implementation, but it must cover the behavior families whose examples are introduced or revised.

If any doctest-style examples are added in a way that becomes executable in this repo, then the validation plan must be updated to run the corresponding doctests explicitly.

## Concrete Steps

From the repository root at `/Users/kurthogarth/Documents/GitHub/ecto_shorts`, implement the work in this order.

First, review `lib/ecto_shorts/actions.ex` from the top down and mark every public function whose docs are missing one of the following: accepted call shapes, important options, return shapes, failure behavior, or a useful example grounded in tests.

Second, revise the `@moduledoc` in `lib/ecto_shorts/actions.ex` so it reflects the true public boundary: responsibility, family grouping, shared option model, shared return-shape model, and shared relationship to `EctoShorts.CommonFilters` and adjacent action helpers. Add or refine public typedocs if the current named types are not readable to a caller from specs alone.

Third, revise the public function docs in `lib/ecto_shorts/actions.ex`, focusing especially on the overloaded and behavior-dense boundaries identified in the tests: `all/1-3`, `find/3`, `update/4`, `delete/1-3`, `transact/2`, `batch/5`, `batch_find/4`, `insert_all/3`, and the Multi-family helpers.

Fourth, review `lib/ecto_shorts/common_filters.ex` from the top down and mark every caller-visible behavior family that is proven by tests but not made discoverable by the current docs.

Fifth, revise the `@moduledoc` in `lib/ecto_shorts/common_filters.ex` so it remains readable while also surfacing the major live filter families and the exact public top-level shapes that route into them. Keep the focus on caller-visible language rather than internal builder organization.

Sixth, revise the public docs for `convert_params_to_filter/3` and `build_query/6` so the caller-visible input shapes, option behavior, and delegation rules are explicit.

Finally, run the targeted validation commands and compare the revised docs against the proof inventory to ensure no documented behavior lacks live evidence and no live, test-backed family remains effectively undocumented.

## Validation and Acceptance

Validation for this task combines review and automated tests.

The review claim for `EctoShorts.Actions` is: a beginner can read `lib/ecto_shorts/actions.ex` and identify the module’s responsibility, the correct family of public functions to start with, the shared option and return-shape model, and the accepted call shapes and important edge behaviors of every public function. The evidence is the revised docs compared against the live function list and the action test suites named above.

The review claim for `EctoShorts.CommonFilters` is: a beginner can read `lib/ecto_shorts/common_filters.ex` and understand how to call `convert_params_to_filter/3`, what `build_query/6` is for, what the top-level routing language looks like, and which broad public filter families are supported today. The evidence is the revised docs compared against the common-filter proof inventory.

The automated-test claim is narrower: running the targeted ExUnit suites after the documentation edits should show that the exercised behaviors still pass exactly as before. Passing tests establish only that the executed cases remain green; they do not prove that the docs are perfectly complete or that undocumented behaviors do not exist elsewhere.

Acceptance for this plan is reached when both of the following are true:

- the docs for the two target modules cover the live, test-backed public behavior surface at a level a beginner can use without reading implementation helpers
- the targeted test commands complete successfully after the documentation-only edits

## Idempotence and Recovery

The planned edits are text-only changes to documentation in two existing source files. Re-running the plan is safe so long as each pass re-reads the live docs and live tests before revising text again.

If a doc change turns out to overstate the contract, the safe recovery path is to narrow the doc back to the test-backed behavior and record that correction in this ExecPlan before making further edits.

If the review uncovers a true conflict between docs, tests, and implementation, the safe recovery path is to stop the documentation edit at that boundary, record the conflict here, and ask for direction rather than silently redefining the contract.

## Artifacts and Notes

The most important artifact from discovery is the mapping between the two modules and their proof suites.

For `EctoShorts.Actions`, that mapping is concentrated in the five action test files, with `actions_crud_test.exs` carrying the widest surface.

For `EctoShorts.CommonFilters`, the mapping is distributed by behavior family across the specialized common-filter test files. The implementation phase should keep a short checklist that maps each newly added or revised documentation section to the test file family that proves it.

## Interfaces and Dependencies

This plan should touch only two implementation files during the documentation phase:

- `lib/ecto_shorts/actions.ex`
- `lib/ecto_shorts/common_filters.ex`

It depends on the public behavior exposed by these existing adjacent modules and helpers, which may be named in docs for orientation but should not be edited as part of this task:

- `EctoShorts.CommonFilters`
- `EctoShorts.CommonSchema`
- `EctoShorts.Actions.Bulk`
- `EctoShorts.Actions.Multi`
- `EctoShorts.Actions.Batch`
- `EctoShorts.Actions.Transaction`
- `EctoShorts.CommonFilters.API`

No new dependencies are required.

Plan note (2026-03-15 21:58Z): This repo-root copy supersedes the draft under `.windsurf/plans` as the governing ExecPlan for implementation.
