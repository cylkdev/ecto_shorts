# Reverify Live API Features Before Completion Work

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the task it describes. If the task later narrows to a specific boundary, test, or contract question, that later reasoning must still be recorded here unless the task is explicitly split into a separate ExecPlan.

This document must be maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

This task exists to prevent API completion work from being driven by stale assumptions. A contributor using this plan should be able to tell which parts of the `ecto_shorts` filter API are already live, which parts were only weakly proved and are now re-proved, which parts are truly missing at runtime, and which future behavior changes would be intentional compatibility work rather than rediscovery. The immediate user-visible outcome is a trustworthy, repo-local governing plan that continues to drive the approved implementation slices without re-opening settled feature-family questions.

A novice should be able to follow this plan and see the same conclusions by reading the named source files and tests. Implementation began only after explicit user approval on 2026-03-14 15:43Z, and every later slice must still be executed from this document rather than from chat context.

## In Scope

This plan governs only the feature re-verification and the resulting completion plan for the live public API.

It includes re-stating the live behavior of the filter pipeline, separating already-implemented behavior from proof gaps and true runtime gaps, capturing the public module boundaries and public function contracts that govern later changes, documenting the internal boundary contracts that make the later edits safe, and specifying the exact future work and validation needed if the user approves implementation.

It includes the following feature families because they were the focus of the live-first audit and materially affect future completion work:

- binding selectors
- join directives
- lock directives
- `with_cte` and `recursive_ctes`
- quantified scalar comparisons
- scalar and array string matching
- array expression behavior

It also includes the proof plan for later implementation, because the missing work is not only runtime work. Some families are already live and only need stronger public proof.

## Out of Scope

This plan does not authorize work outside the approved feature slices and preserved contracts recorded here.

This plan does not update `research/` documents, repository documentation, or examples as a standalone task.

This plan does not broaden scope into unrelated compiler or generator refactors.

This plan does not treat every mismatch between `research/` and the live code as a bug. A mismatch belongs in later implementation only if the live-first audit shows that the behavior is a true runtime gap, a real proof gap, or an intentional future compatibility change that the user still wants.

## Progress

- [x] (2026-03-14 12:05Z) Rebased the task so live code and public tests, not `research/`, are the primary evidence for feature completion.
- [x] (2026-03-14 12:18Z) Closed the binding selector family from live code and tests. Confirmed `{:as, nil}`, `{:as, atom}`, and positive-integer `{:at, n}` support. Did not find `:first` or `:last` aliases.
- [x] (2026-03-14 12:20Z) Closed the join family from live code and tests. Confirmed association, schema, table, query, subquery, fragment, and association shorthand support. Confirmed source kind is selected by the outer join key, `qualifier:` is used for join mode in runtime code, there is no `type:` join-mode alias in live code, and there is no public proof for hints.
- [x] (2026-03-14 12:21Z) Closed the lock family from live code and tests. Confirmed built-in lock aliases plus provider-returned callback support. Confirmed no direct raw function payload at the public boundary.
- [x] (2026-03-14 12:22Z) Closed the `with_cte` family from live code and tests. Confirmed query, subquery, filter-param, `materialized`, nested binding, and `recursive_ctes` interaction support. Confirmed no `operation` support in live code.
- [x] (2026-03-14 12:24Z) Closed the quantified comparison family from live code, tests, and authoritative Ecto docs. Confirmed top-level equality-shorthand runtime support for both `all` and `any`; confirmed lower-level scalar runtime branches for quantified comparison operators; confirmed public proof for `all`; did not find public proof for `any` or broader quantified operator shapes at the `CommonFilters` boundary.
- [x] (2026-03-14 12:26Z) Closed the scalar and array string-matching family from live code, tests, and authoritative Ecto docs. Confirmed that the live contract wraps bare string values and list entries as contains-style patterns even though Ecto `like/2` and `ilike/2` accept raw search patterns. Confirmed no explicit wildcard preservation in the live contract.
- [x] (2026-03-14 12:28Z) Closed the broader array family from live code, tests, and authoritative PostgreSQL docs. Confirmed equality, inequality, membership, overlap, `ANY` comparisons, transforms, and string matching. Confirmed PostgreSQL supports containment operators and `ALL(array)` semantics, but did not find active runtime support for array `nil`, array `count`, containment with `<@`, or `ALL(...)`-style comparison shapes in the live owner.
- [x] (2026-03-14 12:34Z) Created the repo-local governing ExecPlan in `plans/api-feature-reverification.md`.
- [x] (2026-03-14 12:41Z) Reviewed artifact governance with the user and selected `plans/api-feature-reverification.md` as the single governing ExecPlan. The older external megaplan is superseded and no longer authoritative.
- [x] (2026-03-14 12:43Z) Reviewed binding selector scope with the user. Confirmed that `at: :first` and `at: :last` should remain in future completion scope as additive aliases, while integer positional bindings remain the canonical live contract.
- [x] (2026-03-14 14:45Z) Continued the section-by-section review and corrected inaccurate claims before implementation approval. Tightened the quantified, lock, `with_cte`, string-matching, and broader array sections against live code, public tests, and authoritative dependency documentation, and removed stale `:bind` examples from the intended public contract.
- [x] (2026-03-14 15:00Z) Finalized the review artifact language so previously answered Q&A sections are recorded as resolved review notes rather than presented as still-open questions.
- [x] (2026-03-14 15:12Z) Continued the live review in chat and clarified remaining terminology and scope wording in the governing plan. Tightened array references to `ALL(array)`-style behavior, removed the stale join `type:`-alias track, aligned binding-alias examples with the settled future scope, and distinguished settled future-scope items from later optional contract extensions.
- [x] (2026-03-14 15:43Z) Started implementation from this governing plan after explicit user approval. Began with the proof-only slice for quantified `any`, public join-hint coverage, and stronger direct `ArrayExpr` proof. Confirmed during discovery that `config/config.exs` already exposes live hint key `:test_index`, so public hint proof could target the existing contract without adding new config.
- [x] (2026-03-14 15:48Z) Completed the first proof-only slice without runtime edits. Added public `CommonFilters` tests for quantified `any`, added public join-hint coverage for the configured `:test_index` hint key, expanded `test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs` with direct array-expression proof, and ran `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/common_filters_test.exs test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs` plus `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/actions/crud_test.exs`, both passing with no reclassification needed.
- [x] (2026-03-14 15:49Z) Completed the additive binding-selector alias slice at the public boundary. Added `at: :first` and `at: :last` resolution in `CommonFilters` only, kept downstream contracts on integer `{:at, position}` selectors, added boundary-visible proof in `test/ecto_shorts/common_filters_test.exs`, and ran `mix test test/ecto_shorts/common_filters_test.exs` plus `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/actions/crud_test.exs`, both passing.
- [x] (2026-03-14 15:58Z) Performed the final pre-execution review required by the current repo rules before starting the first true runtime-gap slice. Found stale planning-only wording that was no longer safe after implementation approval, and found that the older array `all:` example in `examples/ecto_query_dsl.exs` used an inverted comparison fragment relative to the preserved live `ANY` operator semantics. Updated this plan so the next slice is singular in meaning and no longer depends on chat context to resolve those drifts.
- [x] (2026-03-14 16:18Z) Began the first true runtime-gap slice under TDD. Added and passed boundary-plus-direct proof for array `nil`, then added and passed boundary-plus-direct proof for array `count > 0` and array `count == 0` with the documented coalesced zero-length rule.
- [x] (2026-03-14 16:24Z) The new boundary test for `%{tags: %{all: %{>: "a"}}}` exposed a routing conflict rather than an `ArrayExpr`-only gap. `Postgres.normalize_quantified_term/3` currently rewrites every top-level `{all, payload}` into quantified-subquery handling, so array-local comparison `all:` payloads never reach `ArrayExpr`. Updated this plan before continuing so the remaining slice is singular in meaning again.
- [x] (2026-03-14 16:25Z) Completed the remaining comparison-operator array `all:` slice under the chosen payload-shape split. Narrowed `Postgres.normalize_quantified_term/3` so only quantified-query payloads with `:from` are rewritten into quantified-subquery handling, added `ArrayExpr` support for comparison-operator array-local `all:` payloads while preserving the live reversed operator meaning already used by `ANY`, and proved the executed cases at `Postgres.build_dynamic/4`, `ArrayExpr.dynamic_expr/5`, and `Actions.all/3`.
- [x] (2026-03-14 16:43Z) The user chose Approach 1 for the remaining array `all:` slice: narrow `Postgres.normalize_quantified_term/3` by quantified-query payload shape instead of changing router precedence. Refreshed nearby repo patterns plus current Elixir docs for `Keyword.has_key?/2`, `Map.has_key?/2`, and `is_map/1` before resuming code changes so the split could use plain helper logic rather than an unverified guard form.
- [x] (2026-03-14 16:52Z) Focused and broader proof passed for the completed array `all:` slice. Focused commands: `mix test test/ecto_shorts/dynamics/postgres_test.exs:86`, `mix test test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs:65`, and `mix test test/ecto_shorts/actions/crud_test.exs:1082`, all passing. Broader neighboring regression: `mix test test/ecto_shorts/dynamics/postgres_test.exs test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs test/ecto_shorts/actions/crud_test.exs`, passing with 153 tests and 0 failures.

## Milestones

### Milestone 1: Make the live public boundary explicit

The first milestone is to turn the audit into a reliable picture of the current system. At the end of this milestone, a reader should know where public filter behavior enters the system, where the routing decisions happen, which modules own the directive and expression families, and which parts of the behavior are already proved by public tests.

This milestone is complete when the module specifications, function specifications, context, and internal walkthrough below are accurate enough that a reader can trace a filter from `Actions.all/3` or `CommonFilters.convert_params_to_filter/3` to the owning runtime module without re-discovering the architecture.

### Milestone 2: Separate runtime gaps from proof gaps and future compatibility work

The second milestone is to classify the audit results into three buckets that will drive later implementation. A runtime gap is behavior that the live public path does not implement. A proof gap is behavior that the runtime appears to implement but that the public tests do not prove. Future compatibility work is behavior that is not live today but either remains settled future implementation scope or would require a later explicit contract decision before implementation.

This milestone is complete when each audited family is classified that way and the later plan of work names only the work that remains justified after the live audit.

### Milestone 3: Prepare a safe implementation plan and stop

The third milestone was to make later implementation safe before it started. At the end of this milestone, a contributor knew which files would be touched, which public behaviors had to remain unchanged, which examples defined the missing behavior, and which validation steps would be required after implementation. Work then stopped until the user explicitly authorized implementation, which has now happened.

This milestone is complete when the later `Plan of Work`, example mapping, behaviour specifications, executable tests, and validation matrix are specific enough to guide implementation directly.

### Milestone 4: Execute approved implementation slices without plan drift

The fourth milestone is the current execution stage. Each approved slice must begin with a fresh plan review, follow the smallest owner-module path that satisfies the documented contract, and leave the governing plan more current than it was before the slice started.

This milestone is complete when every executed slice records its proof, preserves the stated unchanged behavior, and leaves no stale planning-only language or ambiguous next-step wording in the governing plan.

## Surprises & Discoveries

- Observation: The join family is broader than the earlier research-first plan assumed.
  Evidence: `lib/ecto_shorts/common_filters/join.ex` accepts `association`, `schema`, `table`, `query`, `subquery`, and `fragment`; `test/ecto_shorts/common_filters_test.exs` publicly proves those source kinds and association shorthand.

- Observation: Join hints appear to be implemented in runtime code but are not publicly proved.
  Evidence: `lib/ecto_shorts/common_filters/join.ex` compiles hint-aware join clauses from configured hints, but the public tests do not exercise `hints:`.

- Observation: The lock family already supports callback-style customization through the provider path.
  Evidence: `lib/ecto_shorts/common_filters/lock.ex` accepts provider results shaped as `{:ok, fn query -> query end}`; `test/ecto_shorts/common_filters_test.exs` proves provider-backed lock behavior.

- Observation: The quantified comparison runtime is broader than the publicly proved shorthand surface.
  Evidence: `Ecto.Query.API` documents `all(subquery)` and `any(subquery)` on the right side of comparison operators `==`, `!=`, `>`, `>=`, `<`, and `<=`; `lib/ecto_shorts/dynamics/postgres.ex` normalizes top-level `:all` and `:any` payloads into equality against a built quantified query; `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` generates quantified branches for both quantifiers across the comparison operator family.

- Observation: The live string-matching contract is a convenience wrapper over a broader Ecto pattern-matching surface.
  Evidence: `Ecto.Query.API` documents `like/2` and `ilike/2` as accepting search patterns directly; `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` wraps scalar `like` and `ilike` values and list entries with `%...%`; `lib/ecto_shorts/dynamics/postgres/array_expr.ex` does the same through `normalize_patterns/1`.

- Observation: Some missing array behaviors are real PostgreSQL-backed possibilities, but the examples actually describe two distinct non-live families that must not be collapsed.
  Evidence: PostgreSQL documents array containment operators such as `<@` and `@>` plus `ALL(array)` semantics; `examples/ecto_query_dsl.exs` contains separate example queries for array `count`, containment using `all: [in: ...]` with `<@`, and comparison-operator `all:` payloads such as `all: [>: "a"]`; the active runtime owner `lib/ecto_shorts/dynamics/postgres/array_expr.ex` does not implement any of those shapes.

- Observation: Public join-hint proof can target an already configured live hint key instead of introducing test-only configuration.
  Evidence: `config/config.exs` sets `config :ecto_shorts, hints: [test_index: ["USE INDEX(test_index)"]]`; `lib/ecto_shorts/common_filters/join.ex` compiles `@hints` from `Application.compile_env(:ecto_shorts, :hints)` and emits hint-aware `build_join/8` clauses for those keys.

- Observation: The first proof-only slice completed cleanly and did not expose a hidden runtime defect in the supposed proof-gap families.
  Evidence: `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/common_filters_test.exs test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs` passed with 268 tests and 0 failures; `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/actions/crud_test.exs` passed with 391 tests and 0 failures.

- Observation: The binding-alias slice could stay entirely at the public boundary because the repo already has a live query-binding count helper.
  Evidence: `lib/ecto_shorts/common_query.ex` exposes `query_binding_count/1`; `lib/ecto_shorts/common_filters.ex` now resolves `at: :first` to positional binding `1` and `at: :last` to `CommonQuery.query_binding_count(query)` before anything reaches the integer-only downstream compiler and dynamic contracts; `mix test test/ecto_shorts/common_filters_test.exs` passed with 180 tests and 0 failures.

- Observation: The old array `all:` example query is inverted relative to the preserved operator meaning already used on the live `ANY` array path.
  Evidence: `lib/ecto_shorts/dynamics/postgres/array_expr.ex` currently treats `{:>, value}` as “some array element is greater than value” by compiling `fragment("? < ANY(?)", ^value, field(...))`; the older example block in `examples/ecto_query_dsl.exs` labeled `tags: [all: [>: "a"]]` shows `fragment("? > ALL(?)", ^"a", p.tags)`, which instead means every array element is less than `"a"`. The next runtime slice must preserve the live caller-visible operator meaning and therefore use the analogous reversed comparison for `ALL`.

- Observation: Array-local comparison `all:` payloads currently collide with quantified subquery `all` routing before they can reach the array owner.
  Evidence: `test/ecto_shorts/actions/crud_test.exs:1082` fails for `%{tags: %{all: %{>: "a"}}}` with `** (KeyError) key :from not found in: [>: "a"]`; `lib/ecto_shorts/dynamics/postgres.ex` rewrites every `{all, payload}` through `normalize_quantified_term/3`; `lib/ecto_shorts/common_filters/set_comparison.ex` then requires `:from`, proving that array-local `all:` currently cannot share the same top-level routing path.

## Decision Log

- Decision: Treat the live public API and public tests as the primary source of truth for feature completion.
  Rationale: The `research/` artifacts may be stale. The user explicitly redirected the task to rule out already-implemented behavior before planning missing work.
  Date/Author: 2026-03-14 / Cascade

- Decision: Separate implemented behavior, proof gaps, and runtime gaps instead of collapsing every audit mismatch into “missing.”
  Rationale: Several families, such as quantified `any`, join hints, and provider-backed lock callbacks, are present in runtime code even though the earlier plan treated them as likely missing.
  Date/Author: 2026-03-14 / Cascade

- Decision: Create the governing ExecPlan in `./plans` rather than continue using the older out-of-repo megaplan.
  Rationale: The current project rules require one governing ExecPlan stored at the repository root in `./plans`. The earlier artifact lived outside the repository and did not satisfy the current standard.
  Date/Author: 2026-03-14 / Cascade

- Decision: Keep `plans/api-feature-reverification.md` as the single governing ExecPlan and retire the older external megaplan as authoritative.
  Rationale: During plan review, the user chose the current repo-local filename as the governing artifact. Preserving that filename keeps the live-first plan title while eliminating competing governance.
  Date/Author: 2026-03-14 / Cascade

- Decision: Keep binding selector aliases `at: :first` and `at: :last` in future completion scope as additive compatibility work.
  Rationale: During plan review, the user chose to keep alias support in scope. The live audit still established that these aliases are not implemented today, so the plan must present them as future additive behavior rather than already-live behavior.
  Date/Author: 2026-03-14 / Cascade

- Decision: Keep the lock directive on the current symbolic, provider-based public contract.
  Rationale: The live `Lock` boundary expects `name:` and optional `values:` and resolves custom lock behavior through the query provider. Ecto documents lock clauses as boolean or string expressions that cannot include fields, so provider-owned translation into a valid Ecto lock expression is viable without widening the public boundary to raw function or raw string payloads. During plan review, the user chose to keep that narrower contract.
  Date/Author: 2026-03-14 / Cascade

- Decision: Keep `with_cte operation:` in future implementation scope even though it is not live today.
  Rationale: The live `WithCte` boundary currently supports `as:` and optional `materialized:` only, but upstream `Ecto.Query.with_cte/3` documents `:operation` support and the user chose to keep Ecto-supported `with_cte` options in scope for later completion rather than treat `operation:` as drift.
  Date/Author: 2026-03-14 / Cascade

- Decision: Keep planning and implementation authorization separate.
  Rationale: The current task is still planning-only. Even though the next implementation steps are now better defined, later code work still requires explicit user approval.
  Date/Author: 2026-03-14 / Cascade

- Decision: Execute the proof-only slice before any compatibility or runtime edits and reclassify only if the new proof exposes a real defect.
  Rationale: The governing plan already identified quantified `any`, join hints, and direct array-expression proof as the safest first slice because live code suggested those behaviors already existed. The new tests passed without runtime edits, so those items remain proof gaps now closed rather than reclassified runtime defects.
  Date/Author: 2026-03-14 / Cascade

- Decision: Implement `at: :first` and `at: :last` only by resolving them to integer positions inside `CommonFilters`.
  Rationale: `lib/ecto_shorts/compiler/query_binding_builder.ex`, `lib/ecto_shorts/dynamics/helpers.ex`, and `lib/ecto_shorts/dynamics/postgres.ex` still accept only integer positional `{:at, position}` contracts. `CommonQuery.query_binding_count/1` provides the last-binding calculation needed for the public alias without widening downstream internal contracts or reviving stale `:bind` work.
  Date/Author: 2026-03-14 / Cascade

- Decision: Split the remaining array work into two contracts instead of treating every nested `all:` payload as one thing.
  Rationale: Repo evidence shows two distinct non-live shapes: containment-style `all: [in: list]` mapped to `<@`, and comparison-operator `all:` payloads such as `all: [>: value]` mapped to `ALL(array)` comparisons. The current next slice will implement only array `nil`, array `count`, and comparison-operator `all:` payloads. Containment-style `all: [in: list]` remains later work so this slice stays singular in meaning.
  Date/Author: 2026-03-14 / Cascade

- Decision: Preserve the current caller-visible operator meaning from the live `ANY` array path when adding comparison-operator `all:` support.
  Rationale: The live `ANY` path already interprets `{:>, value}` as “an element of the array is greater than value,” which is implemented by reversing the SQL comparison around `ANY`. The new `ALL` path must preserve that same caller-facing meaning, so `{:>, value}` will compile to the analogous reversed `ALL` comparison rather than copying the older inverted example query literally.
  Date/Author: 2026-03-14 / Cascade

- Decision: Add a routing distinction in `Postgres` so quantified subquery `all` handling remains intact while array-local comparison `all:` payloads can reach `ArrayExpr`.
  Rationale: The failing `%{tags: %{all: %{>: "a"}}}` boundary test showed that the current unconditional `normalize_quantified_term/3` interception is too broad for the newly approved array-local `all:` contract. The safest minimal fix is to distinguish subquery-shaped quantified payloads from array-local comparison payloads at the Postgres routing boundary before expression ownership is finalized.
  Date/Author: 2026-03-14 / Cascade

- Decision: Implement the routing distinction by checking quantified-query payload shape inside `normalize_quantified_term/3` with ordinary function logic instead of a guard or a broader routing-order change.
  Rationale: Current repo patterns already use plain `Keyword.keyword?/1`, `Keyword.has_key?/2`, and `Map.has_key?/2` checks in function bodies. Current Elixir docs confirm those helpers are valid ordinary runtime checks, while the rejected guard attempt relied on invalid guard-only syntax. This keeps the fix local to the collision point, preserves current quantified-subquery behavior, and lets unsupported array-local `all:` payloads continue following `ArrayExpr` ownership without widening the public contract.
  Date/Author: 2026-03-14 / Cascade

## Outcomes & Retrospective

The old research-first megaplan overstated the amount of missing runtime behavior. The live-first audit reduced the true runtime work to a smaller set of gaps and turned several earlier “missing” items into proof work instead.

Implementation has now started under this governing plan. The first executed slice stayed intentionally narrow: it added proof-only tests for quantified `any`, public join hints, and direct `ArrayExpr` coverage. Those tests passed without runtime edits, which confirms that the earlier classification was accurate for that slice.

The second executed slice added the settled `at: :first` and `at: :last` aliases at the public `CommonFilters` boundary without widening the downstream integer-only positional contracts. Focused and broader regressions passed.

The next slice began as the first true array runtime-gap slice. Array `nil`, array `count > 0`, and array `count == 0` are now implemented with focused direct and boundary proof. The remaining work in that slice narrowed further once live tests exposed a routing conflict for array-local comparison `all:` payloads: the remaining implementation must first distinguish array-local `all:` from quantified subquery `all` at the Postgres routing boundary, then add the matching `ArrayExpr` behavior, while containment-style `<@` work remains explicitly deferred.

## Context and Orientation

`ecto_shorts` exposes database filtering behavior primarily through `EctoShorts.CommonFilters` and the higher-level CRUD entry points in `EctoShorts.Actions`. A caller typically passes filter params into `Actions.all/3` or a similar action helper. That path eventually calls `CommonFilters.convert_params_to_filter/3`, which turns the caller’s map or keyword input into a query by reducing each filter entry in order.

The public predicate-routing boundary lives in `lib/ecto_shorts/common_filters.ex`. That module decides whether a filter key is a binding selector, a filter-group entry such as `where` or `or_where`, an association traversal, or a known filter handled by `EctoShorts.CommonFilters.API`.

The expression-routing boundary lives in `lib/ecto_shorts/dynamics/postgres.ex`. Once the filter pipeline decides that a field predicate should become a dynamic expression, `Postgres.build_dynamic/4` decides whether that field should use `CommonExpr`, `ArrayExpr`, or `ScalarExpr`. That means later feature work for scalar fields, array fields, quantified comparisons, negation, and string matching must preserve the contracts at both boundaries: the public filter pipeline and the dynamic-expression router.

Directive-specific behavior is owned by smaller modules behind `CommonFilters.API`. The important ones for this task are:

- `lib/ecto_shorts/common_filters/join.ex`
- `lib/ecto_shorts/common_filters/lock.ex`
- `lib/ecto_shorts/common_filters/with_cte.ex`
- `lib/ecto_shorts/common_filters/set_comparison.ex`

Public proof currently lives mainly in these files:

- `test/ecto_shorts/common_filters_test.exs`
- `test/ecto_shorts/common_filters_scalar_filter_test.exs`
- `test/ecto_shorts/actions/crud_test.exs`
- `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`
- `test/ecto_shorts/dynamics/postgres_test.exs`
- `test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs`

## Module Specifications

### `EctoShorts.CommonFilters`

`EctoShorts.CommonFilters` is the public query-building boundary for filter params. Use it when the caller has a schema or queryable plus a filter map or keyword list and wants an `Ecto.Query` with the requested filters applied. Do not use it to define new SQL fragments directly; directive owners and dynamic builders handle that lower-level work.

Start with `convert_params_to_filter/3`. The module guarantees that it will reduce recognized filter entries through the API registry, preserve binding-selection context as filters nest, and hand field predicates to the configured query builder or the default API path. The module owns top-level param reduction and binding context selection. It does not own the implementation details of joins, locks, CTEs, or scalar and array expression generation.

### `EctoShorts.Dynamics.Postgres`

`EctoShorts.Dynamics.Postgres` is the public adapter-facing entry point for dynamic predicate building. Use it when a field predicate has already been selected for expression generation and must become an `Ecto.Query.DynamicExpr` on the Postgres path.

Start with `build_dynamic/4`. The module guarantees that it will normalize negation and quantified top-level forms before dispatching to `CommonExpr`, `ArrayExpr`, or `ScalarExpr`. It owns the routing decision between common operators, array-field operators, and scalar-field operators. It does not own the final SQL fragments for each operator family.

### `EctoShorts.Dynamics.Postgres.ArrayExpr`

`ArrayExpr` owns Postgres-specific behavior for array and map-backed field predicates once routing has already determined that the field is array-like. It accepts the normalized binding selector, field key, negation flag, and operator/value term, and returns a dynamic expression when the shape is supported.

The live contract currently covers list equality and inequality, scalar membership, overlap via `in` with a list, element-wise comparisons using `ANY`, `lower` and `upper` transforms, and array `like` and `ilike`. PostgreSQL does support broader array operators such as containment and `ALL(array)` semantics, but the live contract here does not currently document or prove array `nil`, `count`, containment with `<@`, or `ALL(...)`-style comparison shapes.

### `EctoShorts.CommonFilters.Join`

`Join` owns join directive payloads after `CommonFilters.API` dispatches to it. It accepts join params, resolves source kinds, builds the `on` expression, and applies the join to the query.

The live contract supports association, schema, table, query, subquery, and fragment sources. It uses `qualifier` as the runtime key for join type. It also contains hint-aware runtime branches. The live contract does not currently include a `type` compatibility alias.

### `EctoShorts.CommonFilters.Lock`

`Lock` owns lock directive payloads after API dispatch. The live contract expects a map or keyword payload with a `name` key. Built-in names `:for_update` and `:for_share` are handled directly. Other names are resolved through the query provider, which must return `{:ok, function}`, `{:error, reason}`, or `nil`. Provider-owned `values:` may be translated into a valid Ecto lock expression there without widening the public boundary.

The live contract includes provider-backed callback customization. It does not currently expose a direct raw function payload or direct raw string lock payload at the public boundary.

### `EctoShorts.CommonFilters.WithCte`

`WithCte` owns `with_cte` payloads after API dispatch. It accepts a map or keyword list of named CTE definitions, each of which must include `as:` as either a query, subquery, or filter-param payload. It also accepts `materialized:` when that option is boolean or `nil`.

The live contract includes nested use under named and positional bindings and works alongside `recursive_ctes`. The live contract does not currently expose an `operation:` option.

## Function Specifications

### `EctoShorts.CommonFilters.convert_params_to_filter/3`

Purpose: convert a filter map or keyword list into an `Ecto.Query` by reducing entries through the public filter pipeline.

Accepted inputs:

- `source`: a schema, queryable, or existing `Ecto.Query`
- `params`: a map or keyword list of filter directives and field predicates
- `opts`: keyword options that may include query-builder and provider collaborators

Return shape:

- returns an `Ecto.Query`

Caller-visible rules that matter for this task:

- top-level filters begin at the root binding `{:as, nil}`
- top-level `as` and `at` maps change the selected binding for nested filter reduction on the intended public path
- recognized filter keys route through `CommonFilters.API`
- unknown field keys fall back to field predicate building on the current filter
- association-shaped entries can trigger named association bindings automatically

Important omitted-input behavior:

- omitted binding selection keeps the current binding context
- omitted directive-specific keys fall back to the owner module defaults

Important invalid-input behavior:

- invalid shapes are often preserved as an unchanged query with warnings logged by the owner module, rather than raising at the top-level filter reducer

### `EctoShorts.Actions.all/3`

Purpose: execute the filter pipeline against a queryable and return all matching records.

Accepted inputs:

- `queryable`
- `params` as a map or keyword list
- `opts` for repo and dynamic-adapter collaborators

Return shape:

- returns a list of records

Caller-visible rules that matter for this task:

- the action boundary is the main integration proof surface for public filter behavior
- filter semantics observed here must remain consistent with the underlying `CommonFilters` contract

Important omitted-input behavior:

- omitted filter keys preserve the base query

Important invalid-input behavior:

- invalid filter shapes are generally handled by lower-level filter owners, which may keep the query unchanged and log a warning instead of raising

## Internal Boundary Contracts

### Boundary: `CommonFilters.convert_params_to_filter/3` to `apply_filters/6`

Upstream caller: `Actions` and any direct caller of `CommonFilters.convert_params_to_filter/3`.

Accepted input shape: a query source plus a map or keyword list of filter entries.

Produced output shape: an `Ecto.Query` with each filter entry reduced in order.

Owned transformations:

- normalize maps to keyword-like reduction order
- set the initial selected binding to `{:as, nil}`
- iterate each `{key, value}` pair through `apply_filters/6`

Transformations that do not belong here:

- directive-specific query mutations
- field-expression generation
- join-source resolution details

Concrete handoff example:

- Input at the public boundary: `%{at: %{1 => %{published: true}}}`
- Handoff through `apply_filters/6`: selected binding becomes `{:at, 1}` and remaining entries continue under that binding
- Resulting downstream responsibility: the eventual field predicate for `published` is built against that positional binding

### Boundary: `CommonFilters.apply_filters/6` to `CommonFilters.API.build_query/6`

Accepted input shape: a current filter context, source, current query, selected binding, a filter key, a term, and opts.

Produced output shape: a next `Ecto.Query`.

Owned transformations:

- choose whether the key follows the live-code `bind` branch, an intended public binding operator, a predicate-group member, a post-aggregate-group member, an association traversal, a registered API filter, or a fallback field predicate

Forbidden accidental contract expansion:

- `apply_filters/6` must not become the place where join payload aliases, lock payload aliases, or array-only shapes are silently normalized. Those responsibilities belong in the owning directive or dynamic-expression boundary.

### Boundary: `Postgres.build_dynamic/4` to `build_expr/5`

Accepted input shape: source, selected binding, `{field_key, term}`, and opts.

Produced output shape: an `Ecto.Query.DynamicExpr` or `nil` when the selected binding or downstream term is unsupported.

Owned transformations:

- normalize top-level negation from `{:not, term}` into `{negated, term}`
- normalize top-level quantified subquery forms `{:all, payload}` and `{:any, payload}` into equality against a built quantified query when the payload is on the quantified-query contract
- route by operator family or array-field detection

Forbidden accidental contract expansion:

- array-only enhancements such as `count` or `all` must not be added here as incidental intermediate shapes; they belong in `ArrayExpr` once the term reaches the array boundary
- the Postgres router must not eagerly rewrite array-local comparison `all:` payloads into quantified-subquery handling, because doing so prevents the array owner from seeing the approved array-local contract

### Boundary: `ArrayExpr.dynamic_expr/5`

Accepted live shapes today:

- `{:==, list}` and `{:!=, list}` for exact array equality and inequality
- `{:==, value}` and `{:!=, value}` for membership and non-membership
- `{:in, list}` for overlap and `{:in, value}` for scalar membership
- `{:>, value}`, `{:>=, value}`, `{:<, value}`, `{:<=, value}` for `ANY` comparisons
- `{:==, {:lower, value}}`, `{:!=, {:lower, value}}`, and upper-case counterparts
- `{:like, value}` and `{:ilike, value}` for scalar or list pattern search after local contains-style pattern wrapping

Produced output shape: a dynamic expression or `nil`.

Unsupported live shapes that matter for later work:

- `nil`
- nested `count` payloads
- nested `ALL(array)`-style payloads
- containment fragments such as `<@`

Current in-flight execution notes:

- array `nil` is now implemented and proved
- nested `count` with `{:>, value}` and `{:==, 0}` is now implemented and proved
- comparison-operator `all:` still cannot reach this boundary until the Postgres routing conflict is resolved

### Boundary: `Join.build_query/6`

Accepted live option keys that matter:

- source-kind key such as `association`, `schema`, `table`, `query`, `subquery`, or `fragment`
- `source`
- `as`
- `on`
- `qualifier`
- `prefix`
- `hints`

Produced output shape: an `Ecto.Query` with the requested join applied or the unchanged query on invalid input paths.

Unsupported live shape that matters for later work:

- `type:` used as a join-mode alias for `qualifier:`

### Boundary: `Lock.build_query/6`

Accepted live shape today:

- map or keyword payload with `name:` and optional `values:`

Produced output shape: an `Ecto.Query` with a built-in lock clause, a provider-translated lock clause, or the unchanged query when the provider path returns `nil`, `{:error, reason}`, or an invalid callback shape.

Unsupported live shape that matters for later work:

- direct raw function payload without a `name:` indirection
- direct raw string lock clause without a `name:` indirection

### Boundary: `WithCte.build_query/6`

Accepted live shape today:

- `with_cte: [cte_name: [as: query_or_subquery_or_filter_params, materialized: boolean_or_nil]]`

Produced output shape: an `Ecto.Query` with one or more CTEs applied.

Unsupported live shape that matters for later work:

- `operation:` passthrough

## Internal Structure Walkthrough

A typical public integration call begins at `EctoShorts.Actions.all/3`. That action hands filter params to `EctoShorts.CommonFilters.convert_params_to_filter/3`, which starts with `CommonSchema.to_query(source)` and reduces each filter entry from the root selected binding `{:as, nil}`.

For each entry, `apply_filters/6` decides what kind of thing it is looking at. In the intended public contract, top-level `as` and `at` maps change the selected binding and continue nested reduction. The live code still contains a `bind` branch, but that path is treated elsewhere in this plan as stale runtime drift rather than part of the intended caller-facing contract. If the key belongs to a filter group such as predicate or post-aggregate filters, the reducer either keeps descending or dispatches to the registered filter owner. If the key names an association and the term shape looks reducible, `CommonFilters` first ensures a named association binding through `with_named_binding`, then continues reduction under `{:as, association_name}`. Otherwise, the reducer dispatches directly to `CommonFilters.API.build_query/6`.

When a field predicate reaches the Postgres dynamic path, `EctoShorts.Dynamics.Postgres.build_dynamic/4` normalizes top-level negation, then normalizes top-level quantified forms such as `all` and `any` into equality against a built quantified query. After that, it routes by family. Common operators such as `before` and `after` stay in `CommonExpr`. Array and map-backed fields route to `ArrayExpr`. Everything else routes to `ScalarExpr`.

That routing order matters for later implementation. Array `count` support did belong in `ArrayExpr.dynamic_expr/5` once the field was already known to be array-like. The new failing array-local `all:` boundary test shows that comparison-operator `all:` cannot be completed in `ArrayExpr` alone, because the current Postgres router intercepts it first as quantified-subquery work. The next remaining change therefore starts in `Postgres.normalize_quantified_term/3` or a nearby router guard so the array-local contract can reach `ArrayExpr` without breaking quantified subquery `all`. If a join payload alias or lock payload alias is added, the change belongs in the directive owner after API dispatch, not in `CommonFilters.apply_filters/6`.

Invalid-input behavior also differs by boundary. At the top-level reducer, many invalid shapes are still allowed to flow to the owner module. The owner module usually decides whether to log and keep the query unchanged or to raise for impossible internal shapes. That means later proof work must keep the invalid-input behavior visible at the owning boundary instead of hiding it behind broad guards in `CommonFilters`.

## Example Mappings

### Story: Binding selector aliases as completed additive support over the current integer-only downstream contract

The public filter pipeline now supports root binding, named binding, positive-integer positional binding, and the additive aliases `:first` and `:last`. The implemented alias support normalizes back to the current integer-based downstream contract instead of changing the compiler or dynamic builders.

#### Rules:

- The live public path must continue to accept `as: alias_name`, `as: nil`, and `at: positive_integer`.
- Omitting binding selection must continue to mean “use the current binding context.”
- `at: :first` and `at: :last` are now part of the live public contract.
- Alias support must continue to normalize to the existing integer-based downstream contract instead of changing the compiler and dynamic builders to accept multiple internal positional representations.

#### Examples:

`CommonFilters.convert_params_to_filter(Post, %{at: %{2 => %{preload: :author}}}, [])`
`#=> returns a query whose positional binding 2 is used for the nested preload behavior`

`CommonFilters.convert_params_to_filter(Post, %{at: %{1 => %{published: true}}}, [])`
`#=> returns a query that applies the published predicate against positional binding 1`

`CommonFilters.convert_params_to_filter(Post, %{at: %{first: %{published: true}}}, [])`
`#=> returns a query that applies the published predicate against positional binding 1 after public-boundary alias resolution`

#### Resolved Review Notes:

- **Q:** Is `:first` live in the current public path now? **A:** Yes. `CommonFilters` resolves `:first` and `:last` at the public boundary while downstream contracts stay integer-only.
- **Q:** Where does alias normalization happen? **A:** At the public filter boundary before the integer-based downstream binding contracts are used.

### Story: Array feature completion after the live audit

The live array path already supports several behaviors, but the audit confirmed that some research-era array features are not active runtime behavior today. Later implementation must add only the missing shapes while preserving the live ones.

#### Rules:

- Existing live array equality, inequality, membership, overlap, `ANY` comparison, transform, and string-matching behavior must remain unchanged.
- Ecto `like/2` and `ilike/2` accept raw search patterns, but the live repo contract currently adds contains-style wrapping for bare scalar values and list entries on both scalar and array paths.
- Unsupported array-only shapes must continue to follow the current `ArrayExpr` acceptance/rejection behavior unless this plan explicitly changes that contract.
- Invalid or unsupported array payload shapes must not be silently treated as supported behavior.
- PostgreSQL-backed shapes such as containment with `<@` and `ALL(array)` are technically viable, but they are not part of the live contract today.
- The current remaining slice adds array-local comparison `all:` payloads only after the Postgres router distinguishes them from quantified subquery `all`.
- Containment-style `all: [in: list]` with `<@` remains later work and is not part of the current execution slice.

#### Examples:

`Actions.all(Post, %{tags: ["elixir", "erlang"]})`
`#=> returns posts whose tags field exactly equals that list`

`Actions.all(Post, %{tags: %{>: "a"}})`
`#=> returns posts where any array element is greater than "a"`

`Actions.all(Post, %{tags: nil})`
`#=> returns posts whose tags field is null`

`Actions.all(Post, %{tags: %{count: %{>: 0}}})`
`#=> current next slice should return posts whose array_length(tags, 1) is greater than 0`

`Actions.all(Post, %{tags: %{count: %{==: 0}}})`
`#=> current next slice should return posts whose coalesced array length is 0, following the documented example contract for empty-array zero checks`

`Actions.all(Post, %{tags: %{all: %{>: "a"}}})`
`#=> current remaining slice should return posts where every array element is greater than "a", preserving the same caller-facing operator meaning as the existing `ANY` array path`

`Actions.all(Post, %{tags: %{all: %{in: ["elixir", "erlang"]}}})`
`#=> not part of the current execution slice; containment-style `<@` support remains later work`

#### Resolved Review Notes:

- **Q:** Does the active runtime owner already contain array `count` support? **A:** No. The active `ArrayExpr` implementation does not contain `count` branches.
- **Q:** Do official docs make containment and `ALL(array)` real options at the database layer? **A:** Yes. PostgreSQL documents both, but the live repo owner does not currently expose them.
- **Q:** Does every old example query map cleanly onto the preserved current operator meaning? **A:** No. The older `all: [>: value]` example query is inverted relative to the preserved live `ANY` comparison semantics, so this plan defines the `ALL` implementation by preserving the current caller-facing operator meaning instead of copying that stale example literally.
- **Q:** Are containment-style `all: [in: list]` and comparison-operator `all:` part of the same implementation slice? **A:** No. This slice implements only the comparison-operator `all:` family. Containment remains later work.
- **Q:** Can comparison-operator `all:` be implemented in `ArrayExpr` alone? **A:** No. The failing live boundary test showed that `Postgres.normalize_quantified_term/3` currently intercepts top-level `all:` first, so the remaining slice must start by distinguishing array-local `all:` from quantified subquery `all` at the Postgres routing boundary.

### Story: Runtime support versus proof support for quantified comparisons

The quantified comparison family already contains more runtime behavior than the earlier plan assumed, but the live public shorthand is narrower than the full Ecto quantified-comparison contract. Later work should prove the live path before changing runtime code.

#### Rules:

- Ecto’s quantified comparison contract requires `all` and `any` on the right-hand side of a comparison against a subquery and allows them with `==`, `!=`, `>`, `>=`, `<`, and `<=`.
- The live top-level shorthand path currently normalizes bare `all` and `any` payloads into equality against a built quantified query.
- The lower-level scalar runtime currently contains quantified `all` and `any` branches across the comparison operator family and should not be removed or rewritten unless proof work exposes a real defect.
- Omitting a select override in quantified query building must continue to default to the outer field.
- Invalid quantified payload shapes should continue to follow the current helper behavior instead of inventing a new failure model during proof work.

#### Examples:

`CommonFilters.convert_params_to_filter(Post, %{id: %{all: %{from: Comment, where: %{published: true}}}}, [])`
`#=> returns a query with equality against all(selected comment ids)`

`CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{all: %{from: Comment, where: %{published: true}}}}}, [])`
`#=> returns a query with the quantified equality wrapped in not`

`CommonFilters.convert_params_to_filter(Post, %{id: %{any: %{from: Comment, where: %{published: true}}}}, [])`
`#=> returns a query with equality against any(selected comment ids), but public proof still needs to be added`

`ScalarExpr.dynamic_expr({:as, nil}, :id, nil, {:>, {:all, subquery_expr}}, [])`
`#=> lower-level runtime can build a greater-than-all comparison, but that broader operator surface is not yet publicly proved at the CommonFilters boundary`

#### Resolved Review Notes:

- **Q:** What does authoritative Ecto documentation allow here? **A:** `Ecto.Query.API` documents `all(subquery)` and `any(subquery)` on the right side of `==`, `!=`, `>`, `>=`, `<`, and `<=`.
- **Q:** Is `any` at the public shorthand boundary a runtime gap or a proof gap? **A:** A proof gap, unless later targeted tests expose a real defect.
- **Q:** Does the current public proof cover broader non-equality quantified operator shapes? **A:** No. The public tests prove only the equality shorthand for `all`; the broader quantified operator surface is present only in lower-level runtime code today.

### Story: Directive compatibility and preserved live behavior

Later completion work may add settled future-scope directive support or later explicit compatibility extensions, but the live directive contracts are narrower than the older research-first megaplan assumed.

#### Rules:

- Existing join `qualifier:` behavior must remain canonical internally.
- Existing name-based provider-backed lock behavior must remain unchanged.
- Existing `with_cte` support for `as:` and optional `materialized:` must remain unchanged.
- `with_cte operation:` is not part of the live contract today, but remains future implementation scope.
- Invalid directive payloads must continue to follow the current owner-specific logging and unchanged-query behavior unless later implementation deliberately changes that contract.

#### Examples:

`CommonFilters.convert_params_to_filter(Post, %{join: [schema: [source: User, as: :user, on: %{author_id: 1}]]}, [])`
`#=> returns a query with the schema join applied`

`CommonFilters.convert_params_to_filter(Post, %{lock: %{name: :provider_for_update}}, query_provider: EctoShorts.TestQueryProvider)`
`#=> returns a query with a provider-backed FOR UPDATE lock`

`CommonFilters.convert_params_to_filter(Post, %{with_cte: [published_posts: [as: [published: true], materialized: false]]}, [])`
`#=> returns a query with the named CTE applied and materialized false`

`CommonFilters.convert_params_to_filter(Post, %{with_cte: [published_posts: [as: [published: true], operation: :all]]}, [])`
`#=> not part of the live contract today; later implementation would need to add explicit `operation:` support`

#### Resolved Review Notes:

- **Q:** What does `type` mean in this family after review? **A:** In the intended terminology, `type` refers to the join source kind selected by the outer key such as `association`, `schema`, `table`, `query`, `subquery`, or `fragment`. Join mode remains under `qualifier:`. The live API does not accept `type:` as a join-mode alias.

## Behaviour Specifications

### Feature: Preserve the verified live contract while narrowing future completion work

Scenario: Binding selectors remain on the verified live contract
  Given the current filter pipeline
  When a caller uses root binding, named binding, or positive-integer positional binding
  Then the plan must treat that behavior as already implemented
  And the implemented alias support must remain normalized back to the integer-only downstream contract

Scenario: Quantified `any` is treated as live until disproved
  Given the current Postgres quantified routing code
  When the plan classifies set-comparison work
  Then `any` must be listed as implemented in runtime
  And it must be listed as needing public proof rather than immediate runtime implementation

Scenario: Array `count` is treated as missing runtime behavior until this slice lands
  Given the active `ArrayExpr` owner
  When the plan classifies array-family work
  Then array `count` must be listed as a runtime gap
  And existing array equality, membership, comparison, transform, and string-matching behavior must be listed as preserved

Scenario: The current array runtime slice excludes containment-style `all: [in: list]`
  Given the remaining array-family work
  When the next slice is executed
  Then comparison-operator `all:` payloads belong in the slice
  And containment-style `all: [in: list]` must remain deferred instead of being folded in implicitly

Scenario: Comparison-operator array `all:` must not break quantified subquery `all`
  Given the existing quantified subquery support in `Postgres`
  When array-local comparison `all:` support is added
  Then quantified subquery `all` must still route through `SetComparison`
  And array-local comparison `all:` must reach `ArrayExpr` instead of raising on missing `:from`

Scenario: Directive compatibility work preserves the current owner contracts
  Given the live `Join`, `Lock`, and `WithCte` modules
  When the plan describes future compatibility work
  Then it must preserve `qualifier:` as the canonical live join key
  And it must preserve name-based provider-backed lock callbacks
  And it must not widen the public lock boundary beyond `name:` payloads without a separate later decision
  And it must preserve `with_cte` support for `as:` and `materialized:`
  And it must keep `with_cte operation:` in future scope as explicit non-live implementation work

### Feature: Execute approved slices only from the current governing plan

Scenario: Execution still begins from the current ExecPlan instead of chat context
  Given an already approved implementation task
  When a new slice is about to start
  Then the implementer must re-read the governing ExecPlan fresh
  And the plan must be updated first if stale wording or conflicting examples are found

## Executable Tests

Later implementation or proof work must use real ExUnit tests in this repository. The tests below are the required proof surfaces for later milestones.

For proof gaps:

- Add public tests for quantified `any` in `test/ecto_shorts/common_filters_scalar_filter_test.exs` and, if useful, the lower-level dynamic tests in `test/ecto_shorts/dynamics/postgres_test.exs` or `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`.
- Add public tests for join hints in `test/ecto_shorts/common_filters_test.exs`.
- Expand direct array-path proof in `test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs` so the supported live array contract is proved more explicitly.

For runtime gaps and compatibility work during the approved implementation:

- Add boundary-visible tests for binding selector aliases in `test/ecto_shorts/common_filters_test.exs` and any lower-level compiler or dynamic tests needed only if the alias normalization touches those boundaries.
- Add array runtime tests in `test/ecto_shorts/actions/crud_test.exs`, `test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs`, and targeted `test/ecto_shorts/dynamics/postgres_test.exs` coverage for router conflicts where needed. Keep containment-style `<@` work separate until it is intentionally implemented.
- Add directive tests in `test/ecto_shorts/common_filters_test.exs` for `with_cte operation` support if that feature is implemented.
- Add scalar and array string-matching tests only if a later explicit decision adds wildcard-preservation compatibility over the current contains-style wrapper contract.

Each later test must prove one caller-visible claim. Passing tests should be described as evidence for the executed cases only, not as proof of correctness for all possible inputs.

## Validation and Acceptance

The planning artifact itself is acceptable when a reader can trace each audited family to one of three outcomes: already implemented, implemented but under-proved, or missing runtime behavior.

The future validation matrix for implementation work is:

- Claim: binding selector aliases are supported without breaking existing integer positional bindings.
  Boundary: `CommonFilters.convert_params_to_filter/3` plus any downstream binding contract touched by the implementation.
  Proof method: ExUnit boundary tests and targeted lower-level tests only if the alias normalization crosses those boundaries.
  Evidence command: `mix test test/ecto_shorts/common_filters_test.exs` and any smaller focused test files added for the touched compiler or dynamic boundary.
  Residual risk: if alias support depends on query-shape-specific last-binding detection, edge cases across unusual join counts may still need broader coverage.

- Claim: array `nil`, `count`, and comparison-operator `all:` behaviors work without breaking the current live array contract.
  Boundary: `Actions.all/3`, `Postgres.build_dynamic/4`, and `ArrayExpr.dynamic_expr/5`.
  Proof method: boundary integration tests plus targeted router tests plus direct array-expression tests.
  Evidence command: `mix test test/ecto_shorts/actions/crud_test.exs test/ecto_shorts/dynamics/postgres_test.exs test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs`.
  Residual risk: behavior remains Postgres-specific, and containment-style `<@` support still remains separate later work.

- Claim: join hints and quantified `any` are publicly proved.
  Boundary: `CommonFilters.convert_params_to_filter/3` and the existing Postgres dynamic path.
  Proof method: ExUnit behavior tests.
  Evidence command: `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/dynamics/postgres_test.exs test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`.
  Residual risk: these tests establish the executed cases only.

- Claim: later directive compatibility work preserves the current live directive owners.
  Boundary: `Join.build_query/6`, `Lock.build_query/6`, and `WithCte.build_query/6` through the public filter pipeline.
  Proof method: boundary tests plus review against the preserved contracts recorded in this plan.
  Evidence command: `mix test test/ecto_shorts/common_filters_test.exs`.
  Residual risk: any new aliasing must be checked carefully so it does not weaken invalid-input behavior or change the canonical internal option keys unexpectedly.

## Plan of Work

Perform the approved implementation work in this order.

First, add proof-only changes before runtime changes wherever the live audit suggests that runtime behavior already exists. That means quantified `any`, join hints, and stronger direct array-expression proof come before runtime edits. If any proof-only test exposes a real defect, update this plan’s `Progress`, `Decision Log`, and the affected contracts before changing code.

Second, implement small compatibility shims where the desired future behavior is additive and can normalize back to the existing canonical live contract. Binding selector aliases belong in this category. The key safety rule is that the public boundary may accept a broader input shape, but the downstream internal boundary should stay on the existing canonical representation whenever possible.

Third, implement the true runtime gaps in the smallest owner modules possible. The current array slice first fixes Postgres routing so array-local comparison `all:` can reach the array owner without breaking quantified subquery `all`, then completes the remaining array-owner work in `lib/ecto_shorts/dynamics/postgres/array_expr.ex`. Containment-style `all: [in: list]` with `<@` remains separate later work. `with_cte operation` belongs in `lib/ecto_shorts/common_filters/with_cte.ex`. These changes must preserve the current live behaviors already proved by tests.

Fourth, handle the string-matching contract only if a later explicit decision extends the current live behavior. If implemented, the work belongs in `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` and `lib/ecto_shorts/dynamics/postgres/array_expr.ex`. The preserved behavior is that bare strings keep the current contains-style convenience even though Ecto itself accepts raw patterns. The optional compatibility extension would be explicit wildcard preservation for callers who pass patterns that already include wildcards.

After each implementation slice, update this plan, run the named focused tests, and record the result precisely as evidence for the executed cases.

## Concrete Steps

Work from the repository root `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

For proof-only changes, the expected focused commands are:

    mix test test/ecto_shorts/common_filters_scalar_filter_test.exs
    mix test test/ecto_shorts/common_filters_test.exs
    mix test test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs test/ecto_shorts/dynamics/postgres_test.exs
    mix test test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs

For broader regression after a completed slice, the likely command set is:

    mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/actions/crud_test.exs

The contributor executing this plan must record which behaviors those commands exercised and what uncertainty remains afterward.

## Idempotence and Recovery

Planning edits to this file are safe to repeat as long as the `Progress`, `Decision Log`, and `Outcomes & Retrospective` sections are kept current.

Later implementation should proceed in additive, test-backed slices. If a slice exposes a different live contract than this plan currently states, stop, update the governing plan first, and only then continue.

If a proof-only test shows that an assumed runtime path is actually broken, do not continue under the “proof gap” classification. Reclassify the item as a runtime defect in this plan, explain the new evidence in `Surprises & Discoveries`, and update the `Decision Log` before writing code.

## Interfaces and Dependencies

The main public interface for this task is `EctoShorts.CommonFilters.convert_params_to_filter/3`, exercised directly and indirectly through `EctoShorts.Actions.all/3`.

The main internal collaborators are:

- `EctoShorts.CommonFilters.API`
- `EctoShorts.Dynamics.Postgres`
- `EctoShorts.Dynamics.Postgres.ArrayExpr`
- `EctoShorts.Dynamics.Postgres.ScalarExpr`
- `EctoShorts.CommonFilters.Join`
- `EctoShorts.CommonFilters.Lock`
- `EctoShorts.CommonFilters.WithCte`
- `EctoShorts.CommonFilters.SetComparison`
- `EctoShorts.Compiler.QueryBindingBuilder`
- `EctoShorts.Dynamics.Helpers`

The later implementation must preserve their existing public and internal handoff contracts unless this plan is revised first.

## Artifacts and Notes

Important live-audit evidence used to build this plan includes:

- `lib/ecto_shorts/common_filters.ex`
- `lib/ecto_shorts/common_filters/api.ex`
- `lib/ecto_shorts/common_filters/join.ex`
- `lib/ecto_shorts/common_filters/lock.ex`
- `lib/ecto_shorts/common_filters/with_cte.ex`
- `lib/ecto_shorts/common_filters/set_comparison.ex`
- `lib/ecto_shorts/dynamics/postgres.ex`
- `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`
- `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex`
- `lib/ecto_shorts/dynamics/postgres/array_expr.ex`
- `test/ecto_shorts/common_filters_test.exs`
- `test/ecto_shorts/common_filters_scalar_filter_test.exs`
- `test/ecto_shorts/actions/crud_test.exs`
- `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`
- `test/ecto_shorts/dynamics/postgres_test.exs`
- `test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs`
- `Ecto.Query.lock/3` in `Ecto.Query`
- `Ecto.Query.with_cte/3` in `Ecto.Query`
- `Ecto.Query.API` entries for `all/1`, `any/1`, `like/2`, and `ilike/2`
- PostgreSQL documentation for row and array comparisons (`ANY` / `ALL`) and array operators such as `<@`, `@>`, and `&&`

Revision note (2026-03-14 12:34Z): created this repo-local governing ExecPlan because the earlier megaplan lived outside the repository and no longer matched the current planning rules. Rebased the plan on the live-first audit and narrowed the later implementation scope to true runtime gaps, proof gaps, and explicit compatibility choices.

Revision note (2026-03-14 12:41Z): reviewed artifact governance with the user and kept `plans/api-feature-reverification.md` as the single governing ExecPlan. The older external megaplan is superseded and no longer authoritative.

Revision note (2026-03-14 12:43Z): reviewed binding selector scope with the user and kept `at: :first` and `at: :last` in future completion scope as additive compatibility aliases. The plan now treats that as settled future scope rather than an open preference question.

Revision note (2026-03-14 15:49Z): implementation began after explicit user approval. Recorded the completed proof-only slice for quantified `any`, join hints, and direct array-expression coverage, captured the passing focused and broader test evidence, and advanced the next slice to additive binding-selector alias work.

Revision note (2026-03-14 15:53Z): completed the additive binding-selector alias slice by resolving `at: :first` and `at: :last` to integer positions in `CommonFilters`, recorded the passing focused and broader validation evidence, and advanced the next slice to the true array runtime gaps.
