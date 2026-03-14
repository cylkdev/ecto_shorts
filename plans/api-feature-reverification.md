# Reverify Live API Features Before Completion Work

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the task it describes. If the task later narrows to a specific boundary, test, or contract question, that later reasoning must still be recorded here unless the task is explicitly split into a separate ExecPlan.

This document must be maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

This task exists to prevent the next round of API completion work from being driven by stale assumptions. After this plan is complete, a contributor should be able to tell which parts of the `ecto_shorts` filter API are already live, which parts are live but weakly proved, which parts are truly missing at runtime, and which future behavior changes would be intentional compatibility work rather than rediscovery. The immediate user-visible outcome of this planning work is a trustworthy, repo-local governing plan that can drive later implementation without re-opening settled feature-family questions.

A novice should be able to follow this plan and see the same conclusions by reading the named source files and tests. No implementation should happen from this plan until the user explicitly approves it.

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

This plan does not authorize any code changes, test changes, or behavior-changing terminal commands.

This plan does not update `research/` documents, repository documentation, or examples as a standalone task.

This plan does not broaden scope into unrelated compiler or generator refactors.

This plan does not treat every mismatch between `research/` and the live code as a bug. A mismatch belongs in later implementation only if the live-first audit shows that the behavior is a true runtime gap, a real proof gap, or an intentional future compatibility change that the user still wants.

## Progress

- [x] (2026-03-14 12:05Z) Rebased the task so live code and public tests, not `research/`, are the primary evidence for feature completion.
- [x] (2026-03-14 12:18Z) Closed the binding selector family from live code and tests. Confirmed `{:as, nil}`, `{:as, atom}`, and positive-integer `{:at, n}` support. Did not find `:first` or `:last` aliases.
- [x] (2026-03-14 12:20Z) Closed the join family from live code and tests. Confirmed association, schema, table, query, subquery, fragment, and association shorthand support. Confirmed `qualifier` in runtime code, no `type` alias in live code, and no public proof for hints.
- [x] (2026-03-14 12:21Z) Closed the lock family from live code and tests. Confirmed built-in lock aliases plus provider-returned callback support. Confirmed no direct raw function payload at the public boundary.
- [x] (2026-03-14 12:22Z) Closed the `with_cte` family from live code and tests. Confirmed query, subquery, filter-param, `materialized`, nested binding, and `recursive_ctes` interaction support. Confirmed no `operation` support in live code.
- [x] (2026-03-14 12:24Z) Closed the quantified comparison family from live code and tests. Confirmed runtime support for both `all` and `any`; confirmed public proof for `all`; did not find public proof for `any`.
- [x] (2026-03-14 12:26Z) Closed the scalar and array string-matching family from live code and tests. Confirmed that the live contract wraps bare string values and list entries as contains-style patterns. Confirmed no explicit wildcard preservation in the live contract.
- [x] (2026-03-14 12:28Z) Closed the broader array family from live code and tests. Confirmed equality, inequality, membership, overlap, `ANY` comparisons, transforms, and string matching. Did not find active runtime support for array `nil`, `count`, containment with `<@`, or `ALL(...)`-style comparison shapes.
- [x] (2026-03-14 12:34Z) Created the repo-local governing ExecPlan in `plans/api-feature-reverification.md`.
- [x] (2026-03-14 12:41Z) Reviewed artifact governance with the user and selected `plans/api-feature-reverification.md` as the single governing ExecPlan. The older external megaplan is superseded and no longer authoritative.
- [x] (2026-03-14 12:43Z) Reviewed binding selector scope with the user. Confirmed that `at: :first` and `at: :last` should remain in future completion scope as additive aliases, while integer positional bindings remain the canonical live contract.
- [ ] Continue section-by-section review of this governing plan with the user and correct any inaccurate claims before implementation approval.

## Milestones

### Milestone 1: Make the live public boundary explicit

The first milestone is to turn the audit into a reliable picture of the current system. At the end of this milestone, a reader should know where public filter behavior enters the system, where the routing decisions happen, which modules own the directive and expression families, and which parts of the behavior are already proved by public tests.

This milestone is complete when the module specifications, function specifications, context, and internal walkthrough below are accurate enough that a reader can trace a filter from `Actions.all/3` or `CommonFilters.convert_params_to_filter/3` to the owning runtime module without re-discovering the architecture.

### Milestone 2: Separate runtime gaps from proof gaps and future compatibility work

The second milestone is to classify the audit results into three buckets that will drive later implementation. A runtime gap is behavior that the live public path does not implement. A proof gap is behavior that the runtime appears to implement but that the public tests do not prove. Future compatibility work is behavior that is not live today but that may still be desirable because the user wants a broader contract than the current runtime offers.

This milestone is complete when each audited family is classified that way and the later plan of work names only the work that remains justified after the live audit.

### Milestone 3: Prepare a safe implementation plan and stop

The third milestone is to make later implementation safe without starting it. At the end of this milestone, a contributor should know which files will be touched, which public behaviors must remain unchanged, which examples define the missing behavior, and which validation steps will be required after implementation. Then work stops until the user explicitly authorizes implementation.

This milestone is complete when the later `Plan of Work`, example mapping, behaviour specifications, executable tests, and validation matrix are specific enough to guide implementation directly.

## Surprises & Discoveries

- Observation: The join family is broader than the earlier research-first plan assumed.
  Evidence: `lib/ecto_shorts/common_filters/join.ex` accepts `association`, `schema`, `table`, `query`, `subquery`, and `fragment`; `test/ecto_shorts/common_filters_test.exs` publicly proves those source kinds and association shorthand.

- Observation: Join hints appear to be implemented in runtime code but are not publicly proved.
  Evidence: `lib/ecto_shorts/common_filters/join.ex` compiles hint-aware join clauses from configured hints, but the public tests do not exercise `hints:`.

- Observation: The lock family already supports callback-style customization through the provider path.
  Evidence: `lib/ecto_shorts/common_filters/lock.ex` accepts provider results shaped as `{:ok, fn query -> query end}`; `test/ecto_shorts/common_filters_test.exs` proves provider-backed lock behavior.

- Observation: Quantified `any` is already in the runtime path even though the public tests only prove `all`.
  Evidence: `lib/ecto_shorts/dynamics/postgres.ex` normalizes both `:all` and `:any`; `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` generates comparison branches for both quantifiers.

- Observation: The live string-matching contract always wraps patterns for both scalar and array paths.
  Evidence: `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` wraps scalar `like` and `ilike` values and list entries with `%...%`; `lib/ecto_shorts/dynamics/postgres/array_expr.ex` does the same through `normalize_patterns/1`.

- Observation: Some missing array behaviors exist only as example Ecto queries, not as active API support.
  Evidence: `examples/ecto_query_dsl.exs` contains example queries for array `count`, containment using `<@`, and `ALL(...)`-style comparisons; the active runtime owner `lib/ecto_shorts/dynamics/postgres/array_expr.ex` does not implement those shapes.

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

- Decision: Keep planning and implementation authorization separate.
  Rationale: The current task is still planning-only. Even though the next implementation steps are now better defined, later code work still requires explicit user approval.
  Date/Author: 2026-03-14 / Cascade

## Outcomes & Retrospective

The old research-first megaplan overstated the amount of missing runtime behavior. The live-first audit reduced the true runtime work to a smaller set of gaps and turned several earlier “missing” items into proof work instead.

At this stage, the task has not produced code changes. The concrete outcome so far is a better-governed planning state: `plans/api-feature-reverification.md` is the single governing ExecPlan for this task, the older external megaplan is superseded, and the later implementation work can be limited to real runtime gaps plus a small number of explicit compatibility choices.

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

The live contract currently covers list equality and inequality, scalar membership, overlap via `in` with a list, element-wise comparisons using `ANY`, `lower` and `upper` transforms, and array `like` and `ilike`. The live contract does not currently document or prove array `nil`, `count`, containment with `<@`, or `ALL(...)`-style comparison shapes.

### `EctoShorts.CommonFilters.Join`

`Join` owns join directive payloads after `CommonFilters.API` dispatches to it. It accepts join params, resolves source kinds, builds the `on` expression, and applies the join to the query.

The live contract supports association, schema, table, query, subquery, and fragment sources. It uses `qualifier` as the runtime key for join type. It also contains hint-aware runtime branches. The live contract does not currently include a `type` compatibility alias.

### `EctoShorts.CommonFilters.Lock`

`Lock` owns lock directive payloads after API dispatch. The live contract expects a map or keyword payload with a `name` key. Built-in names `:for_update` and `:for_share` are handled directly. Other names are resolved through the query provider, which must return `{:ok, function}`, `{:error, reason}`, or `nil`.

The live contract includes provider-backed callback customization. It does not currently expose a direct raw function payload at the public boundary.

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
- `bind`, `as`, and `at` change the selected binding for nested filter reduction
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

- Input at the public boundary: `%{bind: %{at: 1, published: true}}`
- Handoff into `apply_bind_filters/6`: selected binding becomes `{:at, 1}` and remaining entries continue under that binding
- Resulting downstream responsibility: the eventual field predicate for `published` is built against that positional binding

### Boundary: `CommonFilters.apply_filters/6` to `CommonFilters.API.build_query/6`

Accepted input shape: a current filter context, source, current query, selected binding, a filter key, a term, and opts.

Produced output shape: a next `Ecto.Query`.

Owned transformations:

- choose whether the key is `bind`, a binding operator, a predicate-group member, a post-aggregate-group member, an association traversal, a registered API filter, or a fallback field predicate

Forbidden accidental contract expansion:

- `apply_filters/6` must not become the place where join payload aliases, lock payload aliases, or array-only shapes are silently normalized. Those responsibilities belong in the owning directive or dynamic-expression boundary.

### Boundary: `Postgres.build_dynamic/4` to `build_expr/5`

Accepted input shape: source, selected binding, `{field_key, term}`, and opts.

Produced output shape: an `Ecto.Query.DynamicExpr` or `nil` when the selected binding or downstream term is unsupported.

Owned transformations:

- normalize top-level negation from `{:not, term}` into `{negated, term}`
- normalize top-level quantified forms `{:all, payload}` and `{:any, payload}` into equality against a built quantified query
- route by operator family or array-field detection

Forbidden accidental contract expansion:

- array-only enhancements such as `count` or `all` must not be added here as incidental intermediate shapes; they belong in `ArrayExpr` once the term reaches the array boundary

### Boundary: `ArrayExpr.dynamic_expr/5`

Accepted live shapes today:

- `{:==, list}` and `{:!=, list}` for exact array equality and inequality
- `{:==, value}` and `{:!=, value}` for membership and non-membership
- `{:in, list}` for overlap and `{:in, value}` for scalar membership
- `{:>, value}`, `{:>=, value}`, `{:<, value}`, `{:<=, value}` for `ANY` comparisons
- `{:==, {:lower, value}}`, `{:!=, {:lower, value}}`, and upper-case counterparts
- `{:like, value}` and `{:ilike, value}` for scalar or list pattern search after local normalization

Produced output shape: a dynamic expression or `nil`.

Unsupported live shapes that matter for later work:

- `nil`
- nested `count` payloads
- nested `all` payloads
- containment fragments such as `<@`

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

Unsupported live compatibility shape that matters:

- `type:` as an alias for `qualifier:`

### Boundary: `Lock.build_query/6`

Accepted live shape today:

- map or keyword payload with `name:` and optional `values:`

Produced output shape: an `Ecto.Query` with a lock clause or the unchanged query when the provider path returns `nil`, `{:error, reason}`, or an invalid callback shape.

Unsupported live compatibility shape that matters:

- direct raw function payload without a `name:` indirection

### Boundary: `WithCte.build_query/6`

Accepted live shape today:

- `with_cte: [cte_name: [as: query_or_subquery_or_filter_params, materialized: boolean_or_nil]]`

Produced output shape: an `Ecto.Query` with one or more CTEs applied.

Unsupported live compatibility shape that matters:

- `operation:` passthrough

## Internal Structure Walkthrough

A typical public integration call begins at `EctoShorts.Actions.all/3`. That action hands filter params to `EctoShorts.CommonFilters.convert_params_to_filter/3`, which starts with `CommonSchema.to_query(source)` and reduces each filter entry from the root selected binding `{:as, nil}`.

For each entry, `apply_filters/6` decides what kind of thing it is looking at. If the key is `bind`, `as`, or `at`, the function changes the selected binding and continues reducing nested entries. If the key belongs to a filter group such as predicate or post-aggregate filters, the reducer either keeps descending or dispatches to the registered filter owner. If the key names an association and the term shape looks reducible, `CommonFilters` first ensures a named association binding through `with_named_binding`, then continues reduction under `{:as, association_name}`. Otherwise, the reducer dispatches directly to `CommonFilters.API.build_query/6`.

When a field predicate reaches the Postgres dynamic path, `EctoShorts.Dynamics.Postgres.build_dynamic/4` normalizes top-level negation, then normalizes top-level quantified forms such as `all` and `any` into equality against a built quantified query. After that, it routes by family. Common operators such as `before` and `after` stay in `CommonExpr`. Array and map-backed fields route to `ArrayExpr`. Everything else routes to `ScalarExpr`.

That routing order matters for later implementation. If array `count` or array `all` support is added, the change belongs in `ArrayExpr.dynamic_expr/5` after the field is already known to be array-like. If a join payload alias or lock payload alias is added, the change belongs in the directive owner after API dispatch, not in `CommonFilters.apply_filters/6`. If a top-level quantified shorthand is extended, the change belongs in `Postgres.normalize_quantified_term/3` or the quantified-query helper, not in the action layer.

Invalid-input behavior also differs by boundary. At the top-level reducer, many invalid shapes are still allowed to flow to the owner module. The owner module usually decides whether to log and keep the query unchanged or to raise for impossible internal shapes. That means later proof work must keep the invalid-input behavior visible at the owning boundary instead of hiding it behind broad guards in `CommonFilters`.

## Example Mappings

### Story: Binding selector aliases as future additive support over the current integer-only contract

The public filter pipeline already supports root binding, named binding, and positive-integer positional binding. Later completion work should add `:first` and `:last` as additive aliases without disturbing the current integer-based contract.

#### Rules:

- The live public path must continue to accept `as: alias_name`, `as: nil`, and `at: positive_integer`.
- Omitting binding selection must continue to mean “use the current binding context.”
- `at: :first` and `at: :last` are not part of the live contract today, but they remain in future completion scope as additive aliases.
- Later alias support must normalize to the existing integer-based downstream contract instead of changing the compiler and dynamic builders to accept multiple internal positional representations.

#### Examples:

`CommonFilters.convert_params_to_filter(Post, %{at: %{2 => %{preload: :author}}}, [])`
`#=> returns a query whose positional binding 2 is used for the nested preload behavior`

`CommonFilters.convert_params_to_filter(Post, %{bind: %{at: 1, published: true}}, [])`
`#=> returns a query that applies the published predicate against positional binding 1`

`CommonFilters.convert_params_to_filter(Post, %{at: %{first: %{published: true}}}, [])`
`#=> not part of the live contract today; later implementation would need to define whether this becomes a supported alias or remains unsupported`

#### Open Questions:

- **Q:** Is `:first` already live in the current public path? **A:** No. The live validator and compiler contracts accept only positive integers for `{:at, position}`.
- **Q:** Should alias support remain in future scope? **A:** Yes. The plan keeps `at: :first` and `at: :last` as additive compatibility work.
- **Q:** Where should alias normalization happen? **A:** At the public filter boundary before the integer-based downstream binding contracts are used.

### Story: Array feature completion after the live audit

The live array path already supports several behaviors, but the audit confirmed that some research-era array features are not active runtime behavior today. Later implementation must add only the missing shapes while preserving the live ones.

#### Rules:

- Existing live array equality, inequality, membership, overlap, `ANY` comparison, transform, and string-matching behavior must remain unchanged.
- Omitted array-only operators must continue to follow the current normalization rules unless later implementation explicitly changes them.
- Invalid or unsupported array payload shapes must not be silently treated as supported behavior.
- Later implementation may add array `nil`, `count`, and `all` support, but those behaviors are not part of the live contract today.

#### Examples:

`Actions.all(Post, %{tags: ["elixir", "erlang"]})`
`#=> returns posts whose tags field exactly equals that list`

`Actions.all(Post, %{tags: %{>: "a"}})`
`#=> returns posts where any array element is greater than "a"`

`Actions.all(Post, %{tags: %{count: %{>: 0}}})`
`#=> not part of the live contract today; later implementation would need to define and prove it`

#### Open Questions:

- **Q:** Does the active runtime owner already contain array `count` support? **A:** No. The active `ArrayExpr` implementation does not contain `count` branches.
- **Q:** Do example queries in `examples/ecto_query_dsl.exs` prove the live API supports those shapes? **A:** No. They show desired Ecto queries, not active `CommonFilters` or `Actions` support.

### Story: Runtime support versus proof support for quantified comparisons

The quantified comparison family already contains more runtime behavior than the earlier plan assumed. The later work should prove the live path before changing runtime code.

#### Rules:

- The live runtime path must continue to support quantified `all`.
- The live runtime path currently contains `any` support and should not be removed or rewritten unless proof work exposes a real defect.
- Omitting a select override in quantified query building must continue to default to the outer field.
- Invalid quantified payload shapes should continue to follow the current helper behavior instead of inventing a new failure model during proof work.

#### Examples:

`CommonFilters.convert_params_to_filter(Post, %{id: %{all: %{from: Comment, where: %{published: true}}}}, [])`
`#=> returns a query with equality against all(selected comment ids)`

`CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{all: %{from: Comment, where: %{published: true}}}}}, [])`
`#=> returns a query with the quantified equality wrapped in not`

`CommonFilters.convert_params_to_filter(Post, %{id: %{any: %{from: Comment, where: %{published: true}}}}, [])`
`#=> intended to be live through the quantified runtime path, but public proof still needs to be added`

#### Open Questions:

- **Q:** Is `any` a runtime gap or a proof gap? **A:** A proof gap, unless later targeted tests expose a real defect.
- **Q:** Should later work change runtime code before the proof gap is tested? **A:** No. Add public proof first and patch runtime only if the proof exposes a defect.

### Story: Directive compatibility and preserved live behavior

Later completion work may add compatibility aliases and passthroughs for directives, but the live directive contracts are narrower than the older research-first megaplan assumed.

#### Rules:

- Existing join `qualifier:` behavior must remain canonical internally.
- Existing provider-backed lock behavior must remain unchanged.
- Existing `with_cte` support for `as:` and optional `materialized:` must remain unchanged.
- Invalid directive payloads must continue to follow the current owner-specific logging and unchanged-query behavior unless later implementation deliberately changes that contract.

#### Examples:

`CommonFilters.convert_params_to_filter(Post, %{join: [schema: [source: User, as: :user, on: %{author_id: 1}]]}, [])`
`#=> returns a query with the schema join applied`

`CommonFilters.convert_params_to_filter(Post, %{lock: %{name: :provider_for_update}}, query_provider: EctoShorts.TestQueryProvider)`
`#=> returns a query with a provider-backed FOR UPDATE lock`

`CommonFilters.convert_params_to_filter(Post, %{with_cte: [published_posts: [as: [published: true], materialized: false]]}, [])`
`#=> returns a query with the named CTE applied and materialized false`

`CommonFilters.convert_params_to_filter(Post, %{join: [schema: [source: User, type: :left, as: :user, on: %{author_id: 1}]]}, [])`
`#=> not part of the live contract today; later implementation would need to add a compatibility alias explicitly`

#### Open Questions:

- **Q:** Is join `type:` already supported under another live key? **A:** No. The live code reads `qualifier:`.
- **Q:** Does the lock family already support direct raw function payloads? **A:** No. The live contract requires `name:` and uses the provider callback path for custom lock functions.

## Behaviour Specifications

### Feature: Preserve the verified live contract while narrowing future completion work

Scenario: Binding selectors remain on the verified live contract
  Given the current filter pipeline
  When a caller uses root binding, named binding, or positive-integer positional binding
  Then the plan must treat that behavior as already implemented
  And the plan must not classify `:first` or `:last` aliases as already live

Scenario: Quantified `any` is treated as live until disproved
  Given the current Postgres quantified routing code
  When the plan classifies set-comparison work
  Then `any` must be listed as implemented in runtime
  And it must be listed as needing public proof rather than immediate runtime implementation

Scenario: Array `count` is treated as missing runtime behavior
  Given the active `ArrayExpr` owner
  When the plan classifies array-family work
  Then array `count` must be listed as a runtime gap
  And existing array equality, membership, comparison, transform, and string-matching behavior must be listed as preserved

Scenario: Directive compatibility work preserves the current owner contracts
  Given the live `Join`, `Lock`, and `WithCte` modules
  When the plan describes future compatibility work
  Then it must preserve `qualifier:` as the canonical live join key
  And it must preserve provider-backed lock callbacks
  And it must preserve `with_cte` support for `as:` and `materialized:`

### Feature: Stop after the revised plan unless the user explicitly approves implementation

Scenario: Planning work ends at the plan artifact
  Given the revised governing ExecPlan
  When the planning milestone is complete
  Then the next action is to present the plan to the user
  And no code or test changes are authorized until the user explicitly approves implementation

## Executable Tests

Later implementation or proof work must use real ExUnit tests in this repository. The tests below are the required proof surfaces for later milestones.

For proof gaps:

- Add public tests for quantified `any` in `test/ecto_shorts/common_filters_scalar_filter_test.exs` and, if useful, the lower-level dynamic tests in `test/ecto_shorts/dynamics/postgres_test.exs` or `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`.
- Add public tests for join hints in `test/ecto_shorts/common_filters_test.exs`.
- Expand direct array-path proof in `test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs` so the supported live array contract is proved more explicitly.

For runtime gaps and compatibility work, if the user later approves implementation:

- Add boundary-visible tests for binding selector aliases in `test/ecto_shorts/common_filters_test.exs` and any lower-level compiler or dynamic tests needed only if the alias normalization touches those boundaries.
- Add array runtime tests in `test/ecto_shorts/actions/crud_test.exs` and `test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs` for `nil`, `count`, and `all` once those behaviors exist.
- Add directive tests in `test/ecto_shorts/common_filters_test.exs` for join `type:` alias support, direct raw lock function payload support, and `with_cte operation` support if those features are implemented.
- Add scalar and array string-matching tests only if the user still wants explicit wildcard preservation as a future compatibility change.

Each later test must prove one caller-visible claim. Passing tests should be described as evidence for the executed cases only, not as proof of correctness for all possible inputs.

## Validation and Acceptance

The planning artifact itself is acceptable when a reader can trace each audited family to one of three outcomes: already implemented, implemented but under-proved, or missing runtime behavior.

The future validation matrix for implementation work is:

- Claim: binding selector aliases are supported without breaking existing integer positional bindings.
  Boundary: `CommonFilters.convert_params_to_filter/3` plus any downstream binding contract touched by the implementation.
  Proof method: ExUnit boundary tests and targeted lower-level tests only if the alias normalization crosses those boundaries.
  Evidence command: `mix test test/ecto_shorts/common_filters_test.exs` and any smaller focused test files added for the touched compiler or dynamic boundary.
  Residual risk: if alias support depends on query-shape-specific last-binding detection, edge cases across unusual join counts may still need broader coverage.

- Claim: array `nil`, `count`, and `all` behaviors work without breaking the current live array contract.
  Boundary: `Actions.all/3` and `ArrayExpr.dynamic_expr/5`.
  Proof method: boundary integration tests plus direct array-expression tests.
  Evidence command: `mix test test/ecto_shorts/actions/crud_test.exs test/ecto_shorts/dynamics/postgres/array_expr/specs_test.exs`.
  Residual risk: behavior remains Postgres-specific and should not be overclaimed as adapter-agnostic.

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

If the user approves implementation later, perform the work in this order.

First, add proof-only changes before runtime changes wherever the live audit suggests that runtime behavior already exists. That means quantified `any`, join hints, and stronger direct array-expression proof come before runtime edits. If any proof-only test exposes a real defect, update this plan’s `Progress`, `Decision Log`, and the affected contracts before changing code.

Second, implement small compatibility shims where the desired future behavior is additive and can normalize back to the existing canonical live contract. Binding selector aliases belong in this category. Join `type:` alias support also belongs here if the user later keeps that item in scope. The key safety rule is that the public boundary may accept a broader input shape, but the downstream internal boundary should stay on the existing canonical representation whenever possible.

Third, implement the true runtime gaps in the smallest owner modules possible. Array `nil`, array `count`, and array `all` belong in `lib/ecto_shorts/dynamics/postgres/array_expr.ex`. Direct raw lock function payload support belongs in `lib/ecto_shorts/common_filters/lock.ex`. `with_cte operation` belongs in `lib/ecto_shorts/common_filters/with_cte.ex`. These changes must preserve the current live behaviors already proved by tests.

Fourth, handle the string-matching contract only if the user still wants that behavior change after the live audit. If implemented, the work belongs in `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` and `lib/ecto_shorts/dynamics/postgres/array_expr.ex`. The preserved behavior is that bare strings keep the current contains-style convenience. The new compatibility behavior would be explicit wildcard preservation for callers who pass patterns that already include wildcards.

After each implementation slice, update this plan, run the named focused tests, and record the result precisely as evidence for the executed cases.

## Concrete Steps

When later implementation is authorized, work from the repository root `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

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

Revision note (2026-03-14 12:34Z): created this repo-local governing ExecPlan because the earlier megaplan lived outside the repository and no longer matched the current planning rules. Rebased the plan on the live-first audit and narrowed the later implementation scope to true runtime gaps, proof gaps, and explicit compatibility choices.

Revision note (2026-03-14 12:41Z): reviewed artifact governance with the user and kept `plans/api-feature-reverification.md` as the single governing ExecPlan. The older external megaplan is superseded and no longer authoritative.

Revision note (2026-03-14 12:43Z): reviewed binding selector scope with the user and kept `at: :first` and `at: :last` in future completion scope as additive compatibility aliases. The plan now treats that as settled future scope rather than an open preference question.
