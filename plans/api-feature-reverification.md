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
- [x] (2026-03-14 15:48Z) Completed the first proof-only slice without runtime edits. Added public `CommonFilters` tests for quantified `any`, added public join-hint coverage for the configured `:test_index` hint key, expanded `test/ecto_shorts/dynamics/postgres/array_expr_test.exs` with direct array-expression proof, and ran `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/common_filters_test.exs test/ecto_shorts/dynamics/postgres/array_expr_test.exs` plus `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/actions/crud_test.exs`, both passing with no reclassification needed.
- [x] (2026-03-14 15:49Z) Completed the additive binding-selector alias slice at the public boundary. Added `at: :first` and `at: :last` resolution in `CommonFilters` only, kept downstream contracts on integer `{:at, position}` selectors, added boundary-visible proof in `test/ecto_shorts/common_filters_test.exs`, and ran `mix test test/ecto_shorts/common_filters_test.exs` plus `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/actions/crud_test.exs`, both passing.
- [x] (2026-03-14 15:58Z) Performed the final pre-execution review required by the current repo rules before starting the first true runtime-gap slice. Found stale planning-only wording that was no longer safe after implementation approval, and found that the older array `all:` example in `examples/ecto_query_dsl.exs` used an inverted comparison fragment relative to the preserved live `ANY` operator semantics. Updated this plan so the next slice is singular in meaning and no longer depends on chat context to resolve those drifts.
- [x] (2026-03-14 16:18Z) Began the first true runtime-gap slice under TDD. Added and passed boundary-plus-direct proof for array `nil`, then added and passed boundary-plus-direct proof for array `count > 0` and array `count == 0` with the documented coalesced zero-length rule.
- [x] (2026-03-14 16:24Z) The new boundary test for `%{tags: %{all: %{>: "a"}}}` exposed a routing conflict rather than an `ArrayExpr`-only gap. `Postgres.normalize_quantified_term/3` currently rewrites every top-level `{all, payload}` into quantified-subquery handling, so array-local comparison `all:` payloads never reach `ArrayExpr`. Updated this plan before continuing so the remaining slice is singular in meaning again.
- [x] (2026-03-14 16:25Z) Completed the remaining comparison-operator array `all:` slice under the chosen payload-shape split. Narrowed `Postgres.normalize_quantified_term/3` so only quantified-query payloads with `:from` are rewritten into quantified-subquery handling, added `ArrayExpr` support for comparison-operator array-local `all:` payloads while preserving the live reversed operator meaning already used by `ANY`, and proved the executed cases at `Postgres.build_dynamic/4`, `ArrayExpr.dynamic_expr/5`, and `Actions.all/3`.
- [x] (2026-03-14 16:43Z) The user chose Approach 1 for the remaining array `all:` slice: narrow `Postgres.normalize_quantified_term/3` by quantified-query payload shape instead of changing router precedence. Refreshed nearby repo patterns plus current Elixir docs for `Keyword.has_key?/2`, `Map.has_key?/2`, and `is_map/1` before resuming code changes so the split could use plain helper logic rather than an unverified guard form.
- [x] (2026-03-14 16:52Z) Focused and broader proof passed for the completed array `all:` slice. Focused commands: `mix test test/ecto_shorts/dynamics/postgres_test.exs:86`, `mix test test/ecto_shorts/dynamics/postgres/array_expr_test.exs:65`, and `mix test test/ecto_shorts/actions/crud_test.exs:1082`, all passing. Broader neighboring regression: `mix test test/ecto_shorts/dynamics/postgres_test.exs test/ecto_shorts/dynamics/postgres/array_expr_test.exs test/ecto_shorts/actions/crud_test.exs`, passing with 153 tests and 0 failures.
- [x] (2026-03-14 17:53Z) Tightened the `with_cte operation:` boundary proof from `assert_query/2` to `assert_sql/3`. The stronger proof exposed a real owner gap: raw Ecto compiles `operation: :update_all` to an `UPDATE ... RETURNING` CTE, while the `CommonFilters.convert_params_to_filter/3` path still emits a plain `SELECT` CTE. The invalid `operation:` proof also now shows that the current owner treats invalid input like omitted `operation:` instead of rejecting it with an unchanged query.
- [x] (2026-03-14 18:01Z) Completed the `with_cte operation:` owner slice in `lib/ecto_shorts/common_filters/with_cte.ex`. Added explicit validation for `:all | :update_all | :delete_all`, forwarded valid operations into `Query.with_cte/3` alongside the existing `materialized:` path, preserved omitted-input behavior, restored unchanged-query plus warning behavior for invalid `operation:` payloads, and passed focused validation with `mix test test/ecto_shorts/common_filters_test.exs:1819`, `mix test test/ecto_shorts/common_filters_test.exs:1978`, and neighboring regression with `mix test test/ecto_shorts/common_filters_test.exs`.
- [x] (2026-03-14 18:24Z) Completed the containment-style array `all: [in: list]` slice. Refreshed stale plan wording after the completed `with_cte operation:` work, added failing proof at `ArrayExpr.dynamic_expr/5`, `Postgres.build_dynamic/4`, and `Actions.all/3`, confirmed the gap was owner-local in `ArrayExpr`, added the minimal `{:all, {:in, values}}` `<@` branch, and passed focused validation with `mix test test/ecto_shorts/dynamics/postgres/array_expr_test.exs:72`, `mix test test/ecto_shorts/dynamics/postgres_test.exs:100`, `mix test test/ecto_shorts/actions/crud_test.exs:1096`, plus neighboring regression with `mix test test/ecto_shorts/actions/crud_test.exs test/ecto_shorts/dynamics/postgres_test.exs test/ecto_shorts/dynamics/postgres/array_expr_test.exs` under the locally working OTP 27 toolchain.
- [x] (2026-03-14 18:19Z) Added focused failing proof for containment-style array `all: [in: list]` at `ArrayExpr.dynamic_expr/5`, `Postgres.build_dynamic/4`, and `Actions.all/3`. Under the working local OTP 27 toolchain, the direct and router proofs both returned `nil`, and the public boundary then failed with `expected a keyword list or dynamic expression in where, got: nil`, confirming the gap is owner-local in `ArrayExpr` rather than another Postgres routing conflict.
- [x] (2026-03-14 18:33Z) Completed the approved wildcard-preservation compatibility slice. Re-read the current rules and governing plan, refreshed authoritative Ecto `like/2` and `ilike/2` docs plus current Elixir `String.contains?/2` docs, traced the live public and owner boundaries through `CommonFilters.convert_params_to_filter/3`, `Postgres.build_dynamic/4`, `ScalarExpr.dynamic_expr/5`, `ScalarExprBuilder.quote_expr/3`, and `ArrayExpr.dynamic_expr/5`, added failing proof at the public SQL boundary, direct scalar owner, direct array owner, and `Actions.all/3` integration boundary, confirmed the gap was owner-local in scalar and array pattern construction, implemented `preserve_or_wrap_pattern/1` in `ScalarExprBuilder` and `ArrayExpr`, and passed focused validation with `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs:426 test/ecto_shorts/common_filters_scalar_filter_test.exs:467 test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs:235 test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs:271 test/ecto_shorts/dynamics/postgres/array_expr_test.exs:113 test/ecto_shorts/dynamics/postgres/array_expr_test.exs:149 test/ecto_shorts/actions/crud_test.exs:915 test/ecto_shorts/actions/crud_test.exs:1138`, plus neighboring regression with `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs test/ecto_shorts/dynamics/postgres/array_expr_test.exs test/ecto_shorts/actions/crud_test.exs` under the locally working OTP 27 toolchain.
- [ ] (2026-03-14 19:05Z) Began the newly approved deferred follow-on scope under the same governing plan. Re-read the current rules, re-checked the governing artifact against the current standard, and traced the remaining deferred candidates through live code, current tests, old examples, and authoritative Ecto docs. Confirmed the remaining deferred work is now three concrete slices: broader quantified comparison support at the `CommonFilters` boundary, explicit join-payload `type:` source-family support replacing the earlier planned `kind:` name while preserving outer-key joins and `qualifier:` join mode, and deliberate lock-boundary widening for direct raw function and direct raw string payloads at `Lock.build_query/6`.
- [x] (2026-03-14 20:21Z) Completed the explicit join-payload `type:` source-family slice. Repaired the earlier `type:`-as-`qualifier:` drift, restored association shorthand to its separate outer-key association path, added explicit `type: :association` and `type: :schema` proof in `test/ecto_shorts/common_filters_test.exs`, and passed focused validation with `mix test test/ecto_shorts/common_filters_test.exs:507`, `mix test test/ecto_shorts/common_filters_test.exs:551`, plus nearby existing join proofs at `:473`, `:490`, and `:525`. A broader `mix test test/ecto_shorts/common_filters_test.exs` run still fails only on the already-pending direct raw lock proofs, which remains evidence for the separate lock slice rather than a join regression.
- [x] (2026-03-14 20:32Z) Completed the broader quantified comparison slice. Extended `Postgres.normalize_quantified_term/3` to normalize operator-wrapped quantified-query payloads and extended the generic scalar comparison builder branch so `>`, `>=`, `<`, and `<=` emit quantified `all(...)` / `any(...)` expressions instead of pinning quantified tuples as literal values. Focused and neighboring validation passed with `mix test test/ecto_shorts/dynamics/postgres_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs` and `mix test test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs test/ecto_shorts/dynamics/postgres_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs`.

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
  Evidence: `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/common_filters_test.exs test/ecto_shorts/dynamics/postgres/array_expr_test.exs` passed with 268 tests and 0 failures; `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/actions/crud_test.exs` passed with 391 tests and 0 failures.

- Observation: The binding-alias slice could stay entirely at the public boundary because the repo already has a live query-binding count helper.
  Evidence: `lib/ecto_shorts/common_query.ex` exposes `query_binding_count/1`; `lib/ecto_shorts/common_filters.ex` now resolves `at: :first` to positional binding `1` and `at: :last` to `CommonQuery.query_binding_count(query)` before anything reaches the integer-only downstream compiler and dynamic contracts; `mix test test/ecto_shorts/common_filters_test.exs` passed with 180 tests and 0 failures.

- Observation: The old array `all:` example query is inverted relative to the preserved operator meaning already used on the live `ANY` array path.
  Evidence: `lib/ecto_shorts/dynamics/postgres/array_expr.ex` currently treats `{:>, value}` as “some array element is greater than value” by compiling `fragment("? < ANY(?)", ^value, field(...))`; the older example block in `examples/ecto_query_dsl.exs` labeled `tags: [all: [>: "a"]]` shows `fragment("? > ALL(?)", ^"a", p.tags)`, which instead means every array element is less than `"a"`. The next runtime slice must preserve the live caller-visible operator meaning and therefore use the analogous reversed comparison for `ALL`.

- Observation: Array-local comparison `all:` payloads currently collide with quantified subquery `all` routing before they can reach the array owner.
  Evidence: `test/ecto_shorts/actions/crud_test.exs:1082` fails for `%{tags: %{all: %{>: "a"}}}` with `** (KeyError) key :from not found in: [>: "a"]`; `lib/ecto_shorts/dynamics/postgres.ex` rewrites every `{all, payload}` through `normalize_quantified_term/3`; `lib/ecto_shorts/common_filters/set_comparison.ex` then requires `:from`, proving that array-local `all:` currently cannot share the same top-level routing path.

- Observation: `assert_query/2` was too weak to prove `with_cte` option handling because the relevant `Ecto.Query` inspect output did not distinguish the CTE operation metadata.
  Evidence: `test/ecto_shorts/common_filters_test.exs:1819` initially passed while `lib/ecto_shorts/common_filters/with_cte.ex` still ignored `:operation`; after switching the `with_cte operation:` proofs to `assert_sql/3`, the positive case failed with `UPDATE ... RETURNING` SQL on the raw-Ecto side versus `SELECT ...` SQL on the `CommonFilters` side, and the invalid case failed because the current owner still built a default `SELECT` CTE instead of leaving the outer query unchanged.

- Observation: After the `with_cte operation:` slice landed, the governing plan still described that directive option as future scope, so the next broader completion slice must start with plan repair before implementation continues.
  Evidence: `plans/api-feature-reverification.md` still said `` `with_cte operation:` is not part of the live contract today `` in the directive compatibility rules and examples even after `lib/ecto_shorts/common_filters/with_cte.ex` and `test/ecto_shorts/common_filters_test.exs` were updated and the focused proofs passed.

- Observation: The containment-style `all: [in: list]` slice does not need another Postgres routing change after the earlier quantified-payload split; the live router already preserves the array-local payload and the missing behavior is only in `ArrayExpr`.
  Evidence: `test/ecto_shorts/dynamics/postgres_test.exs:100` failed with `right: nil` rather than a quantified-subquery crash, and `test/ecto_shorts/actions/crud_test.exs:1096` then failed because `Actions.all/3` received `nil` from `where`. `test/ecto_shorts/dynamics/postgres/array_expr_test.exs:72` showed the direct owner result is also `nil`, proving the missing branch is local to `ArrayExpr.dynamic_expr/5`.

- Observation: Wildcard-preservation compatibility required a stronger public proof boundary than `assert_sql/3`, and the resulting failures showed the runtime gap was owner-local in scalar and array pattern construction.
  Evidence: `lib/ecto_shorts/testing.ex:320` through `:330` shows `assert_sql/3` compares only the generated SQL string, so the stronger `CommonFilters` wildcard proofs switched to full `Ecto.Adapters.SQL.to_sql/3` tuple equality to expose `["hello%"]` versus `["%hello%%"]`; `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs:235` and `:271` plus `test/ecto_shorts/dynamics/postgres/array_expr_test.exs:113` and `:149` failed directly on wrapped patterns; after the owner-local helpers landed, those boundaries and the public `Actions.all/3` wildcard tests passed without router or reducer changes.

- Observation: Broader quantified comparison shapes from the older examples are still a real public-boundary gap, not just an unproved lower-level capability.
  Evidence: `examples/ecto_query_dsl.exs:1853` through `:2149` still express public shapes such as `%{id: %{>: %{all: %{from: Comment, body: "Hello"}}}}`; `lib/ecto_shorts/dynamics/postgres.ex:140` through `:149` currently rewrites only top-level `{:all, payload}` and `{:any, payload}` into quantified-query handling; `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex:42` through `:57` then forwards comparison payloads like `{:>, %{all: %{from: ...}}}` unchanged into the scalar compiled comparison path, so those broader public forms do not yet reach the existing lower-level quantified branches.

- Observation: The remaining deferred directive work is still local to the directive owners, but the join slice is a contract correction, not a `type:`-as-`qualifier:` alias.
  Evidence: `lib/ecto_shorts/common_filters/join.ex:53` through `:71` currently selects join source family from the outer join key and `:146` through `:158` uses `qualifier:` for join mode; the governing plan at `:713` already records that `type` refers to source family in the intended terminology, not join mode; there are no live repo uses of `:kind`; `lib/ecto_shorts/common_filters/lock.ex:22` through `:31` still accepts only map/keyword `name:` payloads, while authoritative Ecto docs for `lock/3` confirm direct raw string lock expressions are technically viable.

- Observation: Association shorthand is a separate public surface from explicit join payloads and should not be used to overload the new `type:` source-family selector.
  Evidence: `lib/ecto_shorts/common_filters.ex:54` through `:59` routes top-level association keys through `ensure_association_binding/5` before nested reduction, so shorthand already fixes the source family to association. The approved `type:` change therefore belongs on the explicit `join:` payload surface, while shorthand should continue to derive association source from the outer key and preserve `qualifier:` as the join-mode key if join control keys are allowed there.

- Observation: The explicit join-payload `type:` slice validates cleanly in focused and nearby join proofs, and the only broader-file failures remain the known direct-lock proofs.
  Evidence: `mix test test/ecto_shorts/common_filters_test.exs:507`, `:551`, `:473`, `:490`, and `:525` all passed after the runtime and test updates. `mix test test/ecto_shorts/common_filters_test.exs` still fails only at `test/ecto_shorts/common_filters_test.exs:3021` and `:3034`, where the existing direct raw string and direct raw function lock proofs continue to fail with `Expected lock ..., got: ...`, showing the remaining blocker is still the separate `Lock.build_query/6` slice.

- Observation: Broader quantified comparison support required a second owner-local change in the scalar comparison builder after the router patch landed.
  Evidence: After extending `lib/ecto_shorts/dynamics/postgres.ex` to rewrite operator-wrapped quantified-query payloads, the first focused proof still failed with `Ecto.Query.CastError` because the generated scalar comparison module emitted `field(q, ^key) > ^{:all, query}`-style clauses. Extending the generic comparison branch in `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` to match `{:all, query}` and `{:any, query}` for non-equality operators resolved that remaining owner-local gap, and the focused plus neighboring quantified tests then passed.

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

- Decision: Treat `with_cte operation:` as part of the live directive contract after the completed owner slice.
  Rationale: The `WithCte` owner now validates and forwards `:operation`, and the focused SQL-based proofs confirm both valid-operation behavior and invalid-operation rejection at the public boundary. The governing plan must now describe that behavior as live so later work does not re-open it as future scope.
  Date/Author: 2026-03-14 / Cascade

- Decision: Implement `with_cte operation:` in the `WithCte` owner with explicit validation and explicit `Query.with_cte/3` option assembly rather than a broader `CommonFilters` rewrite or a generic option passthrough.
  Rationale: The current public path already routes `with_cte` through `CommonFilters.convert_params_to_filter/3` into `API.build_query(:with_cte, ...)` and then into `WithCte.build_query/6`. The narrowest valid change is to keep that path intact, validate `:operation` beside the existing `:materialized` handling inside `WithCte`, and extend `apply_cte` to emit the doc-backed Ecto options directly. This preserves existing `:as` and `:materialized` behavior while adding only the missing owner-local contract.
  Date/Author: 2026-03-14 / Cascade

- Decision: Use compiled SQL equality, not `Ecto.Query` inspect equality, as the public proof boundary for `with_cte operation:`.
  Rationale: The initial `assert_query/2` proof passed even while the owner still ignored `:operation`. Compiled SQL is the nearest stable observable boundary that reveals whether `Query.with_cte/3` received `operation:` as intended and whether invalid input still mutates the outer query.
  Date/Author: 2026-03-14 / Cascade

- Decision: Implement containment-style array `all: [in: list]` directly in `ArrayExpr` without another `Postgres.normalize_quantified_term/3` change.
  Rationale: The earlier quantified-payload split already leaves array-local `all` payloads in place unless they contain `:from`. The new failing proofs show the router now passes containment payloads through unchanged and the owner simply lacks a containment branch. The narrowest valid fix is therefore an owner-local `{:all, {:in, values}}` branch that maps to PostgreSQL `<@` while preserving the existing router and quantified-subquery behavior.
  Date/Author: 2026-03-14 / Cascade

- Decision: Extend the string-matching contract additively by preserving caller-supplied `%` and `_` wildcard patterns while keeping the current contains-style wrapping for bare strings and bare list entries.
  Rationale: Ecto `like/2` and `ilike/2` already accept raw search patterns, and the user explicitly approved this compatibility extension. Always preserving raw input would break the current live convenience contract for non-pattern values, while adding a new public flag or payload shape would widen the API unnecessarily. The approved contract therefore remains: bare values still become `%value%`, but values that already include wildcard markers pass through unchanged.
  Date/Author: 2026-03-14 / Cascade

- Decision: Implement wildcard preservation at the owner-local pattern-construction points in `ScalarExprBuilder` and `ArrayExpr`, not in `CommonFilters` or the Postgres router.
  Rationale: The public reducer and dynamic router should stay unaware of string-pattern heuristics. The current wrapping already lives in `ScalarExprBuilder.quote_expr/3` for scalar strings and in `ArrayExpr.normalize_patterns/1` for array strings. A small owner-local helper at those exact points is the narrowest valid change and keeps the rest of the filter pipeline unchanged.
  Date/Author: 2026-03-14 / Cascade

- Decision: Keep planning and implementation authorization separate.
  Rationale: Even while implementation is active, each new behavior-bearing slice still requires explicit user approval before repo changes. The wildcard-preservation work in this slice proceeded only after that explicit approval and remained governed by the refreshed ExecPlan throughout proof, implementation, and validation.
  Date/Author: 2026-03-14 / Cascade

- Decision: Continue the same governing ExecPlan for the newly approved deferred follow-on scope instead of creating a second plan.
  Rationale: The user explicitly asked to pick up the previously deferred items, and those items remain inside the same audited public feature families already governed here: quantified scalar comparisons plus join and lock directives. Updating the existing governing artifact keeps planning authority singular and preserves the earlier audit and implementation evidence as context for the follow-on slices.
  Date/Author: 2026-03-14 / Cascade

- Decision: Execute the remaining deferred work in this order: broader quantified comparisons first, then explicit join-payload `type:` source-family support, then direct raw lock payload compatibility.
  Rationale: Broader quantified comparisons are the only remaining deferred item that current code and examples show as a real public-boundary runtime gap; the join and lock slices are additive owner-local compatibility work that can safely follow once the quantified runtime/public proof boundary is settled.
  Date/Author: 2026-03-14 / Cascade

- Decision: Implement broader quantified comparison support in `Postgres.build_dynamic/4` by extending quantified normalization to comparison payloads whose right-hand side is a quantified-query payload.
  Rationale: The existing lower-level scalar compiled builders already support comparison operators with quantified right-hand sides once the term reaches them as `{op, {quantifier, query}}`. The missing piece is the public/router normalization for shapes like `%{id: %{>: %{all: %{from: Comment}}}}`. Extending quantified normalization at the Postgres routing boundary is the narrowest place that already owns quantified-query construction through `SetComparison.build_quantified_query/3`, and it avoids pushing quantified-query awareness down into the compiled scalar builders.
  Date/Author: 2026-03-14 / Cascade

- Decision: Implement `type:` only as the explicit join-payload source-family selector, replacing the earlier planned `kind:` name while preserving the existing outer-key join shapes and keeping `qualifier:` as the join-mode key.
  Rationale: The governing plan already settled that `type` refers to join source family in this feature family. There are no live repo uses of `:kind`, so this slice is not a runtime rename but a new explicit-join payload shape. The narrowest correct change is to add a single explicit payload form such as `%{join: [type: :association, source: :author, as: :author]}` at the `Join` boundary, remove the mistaken `type:`-as-`qualifier:` drift, and leave association shorthand on its existing outer-key association path.
  Date/Author: 2026-03-14 / Cascade

- Decision: Implement broader quantified comparison support in two narrow places: the Postgres router for operator-wrapped quantified-query payloads and the generic scalar comparison builder branch for non-equality quantified expressions.
  Rationale: Extending `Postgres.normalize_quantified_term/3` was still the correct public-routing fix for shapes like `%{id: %{>: %{all: %{from: Comment}}}}`, but the first focused failure showed that the generic comparison branch in `ScalarExprBuilder.comparison_conditions/5` still treated quantified tuples as ordinary pinned values for `>`, `>=`, `<`, and `<=`. The narrowest correct repair is therefore split across those two existing owners: the router builds quantified subqueries, and the scalar comparison builder emits quantified comparison expressions for the non-equality operator family.
  Date/Author: 2026-03-14 / Cascade

- Decision: Widen the lock boundary only to direct raw string payloads and direct unary function payloads, while preserving the existing `name:` + provider path unchanged.
  Rationale: Authoritative Ecto docs confirm direct raw string lock expressions are valid, and the old repo examples show direct unary function payloads that return a query with the desired lock applied. These two additions cover the deferred lock shapes already evidenced in repo artifacts without broadening the boundary to arbitrary new payload types or weakening the existing provider-backed path.
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

The next slice began as the first true array runtime-gap slice. Array `nil`, array `count > 0`, array `count == 0`, comparison-operator `all:`, and containment-style `all: [in: list]` are now implemented with focused direct, router, and boundary evidence. The later wildcard-preservation compatibility extension for string matching is also now implemented and proved.

With those originally approved slices complete, the user has now expanded the task to consume the previously deferred follow-on items instead of stopping at the earlier feature-complete checkpoint. That follow-on scope is narrower than reopening the whole audit: it is limited to broader quantified comparisons at the public boundary plus the two deferred directive compatibility slices for join `type:` and direct raw lock payloads.

The explicit join-payload `type:` source-family slice is now complete. `Join.build_query/6` accepts the new explicit single-join payload such as `%{join: [type: :association, source: :author, as: :author, qualifier: :left, on: true]}` by normalizing it to the existing outer-key join path, `qualifier:` remains the canonical join-mode key, and association shorthand remains on its separate outer-key association surface. Focused and nearby join proofs passed.

Broader quantified comparisons are also now complete. The public/router boundary in `lib/ecto_shorts/dynamics/postgres.ex` now rewrites operator-wrapped quantified-query payloads into the same quantified-query contract already used by equality shorthand, and the scalar comparison owner in `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` now emits quantified comparison expressions for `>`, `>=`, `<`, and `<=` instead of treating quantified tuples as pinned literal values. Focused and neighboring quantified tests passed.

The remaining deferred follow-on work is now only the direct raw lock payload widening in `Lock.build_query/6`.

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
- `test/ecto_shorts/dynamics/postgres/array_expr_test.exs`

## Module Specifications

### `EctoShorts.CommonFilters`

`EctoShorts.CommonFilters` is the public query-building boundary for filter params. Use it when the caller has a schema or queryable plus a filter map or keyword list and wants an `Ecto.Query` with the requested filters applied. Do not use it to define new SQL fragments directly; directive owners and dynamic builders handle that lower-level work.

Start with `convert_params_to_filter/3`. The module guarantees that it will reduce recognized filter entries through the API registry, preserve binding-selection context as filters nest, and hand field predicates to the configured query builder or the default API path. The module owns top-level param reduction and binding context selection. It does not own the implementation details of joins, locks, CTEs, or scalar and array expression generation.

### `EctoShorts.Dynamics.Postgres`

`EctoShorts.Dynamics.Postgres` is the public adapter-facing entry point for dynamic predicate building. Use it when a field predicate has already been selected for expression generation and must become an `Ecto.Query.DynamicExpr` on the Postgres path.

Start with `build_dynamic/4`. The module guarantees that it will normalize negation and quantified top-level forms before dispatching to `CommonExpr`, `ArrayExpr`, or `ScalarExpr`. It owns the routing decision between common operators, array-field operators, and scalar-field operators. It does not own the final SQL fragments for each operator family.

### `EctoShorts.Dynamics.Postgres.ArrayExpr`

`ArrayExpr` owns Postgres-specific behavior for array and map-backed field predicates once routing has already determined that the field is array-like. It accepts the normalized binding selector, field key, negation flag, and operator/value term, and returns a dynamic expression when the shape is supported.

The live contract currently covers list equality and inequality, scalar membership, overlap via `in` with a list, array `nil`, array `count`, element-wise comparisons using `ANY`, array-local comparison `all:` using `ALL(array)`, containment-style `all: [in: list]` using `<@`, `lower` and `upper` transforms, and array `like` and `ilike`. The next approved compatibility work here is narrower: preserve caller-supplied wildcard patterns for `like` and `ilike` while keeping contains-style wrapping for bare values.

### `EctoShorts.Dynamics.Postgres.ScalarExpr`

`ScalarExpr` owns non-array Postgres field predicates after routing has already decided the field is not array-like and does not belong to `CommonExpr`. Use it when the public filter pipeline has already selected a scalar field predicate and the work is about how that predicate becomes a dynamic expression on the Postgres path.

Start with `dynamic_expr/5`. The module guarantees that it will normalize the incoming term into the operator family expected by the compiled scalar builders and dispatch string operators to the string-specific compiled module. It does not own public filter reduction or array-field behavior.

### `EctoShorts.CommonFilters.Join`

`Join` owns join directive payloads after `CommonFilters.API` dispatches to it. It accepts join params, resolves source kinds, builds the `on` expression, and applies the join to the query.

The live contract supports association, schema, table, query, subquery, and fragment sources through the existing outer-key shapes. It uses `qualifier` as the runtime key for join mode. It also contains hint-aware runtime branches. The approved follow-on join slice adds an explicit single-join payload that uses `type:` for the source family in place of the earlier planned `kind:` name; it does not repurpose `type:` as a join-mode alias.

### `EctoShorts.CommonFilters.Lock`

`Lock` owns lock directive payloads after API dispatch. The live contract expects a map or keyword payload with a `name` key. Built-in names `:for_update` and `:for_share` are handled directly. Other names are resolved through the query provider, which must return `{:ok, function}`, `{:error, reason}`, or `nil`. Provider-owned `values:` may be translated into a valid Ecto lock expression there without widening the public boundary.

The live contract includes provider-backed callback customization. It does not currently expose a direct raw function payload or direct raw string lock payload at the public boundary.

### `EctoShorts.CommonFilters.WithCte`

`WithCte` owns `with_cte` payloads after API dispatch. It accepts a map or keyword list of named CTE definitions, each of which must include `as:` as either a query, subquery, or filter-param payload. It also accepts `materialized:` when that option is boolean or `nil`.

The live contract includes nested use under named and positional bindings, works alongside `recursive_ctes`, and includes validated `operation:` support for `:all | :update_all | :delete_all` with unchanged-query behavior on invalid `operation:` input.

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
- `{:all, {:>, value}}`, `{:all, {:>=, value}}`, `{:all, {:<, value}}`, `{:all, {:<=, value}}` for array-local `ALL(array)` comparisons
- `{:all, {:in, list}}` for containment using `<@`
- `{:like, value}` and `{:ilike, value}` for scalar or list pattern search after owner-local pattern construction

Produced output shape: a dynamic expression or `nil`.

Unsupported live shapes that matter for later work:

- unsupported nested payload shapes outside the proved `count`, `all`, and string-matching contracts

Current in-flight execution notes:

- array `nil`, proved `count`, comparison-operator `all:`, and containment-style `all: [in: list]` are now implemented and proved
- the next approved compatibility change at this boundary is wildcard preservation for `like` and `ilike`

### Boundary: `ScalarExpr.dynamic_expr/5` to `ScalarExprBuilder.quote_expr/3`

Accepted input shape: a normalized binding selector, scalar field key, negation flag, and operator/value term already routed away from `CommonExpr` and `ArrayExpr`.

Produced output shape: an `Ecto.Query.DynamicExpr`.

Owned transformations:

- normalize shorthand terms into operator/value tuples before compiled-module dispatch
- dispatch string operators to the compiled string module
- construct scalar LIKE/ILIKE pattern expressions at the compiled-builder boundary

Forbidden accidental contract expansion:

- string-pattern heuristics must not move up into `CommonFilters` or `Postgres.build_dynamic/4`
- this boundary must preserve the current contains-style convenience for bare values even if wildcard preservation is added

### Boundary: `Join.build_query/6`

Accepted shapes that matter:

- outer-key join entries such as `association: [source: :author, ...]`, `schema: [source: User, ...]`, `table: [source: "users", ...]`, `query: [source: queryable, ...]`, `subquery: [source: params_or_queryable, ...]`, and `fragment: [source: ..., ...]`
- the approved explicit single-join payload shape `type: :association | :schema | :table | :query | :subquery | :fragment` plus `source`, `as`, `on`, `qualifier`, `prefix`, and `hints`

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

- `with_cte: [cte_name: [as: query_or_subquery_or_filter_params, materialized: boolean_or_nil, operation: :all | :update_all | :delete_all]]`

Produced output shape: an `Ecto.Query` with one or more CTEs applied.

Unsupported live shape that matters for later work:

- invalid `operation:` values beyond the validated owner contract

## Internal Structure Walkthrough

A typical public integration call begins at `EctoShorts.Actions.all/3`. That action hands filter params to `EctoShorts.CommonFilters.convert_params_to_filter/3`, which starts with `CommonSchema.to_query(source)` and reduces each filter entry from the root selected binding `{:as, nil}`.

For each entry, `apply_filters/6` decides what kind of thing it is looking at. In the intended public contract, top-level `as` and `at` maps change the selected binding and continue nested reduction. The live code still contains a `bind` branch, but that path is treated elsewhere in this plan as stale runtime drift rather than part of the intended caller-facing contract. If the key belongs to a filter group such as predicate or post-aggregate filters, the reducer either keeps descending or dispatches to the registered filter owner. If the key names an association and the term shape looks reducible, `CommonFilters` first ensures a named association binding through `with_named_binding`, then continues reduction under `{:as, association_name}`. Otherwise, the reducer dispatches directly to `CommonFilters.API.build_query/6`.

When a field predicate reaches the Postgres dynamic path, `EctoShorts.Dynamics.Postgres.build_dynamic/4` normalizes top-level negation, then normalizes top-level quantified forms such as `all` and `any` into equality against a built quantified query only when the payload is on the quantified-query contract. After that, it routes by family. Common operators such as `before` and `after` stay in `CommonExpr`. Array and map-backed fields route to `ArrayExpr`. Everything else routes to `ScalarExpr`.

That routing order still matters for the current wildcard slice, but for a different reason than the completed array work. String-pattern construction already belongs in the owners after routing, not in the public reducer or Postgres router. Scalar-field string predicates travel from `CommonFilters.convert_params_to_filter/3` into `Postgres.build_dynamic/4`, then into `ScalarExpr.dynamic_expr/5`, which dispatches string operators to the compiled scalar string builder where LIKE/ILIKE patterns are assembled. Array-field string predicates travel through the same public and router boundaries but finish in `ArrayExpr.dynamic_expr/5`, where `normalize_patterns/1` currently wraps all values. The wildcard-preservation slice therefore belongs at those two owner-local pattern-construction points. If a join payload alias or lock payload alias is added, the change still belongs in the directive owner after API dispatch, not in `CommonFilters.apply_filters/6`.

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

The live array path now includes the previously missing `nil`, `count`, comparison-operator `all:`, and containment-style `all: [in: list]` shapes that were closed during this execution. Later work should preserve those live shapes while limiting further compatibility changes to explicitly approved slices.

#### Rules:

- Existing live array equality, inequality, membership, overlap, `ANY` comparison, transform, and string-matching behavior must remain unchanged.
- Ecto `like/2` and `ilike/2` accept raw search patterns, and the approved compatibility extension now targets wildcard preservation while keeping contains-style wrapping for bare scalar values and list entries.
- Unsupported array-only shapes must continue to follow the current `ArrayExpr` acceptance/rejection behavior unless this plan explicitly changes that contract.
- Invalid or unsupported array payload shapes must not be silently treated as supported behavior.
- PostgreSQL-backed shapes such as containment with `<@` and `ALL(array)` are now part of the proved live contract at the approved array-local shapes.
- The completed comparison-operator `all:` slice preserved quantified-subquery routing while enabling array-local `ALL(array)` comparisons.
- Containment-style `all: [in: list]` with `<@` is now part of the live contract.

#### Examples:

`Actions.all(Post, %{tags: ["elixir", "erlang"]})`
`#=> returns posts whose tags field exactly equals that list`

`Actions.all(Post, %{tags: %{>: "a"}})`
`#=> returns posts where any array element is greater than "a"`

`Actions.all(Post, %{tags: nil})`
`#=> returns posts whose tags field is null`

`Actions.all(Post, %{tags: %{count: %{>: 0}}})`
`#=> returns posts whose array_length(tags, 1) is greater than 0`

`Actions.all(Post, %{tags: %{count: %{==: 0}}})`
`#=> returns posts whose coalesced array length is 0, following the documented example contract for empty-array zero checks`

`Actions.all(Post, %{tags: %{all: %{>: "a"}}})`
`#=> returns posts where every array element is greater than "a", preserving the same caller-facing operator meaning as the existing `ANY` array path`

`Actions.all(Post, %{tags: %{all: %{in: ["elixir", "erlang"]}}})`
`#=> returns posts where every array element is contained in the given list, mapping to PostgreSQL `<@` semantics`

#### Resolved Review Notes:

- **Q:** Does the active runtime owner already contain array `count` support? **A:** No. The active `ArrayExpr` implementation does not contain `count` branches.
- **Q:** Do official docs make containment and `ALL(array)` real options at the database layer? **A:** Yes. PostgreSQL documents both, but the live repo owner does not currently expose them.
- **Q:** Does every old example query map cleanly onto the preserved current operator meaning? **A:** No. The older `all: [>: value]` example query is inverted relative to the preserved live `ANY` comparison semantics, so this plan defines the `ALL` implementation by preserving the current caller-facing operator meaning instead of copying that stale example literally.
- **Q:** Are containment-style `all: [in: list]` and comparison-operator `all:` both live now? **A:** Yes. Both array-local `ALL(array)` comparisons and containment-style `<@` support are now implemented and publicly proved.
- **Q:** Did comparison-operator `all:` require a Postgres routing change before `ArrayExpr` could own it? **A:** Yes. The completed slice first narrowed quantified-query interception in `Postgres.normalize_quantified_term/3`, then added the matching owner behavior in `ArrayExpr`.

### Story: String-matching compatibility over the current contains-style contract

The live string-matching path already wraps bare values as contains-style patterns for scalar and array fields. The newly approved compatibility extension is narrower than a contract rewrite: preserve caller-supplied `%` and `_` wildcard patterns while keeping the current contains-style convenience for values that do not already contain wildcard markers.

#### Rules:

- Bare scalar `like` and `ilike` values must continue to behave as contains-style search input.
- Bare list entries for scalar and array string matching must continue to behave as contains-style search input.
- Caller-supplied `%` and `_` wildcard markers must be preserved instead of being wrapped again.
- The wildcard-preservation heuristic belongs only in the string owners, not in `CommonFilters` or `Postgres.build_dynamic/4`.
- Negated `like` and `ilike` behavior must continue to wrap or preserve patterns using the same rule as the non-negated forms.

#### Examples:

`CommonFilters.convert_params_to_filter(Post, %{title: %{like: "Hello"}}, [])`
`#=> returns a query that searches using "%Hello%"`

`CommonFilters.convert_params_to_filter(Post, %{title: %{like: "Hello%"}}, [])`
`#=> returns a query that preserves the caller-supplied "Hello%" pattern`

`Actions.all(Post, %{title: %{not: %{ilike: "%world"}}})`
`#=> returns posts whose title does not case-insensitively match the caller-supplied "%world" pattern`

`ArrayExpr.dynamic_expr({:as, nil}, :tags, nil, {:like, ["elixir%", "%lang"]}, [])`
`#=> returns a dynamic expression that preserves those caller-supplied array patterns without adding extra outer wildcards`

#### Resolved Review Notes:

- **Q:** Does upstream Ecto already support raw LIKE and ILIKE patterns? **A:** Yes. `Ecto.Query.API` documents raw search-pattern inputs such as `"Chapter%"` for both `like/2` and `ilike/2`.
- **Q:** Does the current repo already preserve wildcard patterns? **A:** No. The current owners still wrap all bare values and list entries with `%...%`.
- **Q:** Why not implement this in `CommonFilters` or the Postgres router? **A:** Because the existing pattern construction already lives in `ScalarExprBuilder` and `ArrayExpr`, and moving that logic outward would widen responsibilities without changing the public contract.

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
- Existing `with_cte` support for `as:`, optional `materialized:`, and `operation:` must remain unchanged.
- Invalid directive payloads must continue to follow the current owner-specific logging and unchanged-query behavior unless later implementation deliberately changes that contract.

#### Examples:

`CommonFilters.convert_params_to_filter(Post, %{join: [schema: [source: User, as: :user, on: %{author_id: 1}]]}, [])`
`#=> returns a query with the schema join applied`

`CommonFilters.convert_params_to_filter(Post, %{join: [type: :schema, source: User, as: :user, on: %{author_id: 1}]}, [])`
`#=> returns a query with the same schema join applied through the explicit source-family payload`

`CommonFilters.convert_params_to_filter(Post, %{lock: %{name: :provider_for_update}}, query_provider: EctoShorts.TestQueryProvider)`
`#=> returns a query with a provider-backed FOR UPDATE lock`

`CommonFilters.convert_params_to_filter(Post, %{with_cte: [published_posts: [as: [published: true], materialized: false]]}, [])`
`#=> returns a query with the named CTE applied and materialized false`

`CommonFilters.convert_params_to_filter(Post, %{with_cte: [published_posts: [as: [published: true], operation: :all]]}, [])`
`#=> returns a query with the named CTE applied using the `:all` operation`

#### Resolved Review Notes:

- **Q:** What does `type` mean in this family after review? **A:** `type` is the approved name for the explicit join-payload source-family selector and replaces the earlier planned `kind:` name. Outer-key joins such as `association: [...]` and `schema: [...]` remain live. Join mode remains under `qualifier:`. The API must not accept `type:` as a join-mode alias.

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
  Then comparison-operator `all:` payloads must preserve the live caller-facing operator meaning
  And containment-style `all: [in: list]` must compile to PostgreSQL `<@`

Scenario: Comparison-operator array `all:` must not break quantified subquery `all`
  Given the existing quantified subquery support in `Postgres`
  When array-local comparison `all:` support is added
  Then quantified subquery `all` must still route through `SetComparison`
  And array-local comparison `all:` must reach `ArrayExpr` instead of raising on missing `:from`

Scenario: Directive compatibility work preserves the current owner contracts
  Given the live `Join`, `Lock`, and `WithCte` modules
  When the plan describes future compatibility work
  Then it must preserve `qualifier:` as the canonical live join key
  And explicit join payloads may use `type:` only for source family selection
  And it must preserve name-based provider-backed lock callbacks
  And it must not widen the public lock boundary beyond `name:` payloads without a separate later decision
  And it must preserve `with_cte` support for `as:`, `materialized:`, and `operation:`

Scenario: Wildcard-preservation compatibility keeps contains-style behavior for bare values
  Given the live scalar and array string-matching owners
  When a caller passes a bare string or a bare list entry without `%` or `_`
  Then the resulting LIKE or ILIKE pattern must still use contains-style wrapping

Scenario: Wildcard-preservation compatibility preserves explicit patterns
  Given the approved wildcard-preservation extension
  When a caller passes a scalar or array string-matching value that already contains `%` or `_`
  Then the owner must preserve that caller-supplied pattern
  And it must not add extra outer wildcards around it

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
- Expand direct array-path proof in `test/ecto_shorts/dynamics/postgres/array_expr_test.exs` so the supported live array contract is proved more explicitly.

For runtime gaps and compatibility work during the approved implementation:

- Add boundary-visible tests for binding selector aliases in `test/ecto_shorts/common_filters_test.exs` and any lower-level compiler or dynamic tests needed only if the alias normalization touches those boundaries.
- Add array runtime tests in `test/ecto_shorts/actions/crud_test.exs`, `test/ecto_shorts/dynamics/postgres/array_expr_test.exs`, and targeted `test/ecto_shorts/dynamics/postgres_test.exs` coverage for router conflicts where needed. Keep containment-style `<@` work separate until it is intentionally implemented.
- Add directive tests in `test/ecto_shorts/common_filters_test.exs` for `with_cte operation` support if that feature is implemented.
- Add join directive tests in `test/ecto_shorts/common_filters_test.exs` proving that explicit join payloads can use `type:` as the source-family selector while `qualifier:` remains the join-mode key and the outer-key join shapes keep working.
- Add scalar and array string-matching tests for wildcard-preservation compatibility in `test/ecto_shorts/common_filters_scalar_filter_test.exs`, `test/ecto_shorts/actions/crud_test.exs`, `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`, and `test/ecto_shorts/dynamics/postgres/array_expr_test.exs`.

Each later test must prove one caller-visible claim. Passing tests should be described as evidence for the executed cases only, not as proof of correctness for all possible inputs.

## Validation and Acceptance

The planning artifact itself is acceptable when a reader can trace each audited family to one of three outcomes: already implemented, implemented but under-proved, or missing runtime behavior.

The future validation matrix for implementation work is:

- Claim: binding selector aliases are supported without breaking existing integer positional bindings.
  Boundary: `CommonFilters.convert_params_to_filter/3` plus any downstream binding contract touched by the implementation.
  Proof method: ExUnit boundary tests and targeted lower-level tests only if the alias normalization crosses those boundaries.
  Evidence command: `mix test test/ecto_shorts/common_filters_test.exs` and any smaller focused test files added for the touched compiler or dynamic boundary.
  Residual risk: if alias support depends on query-shape-specific last-binding detection, edge cases across unusual join counts may still need broader coverage.

- Claim: array `nil`, `count`, comparison-operator `all:`, and containment-style `all: [in: list]` behaviors work without breaking the current live array contract.
  Boundary: `Actions.all/3`, `Postgres.build_dynamic/4`, and `ArrayExpr.dynamic_expr/5`.
  Proof method: boundary integration tests plus targeted router tests plus direct array-expression tests.
  Evidence command: `mix test test/ecto_shorts/actions/crud_test.exs test/ecto_shorts/dynamics/postgres_test.exs test/ecto_shorts/dynamics/postgres/array_expr_test.exs`.
  Residual risk: behavior remains Postgres-specific, and the executed cases establish the approved array-local shapes only.

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

- Claim: wildcard-preservation compatibility preserves caller-supplied `%` and `_` patterns without breaking the current contains-style convenience for bare scalar and array values.
  Boundary: `CommonFilters.convert_params_to_filter/3`, `Actions.all/3`, `ScalarExpr.dynamic_expr/5`, and `ArrayExpr.dynamic_expr/5`.
  Proof method: boundary SQL tests, boundary integration tests, targeted scalar direct tests, and targeted array direct tests.
  Evidence command: `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs test/ecto_shorts/actions/crud_test.exs test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs test/ecto_shorts/dynamics/postgres/array_expr_test.exs`.
  Residual risk: the executed cases will establish the chosen `%` and `_` preservation rules for the tested scalar and array forms only.

## Plan of Work

Perform the approved implementation work in this order.

First, add proof-only changes before runtime changes wherever the live audit suggests that runtime behavior already exists. That means quantified `any`, join hints, and stronger direct array-expression proof come before runtime edits. If any proof-only test exposes a real defect, update this plan’s `Progress`, `Decision Log`, and the affected contracts before changing code.

Second, implement small compatibility shims where the desired future behavior is additive and can normalize back to the existing canonical live contract. Binding selector aliases belong in this category. The key safety rule is that the public boundary may accept a broader input shape, but the downstream internal boundary should stay on the existing canonical representation whenever possible.

Third, implement the true runtime gaps in the smallest owner modules possible. The completed array slice first fixed Postgres routing so array-local comparison `all:` could reach the array owner without breaking quantified subquery `all`, then completed the remaining array-owner work in `lib/ecto_shorts/dynamics/postgres/array_expr.ex`, including containment-style `all: [in: list]` with `<@`. The completed directive slice implemented `with_cte operation` in `lib/ecto_shorts/common_filters/with_cte.ex`. These completed changes now become preserved live behavior.

Fourth, execute the approved wildcard-preservation compatibility extension at the smallest owner-local pattern-construction points. The work belongs in `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex` and `lib/ecto_shorts/dynamics/postgres/array_expr.ex`. The preserved behavior is that bare strings keep the current contains-style convenience even though Ecto itself accepts raw patterns. The new compatibility behavior is explicit wildcard preservation for callers who pass patterns that already include `%` or `_`.

After each implementation slice, update this plan, run the named focused tests, and record the result precisely as evidence for the executed cases.

## Concrete Steps

Work from the repository root `/Users/kurthogarth/Documents/GitHub/ecto_shorts`.

For proof-only changes, the expected focused commands are:

    mix test test/ecto_shorts/common_filters_scalar_filter_test.exs
    mix test test/ecto_shorts/common_filters_test.exs
    mix test test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs test/ecto_shorts/dynamics/postgres_test.exs
    mix test test/ecto_shorts/dynamics/postgres/array_expr_test.exs

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
- `test/ecto_shorts/dynamics/postgres/array_expr_test.exs`
- `Ecto.Query.lock/3` in `Ecto.Query`
- `Ecto.Query.with_cte/3` in `Ecto.Query`
- `Ecto.Query.API` entries for `all/1`, `any/1`, `like/2`, and `ilike/2`
- PostgreSQL documentation for row and array comparisons (`ANY` / `ALL`) and array operators such as `<@`, `@>`, and `&&`

Revision note (2026-03-14 12:34Z): created this repo-local governing ExecPlan because the earlier megaplan lived outside the repository and no longer matched the current planning rules. Rebased the plan on the live-first audit and narrowed the later implementation scope to true runtime gaps, proof gaps, and explicit compatibility choices.

Revision note (2026-03-14 12:41Z): reviewed artifact governance with the user and kept `plans/api-feature-reverification.md` as the single governing ExecPlan. The older external megaplan is superseded and no longer authoritative.

Revision note (2026-03-14 12:43Z): reviewed binding selector scope with the user and kept `at: :first` and `at: :last` in future completion scope as additive compatibility aliases. The plan now treats that as settled future scope rather than an open preference question.

Revision note (2026-03-14 15:49Z): implementation began after explicit user approval. Recorded the completed proof-only slice for quantified `any`, join hints, and direct array-expression coverage, captured the passing focused and broader test evidence, and advanced the next slice to additive binding-selector alias work.

Revision note (2026-03-14 15:53Z): completed the additive binding-selector alias slice by resolving `at: :first` and `at: :last` to integer positions in `CommonFilters`, recorded the passing focused and broader validation evidence, and advanced the next slice to the true array runtime gaps.

Revision note (2026-03-14 20:21Z): corrected the earlier join-contract drift before continuing implementation. The plan now records `type:` only as the explicit join-payload source-family selector replacing the earlier planned `kind:` name, not as a join-mode alias. Synced the completed `Join.build_query/6` implementation and focused validation evidence, and recorded that the remaining broader-file failures belong to the still-pending direct-lock slice.

Revision note (2026-03-14 20:32Z): completed the broader quantified comparison slice. Recorded that the runtime repair required both the Postgres router update and a second owner-local change in the generic scalar comparison builder branch for non-equality quantified expressions, then synced the passing focused and neighboring quantified validation evidence.
