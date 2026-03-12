# Design Log: ScalarExpr Scalar-Filter Milestone

## Task and Key Files

Active task: Complete the scalar filter path end-to-end, starting with the behaviors asserted in `test/ecto_shorts/common_filters_scalar_filter_test.exs`, while preserving the current user-approved structure of `lib/ecto_shorts/dynamics/postgres.ex`, `lib/ecto_shorts/dynamics/postgres/common_expr.ex`, and `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`.

Key files:
- `DESIGN.md`
- `PLAN.md`
- `BINDING_REFACTOR_PLAN.md`
- `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex`
- `lib/ecto_shorts/dynamics/postgres/scalar_expr_builder.ex`
- `lib/ecto_shorts/dynamics/postgres/common_expr.ex`
- `lib/ecto_shorts/dynamics/postgres.ex`
- `test/ecto_shorts/common_filters_scalar_filter_test.exs`
- `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`
- `test/ecto_shorts/dynamics/postgres_test.exs`

Document updates to track in the same change:
- Create and maintain `DESIGN.md` as the active working document for this refactor.
- Keep `PLAN.md`, `BINDING_REFACTOR_PLAN.md`, and `DESIGN.md` synchronized when the task boundary, chosen approach, validation path, or assumptions change.

## Trigger for Using This Document

Observed facts that triggered this document choice:
- The task now includes a required root-level `DESIGN.md` with prescribed sections and ongoing maintenance expectations.
- The work combines architecture decisions, implementation sequencing, interpretation tracking, and explicit validation checkpoints.
- The repository already requires planning and maintenance documents to record the active task, key files, companion documents, and reasoning for using the document.

Reasoning path from those facts to this document:
- Because the user explicitly required a root `DESIGN.md`, this document must become an active planning and execution artifact rather than an optional note.
- Because the work spans compiler behavior, generated-module routing, runtime dispatch, and test reshaping, the document must capture both design intent and step-by-step replication instructions.
- Because the repository already has root planning artifacts, this document must coexist with them and carry synchronization notes instead of replacing them implicitly.

Nearby document types considered and rejected:
- `RefactorPlan` alone: rejected because the user explicitly requested `DESIGN.md` and specified custom sections not covered by the existing plan format.
- `InvestigationLog`: rejected because the task is not limited to diagnosis; it includes implementation guidance and progress tracking.
- No new document: rejected because it would violate the explicit task instruction.

Replication rule:
- Use a root `DESIGN.md` when the task explicitly requires a living design log with understanding, replication instructions, clarification history, and validation checkpoints, and keep it synchronized with any existing root planning artifacts.

## Understanding Summary

Current understanding:
- The active milestone is to complete the scalar filter path end-to-end, starting with the behaviors already asserted in `test/ecto_shorts/common_filters_scalar_filter_test.exs`.
- The current active subtask inside that broader milestone is the explicit wrapped arithmetic RHS comparison case asserted in `test/ecto_shorts/common_filters_scalar_filter_test.exs:206`.
- The primary scalar-filter research files are:
  - `research/SCALAR_FIELDS.md`
  - `research/COMPARISON_OPERATOR_DIRECTIVES.md`
  - `research/STRING_MATCHING_DIRECTIVES.md`
  - `research/NEGATION_DIRECTIVES.md`
- The later scalar-path files to defer unless proven necessary are:
  - `research/LOGICAL_OPERATOR_DIRECTIVES.md`
  - `research/DATE_TIME_DIRECTIVES.md`
  - `research/ARITHMETIC_OPERATOR_DIRECTIVES.md`
  - `research/AGGREGATE_OPERATOR_DIRECTIVES.md`
  - `research/SET_COMPARISON_DIRECTIVES.md`
- `ScalarExpr` already uses a builder and compiler-generated clauses. The remaining work is to expand supported scalar term shapes without restructuring the approved module layout.
- The scalar runtime wrapper is schema-field-first and currently lives at `dynamic_expr(binding_selector, schema_field_key, negated, term, opts)`.
- The current public scalar call chain is:
  - `CommonFilters.convert_params_to_filter/3`
  - `CommonFilters.apply_filters/6`
  - `CommonFilters.Where.build_query/6`
  - `EctoShorts.Adapters.Postgres.build_dynamic/4`
  - `EctoShorts.Adapters.Postgres.apply_expr/4`
  - `EctoShorts.Adapters.Postgres.build_expr/5`
  - `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5`
  - `EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled.*.dynamic_expr/4`
- `ScalarExpr` must stay dumb at the expression layer. It should only match scalar term shapes and return the corresponding dynamic expression.
- Direct `ScalarExpr` tests must use fully resolved expression-layer tuple shapes all the way down. Nested wrappers such as `:not`, `:lower`, and `:upper` must also be normalized before they reach `ScalarExpr`.
- `CommonExprBuilder` is the current source of truth for how negation should be generated simply: build the positive expression once in `expr_for/3`, then add a generic `{:not, term}` blueprint in `specs_for/4` that wraps the same expression with `Helpers.negated_expr/1`.
- For the scalar path, `:not` should be treated as a resolved wrapper shape handled in `specs_for/4`, not as a separate base expression family that duplicates operator logic in `expr_for/3`.
- Binding-selector validation still belongs only in `lib/ecto_shorts/dynamics/postgres.ex`.
- The current structures of `lib/ecto_shorts/dynamics/postgres.ex`, `lib/ecto_shorts/dynamics/postgres/common_expr.ex`, and `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex` are user-approved. Do not restructure those files unless the user explicitly asks for it.
- The current approved helper names in `Postgres` are `build_dynamic/4`, `apply_expr/4`, and `build_expr/5`. Do not rename or reshape them casually.
- The active direct proof surface for scalar behavior lives in `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`.
- The repo-grounded scalar gap is explicit: `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs` is failing for comparison operators and aliases, list semantics, negation, string matching, and lower/upper transforms.
- Unsupported nil scalar comparisons such as `%{published_at: %{>: nil}}` should raise for this milestone instead of warning and skipping.
- Existing APIs and tests are evidence only, but the current broad scalar filter tests are now the primary acceptance surface for this milestone.
- Work must still proceed incrementally in small steps.

## Key Information to Remember

- The user-approved structure of `lib/ecto_shorts/dynamics/postgres.ex`, `lib/ecto_shorts/dynamics/postgres/common_expr.ex`, and `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex` is the current source of truth. Do not restructure those files unless explicitly asked.
- `Postgres` owns map and keyword-list handling, recursive container reduction, and binding-selector validation.
- `Postgres` normalization should expand map and keyword containers into a flat list of resolved `{field_key, term}` entries, not collapse a single-entry keyword into a bare tuple-returning normalizer.
- Internal structure contract:
  - Normalize once at the boundary.
  - Reducers handle entries one at a time. The reducer does not try to solve all nested semantics in one pass. It takes one normalized entry, decides what kind of entry it is, and delegates.
  - Delegation happens only after structure is stable.
- The exact rules that are missing are:
  - `build_dynamic/4` owns one normalization pass. `postgres.ex` should normalize the incoming term once when `build_dynamic/4` starts.
  - That normalization pass should produce a predictable internal structure for reduction.
  - It should not keep re-normalizing later in the flow.
  - Normalization is structural only.
  - Normalization should only:
    - convert maps to keyword/list form
    - preserve explicit wrapper keys
    - set missing default operator keys
    - make merge shape explicit
  - Normalization should not:
    - build query AST
    - interpret arithmetic semantics
    - fold arithmetic operands
    - rewrite nested payload meaning
  - Defaults belong in normalized structure. If the system wants predictable downstream terms, defaults need to exist before expr-module dispatch.
  - That includes defaults like:
    - `:and`
    - `:==`
  - A bare scalar term should not rely on `ScalarExpr` to invent equality later. It should already arrive in an explicit operator form by the time the reducer delegates it.
  - Current implementation mismatch to remember: `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex` still has `normalize_term/1` and currently defaults bare scalar values to `{:==, value}`.
  - Treat that `ScalarExpr.normalize_term/1` equality defaulting as a remaining implementation mismatch against the desired boundary, not as the long-term contract.
  - The reducer owns entry-time preparation. After normalization, the reducer processes one entry at a time.
  - At that stage it can do limited pre-build work only when the key requires it.
  - Examples already in `postgres.ex`:
    - negation shaping
    - quantified query prebuild via `SetComparison.build_quantified_query/3`
  - That kind of work belongs there because it is tied to the current entry being reduced.
  - Expr modules should receive explicit terms, not infer intent.
  - By the time `CommonExpr`, `ArrayExpr`, or `ScalarExpr` is called, the term should already be explicit enough that the expr module is only deciding how to render/build, not what the term means.
- The layers and boundaries are:
  - Layer 1: `build_dynamic/4`
    - Responsibility:
      - entry boundary
      - normalize once
      - reduce normalized entries
      - merge dynamic fragments
    - It should produce a stream of normalized entries like:
      - `{:and, {:views, {:==, 10}}}`
      - `{:and, {:views, {:>, {:value, ...}}}}`
      - `{:or, {:status, {:==, "draft"}}}`
    - Not necessarily these exact final tuples yet in the current code, but this is the kind of explicit shape the reducer should be working with.
  - Layer 2: reducer plus `apply_expr/4`
    - Responsibility:
      - process entries one by one
      - unwrap structural containers
      - do key-specific prep when needed
      - delegate to the right expr family
    - Allowed work here:
      - map/keyword traversal
      - merge operator handling
      - quantifier prebuilding
      - negation shaping
      - dispatch selection
    - Not allowed here:
      - deep arithmetic interpretation
      - expression AST building
  - Layer 3: `build_expr/5`
    - Responsibility:
      - take one already-prepared entry
      - route it to:
        - `CommonExpr`
        - `ArrayExpr`
        - `ScalarExpr`
    - At this point, the term should already have explicit operator structure.
  - Layer 4: expr modules
    - Responsibility:
      - only build the expression/query behavior for the already-defined term
    - For scalar, that means:
      - comparison handling
      - wrapped RHS rendering rules
      - expression AST generation where needed
    - Expr modules should not invent missing operator defaults if the boundary contract says those defaults belong earlier.
- `CommonExpr`, `ArrayExpr`, and `ScalarExpr` are dumb expression modules. They should only deal with resolved term shapes and return matching dynamic expressions.
- `ScalarExpr` is schema-field-first at the term-contract level: the binding selector and schema field key stay separate, and the resolved operator input is carried inside `term` as `{op, value}`.
- Current runtime detail: the public wrapper is `ScalarExpr.dynamic_expr/5`, and it delegates to generated compiled modules that expose `dynamic_expr/4` with the shape `dynamic_expr(selected_binding, key, negated, value)`.
- Current implementation note: `ScalarExpr.dynamic_expr/5` still calls `normalize_term/1`, and bare scalar values are still defaulted to `{:==, value}` there today. Keep that in mind when reading the current code, but do not treat it as the desired boundary.
- Direct `ScalarExpr` tests must never use partially normalized public API shapes. If a nested value is still a map such as `%{lower: "hello"}` or `%{==: 10}`, the test is still written at the `CommonFilters` / `Postgres` layer, not the `ScalarExpr` layer.
- `ScalarExprBuilder` is no longer only the minimal equality proof. The active scalar-filter milestone must extend it to support the operator and wrapper families already asserted in `test/ecto_shorts/common_filters_scalar_filter_test.exs`.
- `ScalarExprBuilder` should follow the `CommonExprBuilder` negation pattern exactly: reuse the same base scalar expression for both the normal and negated blueprints.
- Current builder implementation details that matter:
  - `ScalarExprBuilder.specs_for/4` sets `Blueprint.key` to the schema field key and `Blueprint.head` to `[negated, value]`.
  - `EctoShorts.Generator.Builder.quote_def/2` turns that into compiled clauses shaped like `dynamic_expr(selected_binding, key, negated, value)`.
  - `ScalarExprBuilder.case_clause_ast/3` first combines `negated` and `value` into a single local `term`, using `{:not, value}` when the negated flag is `:not`, and only then matches the generated conditions.
  - `ScalarExprBuilder.comparison_conditions/5` already has explicit branches for `{op, {:value, wrapped_value}}` and `{:not, {op, {:value, wrapped_value}}}` before the generic pinned-value branch.
  - `ScalarExprBuilder.quote_expr/3` has a dedicated wrapped-value clause `quote_expr(op, q_var, {key_var, {:value, value_var}})`.
  - `value_expr_ast/2` currently supports:
    - `{:field, field_name}` where `field_name` is already an atom
    - `{:value, literal}`
    - binary arithmetic tuples `{op, {left, right}}` for `:+`, `:-`, `:*`, `:/`
    - any other term falls back to a pinned value
- Current direct scalar proof details that matter:
  - `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` currently proves both plain scalar inputs like `1` and explicit tuple inputs like `{:==, 1}` because `ScalarExpr.normalize_term/1` still exists.
  - That direct proof file also already covers quantified comparisons, aggregate comparisons, list membership semantics, string matching, and lower/upper transforms at the expression layer.
- How to use this document as a source of truth:
  - `Task and Key Files`, `Understanding Summary`, and `Instructions` describe the current task and sequencing.
  - `Key Information to Remember` and `Boundary Contract` describe the intended internal contract.
  - Any bullet labeled as a current implementation note or mismatch records where the working tree still diverges from that intended contract.
- Acceptance tests are proof surfaces, not design drivers. Use the source-of-truth module boundaries and documented internal structure first, then use tests to verify the chosen implementation.
- If an acceptance test is blocked by an unrelated upstream failure, do not let that blocker redefine the task. Record the blocker separately, keep it out of scope unless the user explicitly widens scope, and continue reasoning from the primary implementation seam.
- When an unrelated change appears useful only to unblock a proof path, classify it explicitly as one of:
  - required for the actual feature implementation
  - required only for the current proof path
  - out of scope
- Before editing `ScalarExpr` or `ScalarExprBuilder`, restate the exact normalized term shape they are supposed to receive from `Postgres`, including:
  - which defaults must already be explicit
  - what the reducer entry shape is
  - which layer owns entry-time preparation
  - which layer owns rendering
- The current scalar-filter acceptance surface is:
  - `test/ecto_shorts/common_filters_scalar_filter_test.exs`
  - `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`
  - `test/ecto_shorts/dynamics/postgres_test.exs`
- The active direct proof file for `ScalarExpr` is `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`.
- The primary scalar research inventory to remember is:
  - `research/SCALAR_FIELDS.md`
  - `research/COMPARISON_OPERATOR_DIRECTIVES.md`
  - `research/STRING_MATCHING_DIRECTIVES.md`
  - `research/NEGATION_DIRECTIVES.md`
- The later scalar research inventory to defer unless needed is:
  - `research/LOGICAL_OPERATOR_DIRECTIVES.md`
  - `research/DATE_TIME_DIRECTIVES.md`
  - `research/ARITHMETIC_OPERATOR_DIRECTIVES.md`
  - `research/AGGREGATE_OPERATOR_DIRECTIVES.md`
  - `research/SET_COMPARISON_DIRECTIVES.md`
- The current repo-grounded scalar gaps are:
  - comparison operators and aliases beyond equality
  - list semantics and membership checks
  - negation wrappers generated through the shared `specs_for/4` pattern
  - string matching with scalar and list payloads
  - lower/upper transforms
- Unsupported nil comparisons in the scalar milestone should raise, not warn-and-skip.
- Do not tighten behavior just because an edge case looks suspicious. If the behavior change is not required by an approved test or behavior spec, do not add it.
- When uncertain, preserve the current user-approved behavior and ask before changing semantics.

## Behaviour Specification

This specification applies to the current scalar-filter milestone.

### Scalar Research Inventory

Primary scalar-filter research files:
- `research/SCALAR_FIELDS.md`
- `research/COMPARISON_OPERATOR_DIRECTIVES.md`
- `research/STRING_MATCHING_DIRECTIVES.md`
- `research/NEGATION_DIRECTIVES.md`

Adjacent or later scalar-path files:
- `research/LOGICAL_OPERATOR_DIRECTIVES.md`
- `research/DATE_TIME_DIRECTIVES.md`
- `research/ARITHMETIC_OPERATOR_DIRECTIVES.md`
- `research/AGGREGATE_OPERATOR_DIRECTIVES.md`
- `research/SET_COMPARISON_DIRECTIVES.md`

### Boundary Contract

- `EctoShorts.Dynamics.Postgres.CommonExpr`, `EctoShorts.Dynamics.Postgres.ArrayExpr`, and `EctoShorts.Dynamics.Postgres.ScalarExpr` must receive resolved tuple inputs only.
- These modules must not be responsible for resolving maps or keyword lists into tuple forms.
- These modules must stay dumb. They only match on the tuple input shape they are given and return the corresponding `Ecto.Query.dynamic/2` expression.
- Binding-selector validation belongs only to `lib/ecto_shorts/dynamics/postgres.ex`.
- The expression modules must treat `binding_selector` as already-validated input and must not repeat `{:as, ...}` / `{:at, ...}` validity checks locally.
- The `Where` path is the adapter handoff point: `CommonFilters.Where.build_query/6` calls `EctoShorts.Adapters.Postgres.build_dynamic/4`, then wraps the returned dynamic in `Query.where/3` or `Query.or_where/3`.
- Resolution of `%{field: %{operator: value}}` into the tuple shape expected by `ScalarExpr` must happen before `ScalarExpr.dynamic_expr/5` is called.
- That resolution must follow the keyword-list-first API shape: non-struct maps become keyword lists first, keyword and map containers expand into a flat list of resolved `{field_key, term}` entries, and those reduced tuple items are what reach `ScalarExpr`.
- That resolved-tuple rule applies recursively. Nested wrappers such as `:not`, `:lower`, and `:upper` must also arrive as tuples at the `ScalarExpr` layer, not as nested maps.
- Current `Postgres` implementation details:
  - `build_dynamic/4` normalizes `term` with `normalize_params/1`, then reduces those entries with `expr_entry/2`.
  - `expr_entry/2` currently makes implicit merge behavior explicit by returning `{:and, {key, term}}` unless the reduced item already carries `:and` or `:or`.
  - `apply_expr/4` still unwraps map and keyword-list containers before calling `build_expr/5`.
  - `build_expr/5` currently performs only entry-time preparation that is still in scope there:
    - `normalize_negation_term/1`
    - `normalize_quantified_term/3`
    - expression-family dispatch to `CommonExpr`, `ArrayExpr`, or `ScalarExpr`
- Answered boundary questions:
  - The only defaults currently proven and approved at the normalized-structure level are:
    - implicit merge becomes `:and` through `expr_entry/2`
    - quantified terms become equality comparisons through `normalize_quantified_term/3`
    - bare scalar equality still defaults later in `ScalarExpr.normalize_term/1` today, but that remains an implementation mismatch, not the desired long-term contract
  - If bare scalar equality defaulting is moved out of `ScalarExpr`, it belongs in the initial `Postgres` normalization pass rooted at `normalize_params/1` / `normalize_keyword_params/2`, not in `expr_entry/2`, `apply_expr/4`, `build_expr/5`, or any expression module.
  - `apply_expr/4` container unwrapping is part of the current reducer implementation, but it should be treated as temporary structure-walking debt relative to the stricter “normalize once at the boundary” contract.
  - The desired scalar term shape by the time an expression module is called is explicit and resolved:
    - comparison form: `{op, value}`
    - negated comparison form: negation is split into the separate `negated` argument before expression dispatch
    - quantified comparison form after reducer-time preparation: `{:==, {quantifier, built_query}}`
    - plain scalar values are only still accepted today because `ScalarExpr.normalize_term/1` still exists
  - The builder-ready internal shape for the explicit wrapped arithmetic RHS case is:
    - public payload:
      - `%{views: %{>: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}}`
    - resolved scalar term reaching the expression layer:
      - `{:>, {:value, {:+, {{:field, :views}, {:value, 10}}}}}`
  - Conversion of nested public node shapes inside the explicit wrapped arithmetic RHS is part of structural normalization, not arithmetic interpretation. That means:
    - `%{field: "views"}` or `[field: "views"]` must resolve to `{:field, :views}`
    - `%{value: 10}` or `[value: 10]` must resolve to `{:value, 10}`
    - this conversion belongs to the initial `Postgres` normalization pass, not to `ScalarExpr` or `ScalarExprBuilder`
  - Arithmetic semantics that remain out of scope for `Postgres` normalization are:
    - folding variadic operands
    - interpreting operator precedence
    - building AST
    - choosing comparison behavior
  - Only binary arithmetic node shapes are currently in scope for the wrapped scalar RHS case because `value_expr_ast/2` only renders `{op, {left, right}}` for `:+`, `:-`, `:*`, and `:/`.
  - Wrapped arithmetic RHS support is currently a scalar comparison feature. The comparison-operator scope is:
    - `:==`, `:eq`, `:!=`, `:ne`, `:>`, `:>=`, `:<`, `:<=`, `:gt`, `:gte`, `:lt`, `:lte`
  - Negated wrapped arithmetic comparisons are structurally in scope because the generated comparison builder already handles `{:not, {op, {:value, wrapped_value}}}`. Quantified, transform-composed, or wider arithmetic combinations are not in scope unless a working-tree test or approved behavior spec requires them.
  - Valid reducer-time preparation means:
    - the step depends only on the already-identified current entry
    - it does not recurse into arithmetic meaning
    - it does not build AST
    - it does not choose SQL/operator semantics beyond entry preparation
  - In practice, valid reducer-time preparation currently includes:
    - negation flag extraction
    - quantified subquery prebuild
    - merge-op handling
    - expression-family dispatch
  - Reducer-time preparation must not resolve nested wrapped arithmetic node shapes. That node resolution belongs to the initial normalization pass if the term must reach expr modules as a fully resolved term.
  - The direct expression-layer proof input for the explicit wrapped arithmetic RHS case should be:
    - `ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:>, {:value, {:+, {{:field, :views}, {:value, 10}}}}}, [])`
  - While `ScalarExpr.normalize_term/1` still exists, direct scalar tests may keep plain scalar proof inputs like `1` and `nil` to document current behavior. If equality defaulting moves fully upstream later, those plain-scalar tests should move to the `CommonFilters` / `Postgres` layer or be rewritten to explicit tuple inputs.
  - The authoritative proof surfaces for the explicit wrapped arithmetic RHS case are:
    - direct expression-layer proof at `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` using the fully resolved tuple shape above
    - public acceptance proof at `test/ecto_shorts/common_filters_scalar_filter_test.exs:206`
  - `assert_sql/4` compares both the SQL text and the params returned by `Ecto.Adapters.SQL.to_sql/3`. For AST-composed arithmetic expectations, the handwritten expected query must use the pinned form Ecto generates in code, for example `from(p in Post, where: p.views > p.views + ^10)`, instead of the inline literal form `from(p in Post, where: p.views > p.views + 10)`.
  - The authoritative scalar acceptance file path in the current working tree is `test/ecto_shorts/common_filters_scalar_filter_test.exs`. Any older `test/ecto_shorts/common_filters/scalar_filter_test.exs` reference in this document is stale and should be treated as historical text, not as the current file path.
- Example normalization target:
  `%{id: %{or: %{>: 2, <: 4}}, title: "hello"}`
  must resolve to
  `[{ :id, {:or, {:>, 2}} }, { :id, {:or, {:<, 4}} }, { :title, "hello" }]`.

### Repo-Grounded Gap List

The current direct scalar proof file `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` already covers:

- plain scalar equality and nil handling
- comparison operators and aliases
- list membership semantics
- quantified comparisons
- aggregate comparisons
- string matching
- lower/upper transforms

The current public scalar acceptance file `test/ecto_shorts/common_filters_scalar_filter_test.exs` is now green for the currently implemented scalar scope.

The proved string-list behavior for this milestone is:
- `%{field: %{like: ["a", "b"]}}` renders through `LIKE ANY(?)`
- `%{field: %{ilike: ["a", "b"]}}` renders through `ILIKE ANY(?)`
- negated list string matches render by negating the corresponding `ANY` fragment

The next remaining validation surface to recheck before claiming broader milestone completion is `test/ecto_shorts/dynamics/postgres_test.exs`.

### Required Scalar Filter Behaviour

For non-array, non-common-expression field filters:

- `%{field: value}` must continue to produce scalar equality.
- `%{field: %{==: value}}` must produce the same scalar equality expression as `%{field: value}`.
- `%{field: %{eq: value}}` must produce the same scalar equality expression as `%{field: value}`.
- `%{field: %{==: nil}}` must produce the same `is_nil(field)` expression as `%{field: nil}`.
- `%{field: %{eq: nil}}` must produce the same `is_nil(field)` expression as `%{field: nil}`.
- `%{field: %{!=: value}}` and `%{field: %{ne: value}}` must produce scalar inequality.
- `%{field: %{>: value}}`, `%{field: %{>=: value}}`, `%{field: %{<: value}}`, and `%{field: %{<=: value}}` must produce the corresponding scalar comparisons.
- `%{field: %{gt: value}}`, `%{field: %{gte: value}}`, `%{field: %{lt: value}}`, and `%{field: %{lte: value}}` must map to the same comparison behavior as their symbolic forms.
- `%{field: %{in: list}}` must produce membership checks.
- `%{field: %{==: list}}` must be treated as a membership check for scalar fields.
- `%{field: %{!=: list}}` must be treated as a negated membership check for scalar fields.
- `%{field: %{like: "text"}}` and `%{field: %{ilike: "text"}}` must perform wildcard string matching.
- `%{field: %{like: ["a", "b"]}}` and `%{field: %{ilike: ["a", "b"]}}` must match against any pattern in the list.
- `%{field: %{==: %{lower: value}}}` and `%{field: %{==: %{upper: value}}}` must compare against transformed field values.
- `%{field: %{!=: %{lower: value}}}` and `%{field: %{!=: %{upper: value}}}` must compare transformed field values with inequality.
- `%{field: %{not: inner}}` must support the negated scalar cases already asserted in `test/ecto_shorts/common_filters_scalar_filter_test.exs`.
- At the expression layer, negated scalar terms should be represented as `{:not, resolved_term}` and generated by wrapping the already-built positive expression with `Helpers.negated_expr/1`.

### Binding Forms In Scope

All scalar filter behaviors in this milestone must work for:

- `{:as, nil}`
- `{:as, binding_alias}` where `binding_alias` is an atom
- `{:at, position}` where `position` is a positive integer

### Invalid Nil Policy

- Unsupported nil comparisons such as `%{published_at: %{>: nil}}` must raise instead of warning and skipping.

### Out of Scope for This Milestone

- `having`-driven scalar behavior in `test/ecto_shorts/common_filters/query_operation_test.exs`
- aggregate functions
- date/time wrappers
- broader arithmetic behavior beyond the explicit wrapped scalar RHS case already asserted in `test/ecto_shorts/common_filters_scalar_filter_test.exs:206`
- set comparison
- any wider scalar behavior that is not required to make `test/ecto_shorts/common_filters_scalar_filter_test.exs` pass

### Structural Requirement

- `ScalarExpr` must use `EctoShorts.Compiler`.
- `ScalarExprBuilder` must remain the generated-clause implementation surface for the scalar behaviors in scope.
- The compiler/generator path is required because positional binding support depends on generated function heads that can be compiled ahead of runtime.
- The builder should follow the `CommonExprBuilder` negation structure: `expr_for/3` defines the positive scalar expression, and `specs_for/4` adds the generic negated blueprint.

## Instructions

1. Keep `DESIGN.md` updated before and after each implementation step.
2. Record the active scalar milestone, the research inventory, the current gap list, and the focused test surfaces here.
3. Treat `test/ecto_shorts/common_filters_scalar_filter_test.exs` as the primary public acceptance surface for this milestone.
4. Treat `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` as the direct expression-layer proof surface.
5. Preserve the current structures of `lib/ecto_shorts/dynamics/postgres.ex`, `lib/ecto_shorts/dynamics/postgres/common_expr.ex`, and `lib/ecto_shorts/dynamics/postgres/scalar_expr.ex` unless the user explicitly reopens them.
6. Keep all map and keyword-list normalization above the expression-module layer.
7. Treat maps and keyword lists as containers only. Expand them recursively into a flat list of resolved `{field_key, term}` entries and make routing decisions on those reduced item shapes, not on container shapes.
8. Keep `ScalarExpr` dumb. It should receive fully resolved tuple terms and return the matching dynamic expression.
9. Write direct `ScalarExpr` tests using fully resolved tuple shapes all the way down, including nested wrappers like `:not`, `:lower`, and `:upper`.
10. Follow `CommonExprBuilder` for negation: build the positive scalar expression in `expr_for/3`, then add the generic `{:not, term}` blueprint in `specs_for/4` using `Helpers.negated_expr/1`.
11. Expand one operator or wrapper family at a time and stop after each small change for summary and feedback.
12. After each change, update `DESIGN.md` with the exact change, the reason, and the smallest relevant validation result.
13. Use focused validation for this milestone:
    `mix test test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`
    `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs`
    `mix test test/ecto_shorts/dynamics/postgres_test.exs`
14. Record unrelated failures or warnings separately from the scalar-path result.

## Prevention Analysis

This section records how to prevent the same implementation drift, style drift, and reporting mistakes from happening again.

### Failure Summary

The repeated problem in this task was not a lack of effort. The problem was a failure to distinguish between:

- copying an existing pattern exactly
- partially moving toward a pattern
- inventing a new local design while claiming to follow an existing pattern

That caused several concrete mistakes:

- introducing names that did not exist in the source-of-truth module
- introducing helper shapes that did not exist in the source-of-truth module
- adding a compiler seam without actually replacing the old module shape
- reshaping the runtime API to fit the current compiler wrapper instead of correcting the integration point
- widening scope into shared infrastructure without explicit approval
- treating an intermediate scaffold as if it satisfied the request
- claiming a documentation update had been written when it had not been written
- hardening behavior with extra guards even when the scoped tests and approved behavior did not require that hardening
- leaking public container shapes into direct expression-layer tests instead of using fully resolved tuple shapes
- reasoning from `specs_for/4` argument names instead of from the generator contract that `Blueprint.key` becomes generated argument 2 and `Blueprint.head` becomes generated argument 3

### Root Causes

#### 1. Pattern-following was treated as approximation instead of exact replication

The request said to follow `CommonExpr` and `CommonExprBuilder` as the source of truth. The mistake was treating that as “copy the general idea” instead of “replicate the structure exactly unless a behavior requirement forces a deviation.”

#### 2. New names and helpers were introduced without first proving they were necessary

Examples:

- `field_key_var`
- `nil_field_expr`
- `operator` where `key` was the established local name
- a different helper signature for `field_expr`

Each of those additions increased cognitive overhead and widened the distance from the source-of-truth file.

#### 3. Intermediate states were accepted even when the request was for a final replacement

The change to `scalar_expr.ex` introduced compiler scaffolding while leaving the old hand-written implementation in place. That was a transitional hybrid state. The user asked for the module to be replaced with the compiler pattern, not for a partial migration scaffold.

#### 4. Mechanical corrections were treated as if they needed user input

When the source-of-truth file already answers a naming or structural question, asking the user to decide again is unnecessary. The correct action is to self-correct to the known source pattern.

#### 5. Reporting drift occurred

At one point a detailed prevention analysis was said to be in `DESIGN.md` when it had not actually been added. That is a verification failure. Statements about documentation status must be confirmed from the file contents, not inferred from intention.

#### 6. Infrastructure constraints were allowed to redefine the public shape

The compiler wrapper dispatches on argument 2. Instead of treating that as an implementation constraint to work around, the refactor changed `ScalarExpr` into an operator-first runtime API so it would fit the current dispatcher. That reversed the actual source-of-truth boundary, which is schema-field-first from `CommonFilters` onward.

#### 7. A discovered integration problem was treated as implicit approval to widen scope

After restoring the field-first runtime shape, the remaining mismatch moved into `EctoShorts.Compiler`. The mistake was treating that discovery as permission to edit a shared infrastructure module even though `compiler.ex` was not in the currently approved code-change scope. A real problem was found, but the correct action was to stop, explain the mismatch, and ask whether widening scope was acceptable.

#### 8. Theoretical edge-case hardening was allowed to outrun the scoped task

At one point, an extra guard was added in `ScalarExpr.dynamic_expr/4` to avoid coercing unsupported tuple and list terms into implicit equality. That was a real concern, but it was not required to make the approved tests pass, and it changed semantics without approval. The task was the minimal scalar equality behavior, not proactive hardening.

#### 9. Boundary normalization was applied only at the outermost layer

When direct `ScalarExpr` tests were expanded, some new tests converted only the outermost shape to a tuple and left nested wrapper payloads as maps. That produced partially normalized terms such as `{:not, %{==: %{lower: "hello"}}}` instead of fully resolved expression-layer terms like `{:not, {:==, {:lower, "hello"}}}`.

#### 10. The generator contract was not kept explicit while refactoring the builder

The builder refactor treated the first `specs_for/4` argument name such as `key` or `operator` as if it directly controlled the generated `dynamic_expr/3` argument positions. That was wrong.

The actual contract is in `EctoShorts.Generator.Builder.quote_def/2`:

- `Blueprint.key` becomes generated argument 2
- `Blueprint.head` becomes generated argument 3

For the scalar expression layer, that means:

- generated argument 2 must be the schema field key
- generated argument 3 must be the resolved scalar term such as `{:eq, value}` or `{:not, {:eq, value}}`

Losing track of that contract caused the generated clause shape to flip into `dynamic_expr(binding, :eq, {key, value})` instead of the required `dynamic_expr(binding, key, {:eq, value})`. It also led to negation checks against the field key variable, which can never correctly represent the scalar wrapper shape.

#### 11. The normalization contract was not defined before moving deeper into the scalar path

Work drifted into `ScalarExpr` and `ScalarExprBuilder` before the internal structure contract for `Postgres` was written down precisely enough.

That created confusion about:

- where normalization happens
- whether normalization happens once or repeatedly
- which defaults belong in normalized structure
- what shape the reducer should consume
- what the expr layer is allowed to infer

The missing design information was:

- `build_dynamic/4` owns one normalization pass
- normalization is structural only
- reducers process one normalized entry at a time
- delegation happens only after structure is stable
- expr modules receive explicit resolved terms and should not infer missing structure

#### 12. The focused test was treated as the task instead of as proof

The arithmetic acceptance test was allowed to become the center of the reasoning. Once an unrelated upstream crash appeared, the conversation drifted toward the test blocker instead of staying on the actual implementation seam.

The missing design information was:

- acceptance tests prove behavior, but they do not define the design
- source-of-truth module boundaries and documented normalized shapes come first
- if a proof path is blocked by an unrelated failure, the blocker must not become implicit scope

#### 13. An unrelated blocker was discussed as if it might justify an unrelated code change

`EctoShorts.Utils.map_to_keyword/1` became a topic because it blocked one proof path, not because it was part of the scalar arithmetic implementation.

The missing information was the need to classify every proposed unrelated change explicitly:

- needed for the feature itself
- needed only to unblock a chosen proof path
- fully out of scope

Without that classification, the discussion blurred the line between implementation and verification plumbing.

#### 14. Explanations did not always connect the immediate question to the actual dependency chain

When asked why an unrelated function would need to change, the explanation initially answered around the issue instead of directly stating the dependency chain and the distinction between:

- feature work
- proof-path work
- out-of-scope work

The missing information was the requirement to state the exact call chain and label the reason for a proposed change in plain terms.

#### 15. Documentation was briefly updated as target-state-only, which hid a current implementation mismatch

`DESIGN.md` was updated with the desired rule that bare scalar equality defaults belong before expr dispatch, but the current implementation mismatch in `ScalarExpr.normalize_term/1` was not initially recorded next to that rule.

The missing information was:

- state the intended rule
- state the current implementation
- state the mismatch explicitly
- keep both visible until the mismatch is resolved

#### 16. Errors were allowed to imply a direction before the evidence proved whether the test or the code was wrong

When a focused proof path fails, the failure does not by itself prove that the test is wrong or that the implementation is wrong.

The required rule is:

- if the observed error does not prove which side is incorrect, stop and ask for clarification before changing either the test or the code
- only proceed without clarification when the repo evidence proves the intended behavior, boundary, and target shape
- after clarification is given, record the chosen approach here before continuing so the same ambiguity does not reopen later

### Conversation-Specific Prevention Rules

Apply these rules in addition to the broader criteria above.

#### Rule 1: Do not split a user instruction into artificial option branches when the instruction already defines the contract

If the user already gave a direct rule such as “make the shape consistent and nothing else,” do not reframe that into narrower option menus unless a real ambiguity remains after checking the code.

#### Rule 2: Separate design from proof every time

Before talking about tests, restate:

- the primary implementation seam
- the layer boundary being changed
- whether the test is design evidence or only proof

If the test is only proof, do not let a blocked proof path redirect the design discussion.

#### Rule 3: Never propose an unrelated fix without labeling why it is being proposed

When an upstream blocker is unrelated to the feature:

- state that it is unrelated
- state whether it blocks implementation or only verification
- keep it out of scope unless the user expands scope

#### Rule 4: When asked “why,” answer with the direct dependency chain first

Use this format:

- requested goal
- exact call path
- exact blocking point
- whether the blocked point is part of the feature or only part of one proof path

Do not start with broader architecture if the user is asking for a direct causal explanation.

#### Rule 5: Before moving into deeper layers, write the structure contract explicitly

Before editing `ScalarExpr` or `ScalarExprBuilder`, restate:

- normalized entry shape from `Postgres`
- which defaults are explicit by then
- what `apply_expr/4` may still prepare
- what `build_expr/5` delegates
- what expr modules are still allowed to decide

If that contract is not explicit, stop and define it first.

#### Rule 6: `DESIGN.md` must record both target state and important current mismatches

If the code still violates the desired boundary in a way that matters for implementation, record the mismatch directly next to the rule it conflicts with.

Do not rely on memory or prior conversation context to reconcile the difference.

### Decision Criteria

Use these criteria before every edit in this task and in future similar refactors.

#### Criterion A: Determine whether the task is exact replication or behavior design

If the user says to follow an existing module “exactly,” “as the source of truth,” or “copy this pattern,” then default to exact replication.

Exact replication means:

- same section ordering
- same helper names
- same local variable names
- same helper signatures
- same blueprint shape
- same alias order
- same documentation markers such as `@doc false`

Only deviate if a required behavior cannot be expressed without the deviation.

When the source-of-truth file already shows how a wrapper concern is solved, copy that approach before inventing a new one. For this task, `CommonExprBuilder` already shows that negation belongs in `specs_for/4` as a wrapper around the base expression, not in duplicated `expr_for/3` clauses.

#### Criterion B: Treat every new name as a deviation

If a local name, helper name, argument, guard, or function shape does not exist in the source-of-truth file, it is a deviation.

Before introducing a deviation, answer all of these:

1. Does the source-of-truth file already solve this with an existing name or shape?
2. Is the new thing required for behavior, or is it just convenient?
3. Would a novice reading both files see the difference immediately and ask why it exists?
4. Can the deviation be justified in one sentence that refers to a concrete behavior requirement?

If the answer to any of those is “no” or “unclear,” do not add it.

#### Criterion C: Do not confuse scaffolding with completion

A structural request is complete only when the target file has the requested final shape.

For this task, `scalar_expr.ex` only counts as complete when it matches the current approved compiler-backed wrapper shape:

- builder alias
- `use EctoShorts.Compiler`
- one compiled module entry each for:
  - comparison
  - membership
  - string transform
  - string
- `operators/0`
- wrapper `dynamic_expr/5`
- `normalize_term/1` documented either as current behavior or explicitly removed in the same batch that replaces it upstream
- `compiled_module_for/2`

If any of the old hand-written scalar expression logic is still present, the module has not been fully refactored yet.

#### Criterion D: Containers are not decision points

When the user says maps and keyword lists are containers, then:

- maps are converted to keyword lists
- keyword lists are reduced recursively
- decisions are made on reduced items, not on container shapes

This rule must be applied consistently at every relevant layer.

It also applies recursively to nested scalar wrappers. If a nested payload is still a map, it has not been fully normalized yet.

#### Criterion E: Expression modules stay dumb

If an expression module is described as “only responsible for returning the dynamic expression when a matching function is called,” then it must not:

- normalize maps
- normalize keyword lists
- interpret container structure
- absorb upstream parsing responsibilities

It may only:

- match the already-resolved input shape
- return the corresponding dynamic AST

Direct tests for expression modules must follow the same rule. They must use the already-resolved expression-layer input shape, not public `CommonFilters` container shapes.

#### Criterion F: Do not ask the user to decide things already answered by the source pattern

Ask for clarification only when:

- behavior is ambiguous
- scope is ambiguous
- a required deviation has no existing precedent

Do not ask for clarification when:

- the answer already exists in the source-of-truth module
- the issue is purely naming or structural consistency
- the correct action is simply to remove an invented deviation

#### Criterion G: The external data shape wins over internal convenience

If the repository boundary already defines the runtime shape, keep that shape stable even when the current helper, compiler, or generator implementation makes it awkward.

For this task, that means:

- `CommonFilters` and `Postgres` establish a schema-field-first flow
- `ScalarExpr` must stay schema-field-first
- the operator belongs inside the third-argument tuple
- the compiler/builder integration must adapt to that fact
- the API shape must not be bent to fit a convenient dispatcher

There are two valid shape layers in this task:

- public / end-to-end layer: the `CommonFilters` / `Postgres` container shapes
- expression layer: fully resolved tuple shapes used by `ScalarExpr`

Do not mix those two layers in the same test or implementation decision.

#### Criterion H: Shared infrastructure is out of bounds until scope is explicitly widened

If a fix appears to require changes to a shared module such as `compiler.ex`, `generator.ex`, or another cross-cutting abstraction:

- do not change it just because the local module now exposes the mismatch
- treat the mismatch as a scope boundary
- explain exactly why the local code no longer fits the shared abstraction
- ask for approval before editing the shared infrastructure module

Finding a real mismatch is not approval to widen scope.

#### Criterion I: Do not harden behavior beyond the approved proof surface

If a new guard, branch, fallback, or restriction is not required by:

- the approved behavior specification
- a failing test that is already in scope
- or a user-approved semantic clarification

then do not add it.

Concerns about possible unsupported terms, future operators, or suspicious edge cases are not enough by themselves. Record the concern if needed, but do not change behavior unless the task requires it.

#### Criterion J: Acceptance tests and direct tests prove different boundaries

Use the right test layer for the right decision:

- `test/ecto_shorts/common_filters_scalar_filter_test.exs` proves the public end-to-end scalar filter behavior, including container normalization
- `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` proves only the direct expression-layer behavior after normalization has already happened

If a direct `ScalarExpr` test contains nested maps like `%{lower: "hello"}` or `%{==: 10}`, that is a boundary mistake unless the expression layer is explicitly supposed to normalize that shape.

#### Criterion K: Reuse the source-pattern negation wrapper instead of duplicating operator logic

If the source-of-truth builder solves negation by:

- building the positive expression once
- adding a generic `{:not, term}` blueprint in `specs_for/4`
- wrapping that expression with `Helpers.negated_expr/1`

then use that same pattern.

Do not:

- create a separate base `:not` expression family in `expr_for/3`
- duplicate every operator family with manual negated AST
- flip operators manually unless the approved source pattern or behavior explicitly requires it

#### Criterion L: Derive generated clause shape from `Blueprint`, not from local builder argument names

Before changing any builder that feeds `EctoShorts.Generator.Builder.quote_def/2`, restate the contract explicitly:

- `Blueprint.key` becomes generated argument 2
- `Blueprint.head` becomes the remaining generated arguments after the key

For the current scalar builder, `Blueprint.head` is `[negated, value]`, so the generated shape is:

- `dynamic_expr(selected_binding, key, negated, value)`
- not `dynamic_expr(selected_binding, :operator, {key, value})`

That means:

- the schema field key belongs in `Blueprint.key`
- the negation flag and resolved value shape belong in `Blueprint.head`
- the scalar operator wrapper is carried inside the resolved `value` term
- negation checks must inspect the resolved scalar term shape, not the field key variable

If a refactor idea cannot preserve those facts, the idea is wrong even if the local builder code looks simpler.

### Pre-Change Checklist

Before editing a function, answer these questions in order:

1. What exact file is the source-of-truth pattern for this change?
2. What exact function or section am I changing?
3. Is the requested outcome a final replacement or only a preparatory step?
4. Am I introducing any new names not present in the source-of-truth file?
5. Am I introducing any new helper signatures not present in the source-of-truth file?
6. Am I leaving behind any old implementation that the request implied should be replaced?
7. Can I point to the exact line or shape in the source-of-truth file that justifies the change?
8. Am I preserving the boundary’s established data shape, or quietly reshaping it to fit an internal helper?
9. Does this change touch a shared infrastructure module that is outside the currently approved scope?
10. If I summarize the change in one sentence, does it describe a completed step rather than a vague move in the right direction?
11. Am I changing behavior only because of a theoretical concern rather than an approved failing case?
12. Am I writing this test at the correct layer: public container shape for end-to-end tests, fully resolved tuple shape for direct expression tests?

If any answer is uncertain, stop before editing.

### Post-Change Verification Checklist

After editing a function, verify all of these before responding:

1. Read the changed file, not just the patch.
2. Check that any names introduced actually exist in the source-of-truth style, or document the justified deviation.
3. Check that no banned intermediate state remains.
4. Check that the summary statement matches what is literally in the file.
5. Check that the runtime/API shape still matches the clarified boundary and was not silently reshaped to fit an internal implementation detail.
6. Check that no shared infrastructure file was changed without prior approval.
7. If claiming `DESIGN.md` was updated, verify that the section text actually exists in the file.

### Stop Rules

Stop and self-correct before continuing when any of these happen:

- a new local name is invented without explicit need
- a new helper appears that is not in the source-of-truth file
- an extra function argument appears without explicit justification
- a hybrid transitional state remains after a step that was supposed to be a replacement step
- a public or runtime argument shape is being changed mainly to fit an internal compiler, generator, or helper limitation
- a discovered mismatch is pushing the work toward `compiler.ex` or another shared module that is not in scope
- a new guard or stricter branch is being added even though the approved tests and behavior spec do not require it
- a response sentence begins to describe intention rather than file reality

### Rules for Future Similar Tasks

When a user says “follow X exactly”:

- use X as the coding-style contract
- copy names exactly
- copy helper signatures exactly
- copy helper placement exactly
- copy section ordering exactly
- do not optimize, generalize, or “improve” the structure unless the user asks

When a user says “do not add new things”:

- treat every invented local, helper, or abstraction as prohibited by default
- remove the deviation instead of defending it

When a user says “make one small change at a time”:

- choose a step that is independently valid
- do not justify a partial scaffold as if it were a final refactor
- stop after the exact function or section that changed

When an internal tool or abstraction fights the established API shape:

- treat the API shape as the source of truth
- do not redesign the runtime arguments for convenience
- document the mismatch explicitly
- correct the integration point rather than the public shape

When the needed integration point is a shared infrastructure module:

- stop before editing it
- state that the change is outside current scope
- explain the exact mismatch and why local-only changes no longer fit
- wait for approval before widening scope

When a possible edge-case bug is noticed but the task is narrowly scoped:

- check whether the approved tests or behavior spec actually require a change
- if not, do not tighten the implementation
- record the concern only if it will be important later
- prefer preserving the current approved behavior over speculative hardening

### Practical Rule for This Task

For the remainder of this refactor:

- `CommonExpr` and `CommonExprBuilder` are the structural source of truth
- behavior-specific differences are allowed only where the current scalar-filter milestone requires them
- style differences are not allowed unless explicitly clarified
- if a difference is not behavior-required and not user-approved, remove it
- keep the current structures of `postgres.ex`, `common_expr.ex`, and `scalar_expr.ex` intact unless the user explicitly asks for a structural change

## Progress

Legend:
- `[ ]` not started
- `[~]` in progress
- `[x]` completed

- [x] Create `DESIGN.md` before implementation edits.
- [x] Record the scalar research inventory, interpretation checks, and current milestone behavior spec.
- [x] Align `DESIGN.md`, `PLAN.md`, and `BINDING_REFACTOR_PLAN.md` with the scalar-filter milestone.
- [x] Build the direct `ScalarExpr` proof surface in `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`.
- [x] Update the invalid-nil scalar acceptance test to the approved raise behavior.
- [x] Normalize the explicit wrapped arithmetic RHS payload in `lib/ecto_shorts/dynamics/postgres.ex` into the resolved tuple shape expected by the scalar expression layer.
- [x] Add the direct wrapped arithmetic scalar proof in `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`.
- [x] Re-check whether any `ScalarExprBuilder` change is still needed after the `Postgres` normalization batch.
- [ ] Implement the remaining scalar operator and wrapper families in the scalar code path.
- [ ] Run focused validation for `scalar_expr_test.exs`, `common_filters_scalar_filter_test.exs`, and `postgres_test.exs`.
- [ ] Record final scalar-filter milestone results and unrelated warnings separately.

## Pre-Task Validation

Current scalar-filter milestone assumptions:
- Assumption A1: `ScalarExpr` must not normalize maps or keyword lists. Status: clarified by user.
- Assumption A2: Resolution into tuple input shape must happen before `ScalarExpr.dynamic_expr/5`. Status: clarified by user.
- Assumption A3: Direct `ScalarExpr` tests must use fully resolved tuple terms all the way down. Status: clarified by user.
- Assumption A4: Existing tests are evidence, but `test/ecto_shorts/common_filters_scalar_filter_test.exs` is now the primary public acceptance surface for this milestone. Status: grounded by repo review.
- Assumption A5: The current structures of `postgres.ex`, `common_expr.ex`, and `scalar_expr.ex` are user-approved and should not be restructured casually. Status: clarified by user.
- Assumption A6: The milestone currently excludes `having`, aggregate, date/time, wider arithmetic beyond the explicit wrapped scalar RHS case, and set-comparison behavior unless a scalar-filter implementation step proves they are immediately required. Status: clarified during planning.

Current boundary review:
- `EctoShorts.CommonFilters` calls `EctoShorts.Adapters.Postgres.build_dynamic/4`.
- `EctoShorts.Adapters.Postgres` owns container handling and selected-binding validation, then routes non-array, non-common keys to `EctoShorts.Dynamics.Postgres.ScalarExpr.dynamic_expr/5`.
- `EctoShorts.Dynamics.Postgres.ScalarExpr` is the direct expression layer and should only receive resolved tuple terms.
- `ScalarExprBuilder` and the compiler path are already in place; the remaining work is extending the generated scalar clauses to cover the missing scalar families.
- The next scalar-builder correction is structural as well as behavioral: negation should follow the shared `CommonExprBuilder` `specs_for/4` wrapper pattern instead of being treated as a standalone base expression family.
- The next `Postgres` correction is to normalize containers into a flat list of resolved `{field_key, term}` entries so nested public scalar shapes are fully expanded before routing.

Current repo-grounded failure surface:
- `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs` fails for comparison operators and aliases beyond equality, list semantics, negation, string matching, and lower/upper transforms.
- The invalid-nil scalar acceptance case has been updated to require a raise instead of warning-and-skip.
- `test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs` now provides the direct expression-layer proof surface needed before changing builder behavior.

## Post-Task Validation

Planned validation commands:
- `mix test test/ecto_shorts/compiler_test.exs`
- `mix test test/ecto_shorts/common_filters_scalar_filter_test.exs`
- `mix test test/ecto_shorts/dynamics/postgres/scalar_expr_test.exs`
- `mix test test/ecto_shorts/dynamics/postgres_test.exs`

Results:
- `mix test test/ecto_shorts/compiler_test.exs` passed with `7 tests, 0 failures`.
- That run intentionally emitted generated-module compile errors and warnings as part of the compiler error-reporting coverage in the test file.
- `mix test test/ecto_shorts/common_filters_test.exs` passed with `8 tests, 0 failures`.
- `mix test test/ecto_shorts/dynamics/postgres_test.exs` passed with `2 tests, 0 failures`.
- `mix test test/ecto_shorts/common_filters_test.exs test/ecto_shorts/dynamics/postgres_test.exs` passed with `10 tests, 0 failures`.
- Unrelated warnings still come from `lib/ecto_shorts/common_filters.old.ex` and one compiler redefinition warning was emitted for `EctoShorts.Dynamics.Postgres.ScalarExpr.Compiled` during recompilation.
- Current scalar-filter milestone validation is still in progress. The focused `common_filters_scalar_filter_test.exs` suite is not green yet, so this section should be treated as partial history rather than final milestone completion.
