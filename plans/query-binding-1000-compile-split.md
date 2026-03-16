# Query Binding 1000 Compile-Time Split

Increase the live positional binding limit to `1000`, measure the clean-compile cost of that end state, and reduce the resulting compile-time regression by splitting the behavior-heavy `query_binding_contracts/2` consumers into thin generated shims plus extracted runtime logic while preserving public query semantics.

This is the governing `ExecPlan` for the task. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

## Scope and intent

The user asked for three coupled outcomes:

1. make positional binding support reach `1000`
2. profile how long the project currently takes to compile
3. split the modules that use query binding contracts so compilation becomes faster

The change is behavior-bearing because the live binding-selector contract reaches deeper positional bindings after the change. The public filter API and query semantics must otherwise remain unchanged.

## In scope

- Measure clean compile time in the repo's normal development compile environment and record the exact command and result.
- Make `EctoShorts.QueryBinding.query_binding_contracts/2` use its module attribute default of `1000` for compile-time contract generation without changing `config/config.exs` or `EctoShorts.Config`.
- Preserve the existing public binding selector surface: `{:as, nil}`, `{:as, name}`, and `{:at, position}`, including `:first` and `:last` resolution at the `EctoShorts.CommonFilters` boundary.
- Refactor the measured `query_binding_contracts/2` hotspot consumers so their generated clauses become thin wrappers around non-generated runtime logic.
- Add or update focused tests that prove the `1000`-binding contract and preserve behavior in the touched modules.
- Re-measure clean compile time after the refactor and record the comparison.

## Out of scope

- Changing public filter payload shapes or dynamic-expression semantics.
- Rewriting every consumer unconditionally if the baseline shows some are not meaningful compile-time contributors.
- General cleanup unrelated to the positional-binding and compile-time goals.

## Module specifications

### `EctoShorts.QueryBinding`

This module owns generation of binding-contract AST and helper functions for positional and named bindings. Callers use `query_binding_contracts/2` when they need compile-time binding-list shapes that Ecto macros can match. It does not own filter semantics, dynamic-expression semantics, or application-config policy for unrelated runtime boundaries.

### `EctoShorts.CommonFilters`

This module owns the public filter-routing contract. `convert_params_to_filter/3` is the caller-facing entry point that resolves top-level filter params, translates `:as` and `:at` selectors into the active binding selector, and delegates each filter family to the appropriate builder through `build_query/6` and `EctoShorts.CommonFilters.API`. The live binding-selector semantics must remain stable across this task.

### `EctoShorts.DynamicBuilders.Postgres`

This module owns Postgres-specific dynamic-expression dispatch. `build_dynamic/4` normalizes one public filter entry and routes it to `CommonExpr`, `ArrayExpr`, or `ScalarExpr` based on operator and field type. The task may restructure internal helpers, but the public adapter contract and returned `Ecto.Query.DynamicExpr` semantics must remain unchanged.

### Hotspot consumer modules

The likely compile-time hotspots are the modules that still generate substantial function bodies once per binding contract: `EctoShorts.CommonFilters.Join`, `Select`, `OrderBy`, `Windows`, and `EctoShorts.DynamicBuilders.Postgres.ArrayExpr`. These modules should continue to own their filter-family or expression-family behavior, but their generated loops should stop duplicating large runtime decisions.

## Function specifications

### `EctoShorts.QueryBinding.query_binding_contracts/2`

- Accepts a caller context module and optional keyword options.
- Accepts `positions: integer()` as an explicit override.
- Returns `{target_binding_var, binding_patterns}` where `binding_patterns` includes:
  - the default binding contract `{:as, nil}`
  - the named binding contract `{:as, binding_alias}`
  - positional binding contracts `{:at, 1}` through `{:at, max}`
- After this task, the default `query_binding_contracts/2` path must include a `{:at, 1000}` contract through the module attribute unless the caller passes `positions:`.
- This function may change internal implementation, but callers must still be able to use the returned binding patterns in `for`-generated Ecto clauses.

### `EctoShorts.CommonFilters.convert_params_to_filter/3`

- Accepts the public source and filter params.
- Preserves the documented meaning of top-level `:as` and `:at` selectors.
- Delegates selected-binding work through `build_query/6` without changing user-visible query semantics.
- This task must not change the result shape or the meaning of existing binding selectors other than allowing deeper positional defaults.

### `EctoShorts.DynamicBuilders.Postgres.build_dynamic/4`

- Accepts a source, selected binding, one filter entry, and options.
- Returns an `Ecto.Query.DynamicExpr` for that entry.
- Must continue to route array-like fields to `ArrayExpr` and scalar-like fields to `ScalarExpr` with no user-visible semantic drift.
- Internal extraction is allowed as long as the observable dynamic-expression behavior stays the same for the executed cases.

## Internal boundary contracts

- `CommonFilters.convert_params_to_filter/3`
  -> `CommonFilters.build_query/6`
  -> `CommonFilters.API.build_query/6`
  -> filter-family module such as `Join`, `Select`, `OrderBy`, or `Windows`

- `DynamicBuilders.build_dynamic/4`
  -> `DynamicBuilders.Postgres.build_dynamic/4`
  -> `ArrayExpr.dynamic_expr/5`, `ScalarExpr.dynamic_expr/5`, or `CommonExpr.dynamic_expr/5`

- `QueryBinding.query_binding_contracts/2` is the compile-time seam. Consumer modules may keep compile-time-generated heads and binding bodies, but behavior-heavy work should move behind non-generated helpers or extracted neighbor modules compiled once.

## Chosen approach

Evaluate the work against these alternatives before and during implementation:

1. Raise the effective limit to `1000` and leave the heavy generated modules alone.
   - Rejected as the final shape because it satisfies the contract goal but not the compile-time goal.

2. Replace most generated binding contracts with runtime branching over selector terms.
   - Rejected unless a tiny local case proves safe, because Ecto binding-list literals are part of the compile-time boundary and broad runtime replacement risks semantic drift.

3. Keep compile-time binding heads where Ecto requires them, but extract runtime shaping, normalization, and branching into non-generated functions or adjacent helper modules.
   - Chosen because it matches the proven `scalar_expr.ex` direction and directly targets AST duplication while preserving the current contract model.

## Plan of work

### Milestone 1 - Baseline the requested end state

Record the clean compile baseline from the repo root using a command that is easy to repeat. Capture the environment, the exact command, and the wall-clock result.

Preferred command:

    env MIX_ENV=dev /usr/bin/time -lp sh -c 'mix clean && mix compile'

Also record the effective positional-binding limit used during that compile. If the current dev compile is still pinned to `10`, explicitly capture that fact, then record a second before-state for the unoptimized `1000`-binding configuration so the optimization is measured against the requested end state rather than the easier old default.

### Milestone 2 - Make the `query_binding` default use the module attribute

Keep config out of scope. Remove the config dependency from `query_binding_contracts/2` so the compile-time contract generator uses `@default_max_positional_bindings` unless a caller passes `positions:`. Remove stray debug output from the limit-resolution path if present. Update direct tests so the existence of the `{:at, 1000}` contract is proven without giant brittle assertions.

### Milestone 3 - Split the measured hotspots

Refactor the measured hotspot consumers so each generated clause only performs the minimum binding-literal work and delegates the rest to runtime helpers or extracted modules. Start with the worst contributors from the compile baseline. Keep the public owner modules in place; extracted modules should hold only non-generated logic.

Likely first targets:

- `lib/ecto_shorts/common_filters/join.ex`
- `lib/ecto_shorts/common_filters/select.ex`
- `lib/ecto_shorts/common_filters/order_by.ex`
- `lib/ecto_shorts/common_filters/windows.ex`
- `lib/ecto_shorts/dynamic_expressions/postgres/array_expr.ex`

### Milestone 4 - Verify behavior and capture the compile win

Run focused suites for the touched families, then broader regression checks. Re-run the clean compile timing with the same command and compare against the before-state. Record both the evidence and the residual uncertainty.

## Validation matrix

| Claim | Boundary | Evidence | Command / step | Notes |
| --- | --- | --- | --- | --- |
| The ordinary default positional-binding contract reaches `1000` | `QueryBinding.query_binding_contracts/2` | Direct unit test | `mix test test/ecto_shorts/query_binding_test.exs` | Add/update focused assertions for `{:at, 1000}` |
| Join behavior is preserved after splitting | `CommonFilters.convert_params_to_filter/3` through `Join` | Existing boundary tests | `mix test test/ecto_shorts/common_filters/common_filters_join_test.exs test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_join_test.exs` | Covers executed join cases only |
| Select behavior is preserved after splitting | `CommonFilters.convert_params_to_filter/3` through `Select` | Existing boundary tests | `mix test test/ecto_shorts/common_filters/common_filters_select_test.exs test/ecto_shorts/common_filters/common_filters_select_merge_test.exs test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_select_test.exs test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_select_merge_test.exs` | Covers executed projection cases only |
| Windows behavior is preserved after splitting | `CommonFilters.convert_params_to_filter/3` through `Windows` | Existing boundary tests | `mix test test/ecto_shorts/common_filters/common_filters_windows_test.exs test/ecto_shorts/common_filters_schemaless/common_filters_schemaless_windows_test.exs` | Covers executed window cases only |
| Array expression behavior is preserved after splitting | `DynamicBuilders.Postgres.build_dynamic/4` through `ArrayExpr` | Existing targeted tests | `mix test test/ecto_shorts/dynamic_expressions/postgres/array_expr_test.exs` | Covers executed array-expression cases only |
| Compile time improves relative to the unoptimized `1000` before-state | Clean compile boundary | Timing comparison | `env MIX_ENV=dev /usr/bin/time -lp sh -c 'mix clean && mix compile'` before and after | Environmental noise remains; compare same machine and command |
| Broader behavior remains green | Repo test surface | Regression evidence | Choose the narrowest broader suite warranted by touched files, potentially `mix test` if changes are wide enough | Passing tests do not prove unexecuted cases |

## Progress

- [x] 2026-03-16: Loaded `.agent/RULES.md`, `.agent/PLANS.md`, and the required spec/testing guides.
- [x] 2026-03-16: Mapped the `QueryBinding` consumers and the public boundary chain through `CommonFilters` and `DynamicBuilders.Postgres`.
- [x] 2026-03-16: Confirmed the current inconsistency between `@default_max_positional_bindings 1000` and the dev config override of `10`.
- [x] 2026-03-16: Recorded the current dev before-state with `env MIX_ENV=dev /usr/bin/time -lp sh -c 'mix clean && mix compile'` at an effective default limit of `10`; wall time `33.06s`.
- [x] 2026-03-16: Implemented the `1000`-default alignment in `lib/ecto_shorts/query_binding.ex` and reduced generated branching in the main compile-time hotspot families.
- [x] 2026-03-16: Recorded the unoptimized module-attribute `1000` pre-refactor baseline with `env MIX_ENV=dev /usr/bin/time -lp sh -c 'mix clean && mix compile'`; wall time `1451.02s`.
- [x] 2026-03-16: Verified the touched files compile with `mix compile` after the refactor pass.
- [x] 2026-03-16: Recorded the post-refactor clean compile timing with the same command; wall time `379.90s`.

## Surprises & Discoveries

- `lib/ecto_shorts/query_binding.ex` already declares `@default_max_positional_bindings 1000`, and the user clarified that this module attribute is the intended scope boundary; config changes are out of scope for this task.
- `config/config.exs` still sets `max_positional_bindings: 10` in the non-test branch, so the historical current-state compile baseline remains `33.06s`, but the optimization work should now measure the compile-time end state through the `query_binding` module-attribute path rather than by changing config.
- The current dev clean-compile before-state of `33.06s` is therefore a historical baseline only; the relevant pre-refactor optimization baseline is the clean compile after `query_binding_contracts/2` is switched to its module-attribute default and before the hotspot splits land.
- `EctoShorts.QueryBinding.query_binding_contracts/2` needed to stop consulting config for this task so the module-attribute `1000` path is the actual compile-time source of truth during measurement.
- `scalar_expr.ex` already demonstrates the desired thin-shim pattern, so the remaining heavier generated consumers are better first optimization targets.
- Collapsing the per-binding branching in `select`, `array_expr`, `common_expr`, `order_by`, `group_by`, `distinct`, and `windows` was enough to cut clean compile wall time from `1451.02s` to `379.90s` without needing a second-pass `join` rewrite.
- `Query.windows/2` accepts pinned inner values inside the window definition, but not a fully pinned outer window-definition term.
- `fragment/1` style SQL strings still need to remain literal strings at macro expansion time; trying to pin the fragment SQL text in `array_expr.ex` fails compilation.

## Decision Log

- 2026-03-16: Use a new governing plan in `./plans/query-binding-1000-compile-split.md` rather than reusing `scalar-compile-time-opt.md`, because the prior plan explicitly left `10` in place and does not govern this task.
- 2026-03-16: Use measurement-driven hotspot selection instead of splitting every consumer by default.
- 2026-03-16: Prefer thin generated shims plus extracted runtime helpers over broad runtime selector branching.
- 2026-03-16: Keep config out of scope; use `EctoShorts.QueryBinding`'s module attribute as the compile-time default source of truth for this task.
- 2026-03-16: Stop after the first refactor pass once the clean compile dropped from `1451.02s` to `379.90s`; that is a large enough win to satisfy the requested baseline -> refactor -> remeasure flow without adding a more invasive `join` rewrite.

## Outcomes & Retrospective

Current recorded measurements:

- Historical current-state baseline: `env MIX_ENV=dev /usr/bin/time -lp sh -c 'mix clean && mix compile'`
  - Effective default positional binding limit: `10`
  - Wall time: `33.06s`

- Task-relevant pre-refactor baseline: `env MIX_ENV=dev /usr/bin/time -lp sh -c 'mix clean && mix compile'`
  - Effective default positional binding limit for this task: module-attribute `1000` path in `lib/ecto_shorts/query_binding.ex`
  - Wall time: `1451.02s`

- Post-refactor after-state: `env MIX_ENV=dev /usr/bin/time -lp sh -c 'mix clean && mix compile'`
  - Effective default positional binding limit for this task: module-attribute `1000` path in `lib/ecto_shorts/query_binding.ex`
  - Wall time: `379.90s`

- Compile-time delta for the task-relevant baseline:
  - Improvement: `1071.12s` faster wall time
  - Relative improvement: about `73.8%`

Touched implementation files:

- `lib/ecto_shorts/query_binding.ex`
- `lib/ecto_shorts/dynamic_expressions/postgres/common_expr.ex`
- `lib/ecto_shorts/dynamic_expressions/postgres/array_expr.ex`
- `lib/ecto_shorts/common_filters/order_by.ex`
- `lib/ecto_shorts/common_filters/group_by.ex`
- `lib/ecto_shorts/common_filters/distinct.ex`
- `lib/ecto_shorts/common_filters/select.ex`
- `lib/ecto_shorts/common_filters/windows.ex`

Validation executed:

- `mix compile`
- `env MIX_ENV=dev /usr/bin/time -lp sh -c 'mix clean && mix compile'`

Remaining uncertainty / follow-up:

- `join.ex` still appears in the clean compile "taking more than 10s" output and remains the largest untouched hotspot from the original target set.
- Focused runtime suites were not run in this pass because the user requested a minimal baseline -> refactor -> remeasure workflow unless the refactor broke compilation or obvious boundaries.
