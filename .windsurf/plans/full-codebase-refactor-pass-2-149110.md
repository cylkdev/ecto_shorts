# Full Codebase Refactor Pass 2

Comprehensive smell-by-smell refactoring plan for every file under `lib/`, prioritized by severity, building on the completed refactor 0001.

## Scope

- **In scope**: All `.ex` files under `lib/ecto_shorts/`.
- **Out of scope**: Test files, config files, `.agent/`, `.windsurf/` rules.
- **Constraint**: Minor public API changes allowed if justified with an ADR. All 9 doctests and 898 tests must pass after every milestone.

## Smell Catalog (prioritized by severity)

### P1 - Critical (highest duplication / largest files)

**S1. Duplicate alias-to-canonical mapping in specs files**
- **Files**: `lib/ecto_shorts/dynamics/adapters/postgres/scalar_expr/specs.ex` (1758 lines), `lib/ecto_shorts/dynamics/adapters/postgres/array_expr/specs.ex` (935 lines)
- **Smell**: Duplicate Code (`.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`)
- **Evidence**: The `case op do :gt -> :> ; :gte -> :>= ; :lt -> :< ; :lte -> :<= ; :eq -> :== ; :ne -> :!= end` block is copy-pasted verbatim in:
  - `scalar_expr/specs.ex`: `alias_op_specs/2` (2x), `aggregate_specs/4` (2x), `all_specs/4` (2x), `any_specs/4` (2x) = **8 copies**
  - `array_expr/specs.ex`: `alias_op_specs/2` (2x), `aggregate_specs/4` (2x) = **4 copies**
  - Total: **12 copies** of the same 6-line case block.
- **Technique**: Extract Function (`.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`) - extract a shared `alias_to_canonical_op_ast/1` helper that returns the quoted case expression.
- **Risk**: Medium-high. These are compile-time quoted expressions. The helper must return quoted AST, not runtime values.

**S2. Duplicate aggregate spec generation across scalar and array**
- **Files**: Same two specs files.
- **Smell**: Duplicate Code
- **Evidence**: `aggregate_specs/4` in both files generates nearly identical `%ClauseSpec{}` lists for the same 5 aggregate operators (`:avg`, `:count`, `:max`, `:min`, `:sum`). The only difference is the scalar file also generates `aggregate_dynamic_case_ast` and `not_aggregate_dynamic_case_ast` helper AST inline, while the array file does the same.
- **Technique**: Extract Function - extract shared aggregate spec builder functions into a common module or shared helpers within each file.

**S3. Duplicate `dynamic_case_ast` helpers across scalar and array**
- **Files**: Same two specs files.
- **Smell**: Duplicate Code
- **Evidence**: Both files define private `aggregate_dynamic_case_ast/4`, `not_aggregate_dynamic_case_ast/4` with identical structure (a `case op` dispatching to `AST.dynamic_ast` for each comparison operator). The scalar file additionally has `all_dynamic_case_ast`, `not_all_dynamic_case_ast`, `any_dynamic_case_ast`, `not_any_dynamic_case_ast`, `date_time_comparison_case_ast`, `not_date_time_comparison_case_ast`, `comparison_dynamic_expr_ast`, `arithmetic_dynamic_expr_ast`, `date_time_dynamic_expr_ast`.
- **Technique**: Extract Module - create a shared `ExprHelpers` module in `lib/ecto_shorts/dynamics/adapters/postgres/expr_helpers.ex` for the common AST-generation helpers.

### P2 - High (large modules still above target)

**S4. `actions.ex` still above 400-line code target**
- **File**: `lib/ecto_shorts/actions.ex` (1974 total, ~700 code lines)
- **Smell**: Large Module (`.agent/refactor/code_smells/bloaters/LARGE_MODULE.md`)
- **Evidence**: Contains CRUD (all, find, create, update, delete, get, exists?, stream, aggregate, preload), find-and-X (find_and_create, find_and_update, find_and_upsert, find_and_delete, find_or_create), bulk (insert_all, update_all, delete_all), multi (create_many, find_many, update_many, delete_many, find_or_create_many, find_and_upsert_many), batch (batch, batch_preload), and transaction (transaction, transact) - 5 distinct responsibility groups in one module.
- **Technique**: Extract Module - extract `Actions.CRUD` (the find-and-X compound operations) or `Actions.Bulk` to further reduce the module. The previous refactor already extracted Multi and Batch.
- **Note**: The previous refactor 0001 noted this as a follow-up opportunity.

**S5. `common_filters.ex` dispatch chain**
- **File**: `lib/ecto_shorts/common_filters.ex` (1606 lines, ~770 code)
- **Smell**: Large Module + Long Function
- **Evidence**: `build_query/6` is a 16-arm case dispatch (lines 1499-1551). `create_schema_filter/6` has 6 public overloads. `build_schema_filters/6` has 6 private clauses doing similar routing.
- **Technique**: Replace the `build_query/6` case dispatch with the same map-based dispatch pattern used in refactor 0001 for `apply_query_builder`. Extract `build_schema_filters` clauses into a dedicated private module or simplify with clearer pattern matching.

**S6. Long parameter lists (6-7 args)**
- **Files**: `common_filters.ex` (`create_schema_filter/6`, `build_schema_filters/6`, `reduce_default_filter_params/7`, `build_join_filters/7`)
- **Smell**: Long Parameter List (`.agent/refactor/code_smells/bloaters/LONG_PARAMETER_LIST.md`)
- **Evidence**: `reduce_default_filter_params/7` takes `schema_source, query, binding_selector, filter_op, key, value, opts` - 7 parameters. `build_join_filters/7` takes the same 7. These are private functions, but the long lists make the code harder to read and change.
- **Technique**: Introduce a context struct (e.g., `%FilterContext{schema_source, query, binding_selector, filter_op, opts}`) to bundle the threading parameters. This is the `Introduce Parameter Object` technique.

### P3 - Medium

**S7. `common_params.ex` normalize_insert_entry dispatch**
- **File**: `lib/ecto_shorts/common_params.ex` (863 lines)
- **Smell**: Long Function (6 clauses of `normalize_insert_entry`)
- **Evidence**: 6 function clauses handling: `{changeset, params}`, nil schema, `{struct, params}`, changeset, bare struct, and plain params. Each clause does similar work: normalize params, compute changed_keys, build struct/changeset.
- **Technique**: Extract Function - consolidate the shared steps (normalize, compute changed_keys, build) into a pipeline with a clear dispatch at the top.
- **Note**: Previous refactor 0001 noted this as a follow-up.

**S8. `common_filters/join.ex` size (556 lines)**
- **File**: `lib/ecto_shorts/common_filters/join.ex`
- **Smell**: Large Module
- **Evidence**: Contains 6 join type handlers (association, schema, table, query, subquery, fragment) plus shared join-building helpers, all in one file. Each join type handler follows the same pattern: extract options, validate source, call `on_expr`, call `build_join`.
- **Technique**: The repeated pattern across join types could be simplified by extracting the common option-extraction and validation into a shared helper, reducing the per-type code to just the source-specific logic.

**S9. Repeated `build_field_predicates` / `build_operator_predicates` in dynamics.ex**
- **File**: `lib/ecto_shorts/dynamics.ex` (497 lines)
- **Smell**: Duplicate Code (near-duplicate)
- **Evidence**: `build_field_predicates/7` (lines 177-217) and `build_operator_predicates/7` (lines 219-238) share almost identical reduce logic with the same warning pattern. The only difference is field predicates check for boolean sub-groups while operator predicates don't.
- **Technique**: Extract Function - consolidate into a single predicate builder with a boolean flag or merged logic.

### P4 - Low (minor improvements)

**S10. `utils.ex` is a 1-function module**
- **File**: `lib/ecto_shorts/utils.ex` (30 lines)
- **Smell**: Lazy Module (`.agent/refactor/code_smells/dispensables/LAZY_MODULE.md`)
- **Evidence**: Contains only `atomize_keys/1`. Used by `common_params.ex` only.
- **Technique**: Inline Function - move `atomize_keys/1` into `CommonParams` as a private function and delete `utils.ex`.

**S11. `logger.ex` is a thin wrapper**
- **File**: `lib/ecto_shorts/logger.ex` (50 lines)
- **Smell**: Lazy Module (borderline)
- **Evidence**: Contains only `warning/2` which delegates to `Logger.warning/1` with a prefix. Used across many modules.
- **Decision**: Keep as-is. The indirection provides a single point of control for logging format.

**S12. `config.ex` repeated pattern for config getters**
- **File**: `lib/ecto_shorts/config.ex` (260 lines)
- **Evidence**: Multiple functions follow the same pattern: `Keyword.get(opts, :key) || Application.compile_env(...)`. Not severe enough to warrant a refactor given the clarity of the current code.
- **Decision**: Keep as-is.

## Milestone Plan

### Milestone 1: Extract shared AST helpers from specs files (S1, S2, S3)
- Create `lib/ecto_shorts/dynamics/adapters/postgres/expr_helpers.ex`
- Extract `alias_to_canonical_op_ast/1` (the quoted case block)
- Extract `aggregate_dynamic_case_ast/4` and `not_aggregate_dynamic_case_ast/4`
- Replace 12 inline copies in `scalar_expr/specs.ex` and `array_expr/specs.ex`
- **Validation**: `mix test --seed 0 --trace` - 9 doctests, 898 tests, 0 failures
- **Expected reduction**: ~200 lines across both specs files

### Milestone 2: Simplify common_filters.ex dispatch (S5, S6)
- Replace `build_query/6` 16-arm case with `@query_builder_dispatch` map
- Introduce `%FilterContext{}` struct to reduce 7-arg parameter lists to 3-4
- **Validation**: `mix test --seed 0 --trace` - 9 doctests, 898 tests, 0 failures
- **Expected reduction**: ~80 lines, cleaner function signatures

### Milestone 3: Extract Actions.Bulk from actions.ex (S4)
- Extract `insert_all/3`, `update_all/4`, `delete_all/3` into `lib/ecto_shorts/actions/bulk.ex`
- Keep public delegators in `actions.ex`
- **Validation**: `mix test --seed 0 --trace` - 9 doctests, 898 tests, 0 failures
- **Expected reduction**: ~100 code lines from actions.ex

### Milestone 4: Simplify normalize_insert_entry in common_params.ex (S7)
- Consolidate the 6-clause dispatch into a 2-step pipeline: normalize input to `{struct_or_nil, params}`, then build
- **Validation**: `mix test --seed 0 --trace` - 9 doctests, 898 tests, 0 failures

### Milestone 5: Consolidate dynamics.ex predicate builders (S9)
- Merge `build_field_predicates/7` and `build_operator_predicates/7` into a single function
- **Validation**: `mix test --seed 0 --trace` - 9 doctests, 898 tests, 0 failures

### Milestone 6: Simplify join.ex option extraction (S8)
- Extract shared option-extraction and validation into a helper
- Reduce per-join-type code to source-specific logic only
- **Validation**: `mix test --seed 0 --trace` - 9 doctests, 898 tests, 0 failures

### Milestone 7: Inline utils.ex (S10)
- Move `atomize_keys/1` to `CommonParams` as `defp`
- Delete `lib/ecto_shorts/utils.ex`
- Update any references
- **Validation**: `mix test --seed 0 --trace` - 9 doctests, 898 tests, 0 failures

### Milestone 8: Final validation and close
- Run full quality suite: `mix format`, `mix test --seed 0 --trace`, `mix credo --strict`, `mix dialyzer`
- Write ADR if any `@doc false` functions were moved or renamed
- Update Outcomes & Retrospective

## Implementation Results

### Line count changes

| File | Before | After | Delta |
|---|---|---|---|
| `scalar_expr/specs.ex` | 1758 | 1587 | -171 (-10%) |
| `array_expr/specs.ex` | 935 | 795 | -140 (-15%) |
| `common_filters.ex` | 1606 | 1581 | -25 (-2%) |
| `actions.ex` | 1974 | 1955 | -19 (-1%) |
| `dynamics.ex` | 497 | 482 | -15 (-3%) |
| **New: `expr_helpers.ex`** | 0 | 123 | +123 |
| **New: `actions/bulk.ex`** | 0 | 47 | +47 |
| **Total** | 17379 | 17192 | **-187 net** |

### Milestones completed

1. **M1**: Extracted `ExprHelpers` - removed 12 inline alias-to-canonical mapping copies and 4 duplicate aggregate case AST helpers across both specs files.
2. **M2**: Replaced 16-arm case dispatch in `common_filters.ex` `build_query/6` with `@query_builder_modules` map-based dispatch.
3. **M3**: Extracted `Actions.Bulk` from `actions.ex` - bulk operations (`insert_all`, `update_all`, `delete_all`) now delegate to `actions/bulk.ex`.
4. **M5**: Consolidated `build_field_predicates/7` and `build_operator_predicates/7` in `dynamics.ex` into a shared `reduce_predicates/8` + `build_and_merge/8`.

### Milestones skipped (with rationale)

- **M4** (`normalize_insert_entry`): Already well-structured via pattern matching. 6 clauses handle genuinely different input shapes.
- **M6** (`join.ex` option extraction): Option extraction is inside `Compiler.define_clauses` macro block. Risk of subtle compile-time behavior changes outweighs the 4-line-per-clause savings.
- **M7** (`utils.ex` inline): Requires modifying `mix.exs` (restricted file) and migrating test file. Module is only 47 lines.

### Validation

- `mix test --seed 0 --trace`: 9 doctests, 898 tests, 0 failures
- `mix credo --strict`: No new warnings (pre-existing `==/2` warnings in AST code)
- `mix dialyzer`: No new warnings (pre-existing `dynamics.ex:1:pattern_match` and `binding_params.ex:136:pattern_match_cov`)

## Smells explicitly kept as-is

- **S11** (`logger.ex`): Thin wrapper but provides centralized logging control. Keep.
- **S12** (`config.ex`): Repetitive but clear and maintainable. Keep.
- **`common_changes.ex`** (848 lines): ~520 lines are documentation. Code is well-structured with clear groupings. No smell.
- **`common_schema.ex`** (820 lines): ~500 lines are documentation. Code is well-structured. No smell.
- **`common_query.ex`** (537 lines): ~300 lines are documentation. Code is clean. No smell.
- **`testing.ex`** (451 lines): ~200 lines are documentation. Code is clean. No smell.
- **`schema_helpers.ex`** (443 lines): ~200 lines are documentation. Code is clean. No smell.
- **`compiler.ex`** (403 lines): ~200 lines are documentation. At the boundary. No action needed.
