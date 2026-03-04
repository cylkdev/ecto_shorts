# Cover uncovered lines in scalar_expr/specs.ex

Add tests for 8 uncovered compile-time spec-generating functions in `lib/ecto_shorts/dynamics/adapters/postgres/scalar_expr/specs.ex` (52 missed lines, 24.6% coverage), following the existing pattern in `test/ecto_shorts/compiler/expr_builder_group_specs_test.exs`.

## Context

The coverage report (`mix coveralls.detail`) shows `scalar_expr/specs.ex` as the file with the most uncovered lines (52). The file contains compile-time functions that generate `ClauseSpec` structs used by the compiler to build pattern-match clauses in `Postgres.build_dynamic/4`.

Three functions are already covered by existing tests in `expr_builder_group_specs_test.exs`:

- `alias_op_specs/2`
- `nil_specs/4`
- `base_op_specs/4`

Eight functions have zero coverage:

| Function | Uncovered lines | Notes |
|---|---|---|
| `list_semantic_specs/2` | 4551-4555 | Delegates to `compose` |
| `aggregate_specs/4` | 4774-4790 | Some clauses delegate, some are direct |
| `all_specs/4` | 4907-4918 | Delegates and direct |
| `any_specs/4` | 5017-5028 | Delegates and direct |
| `arithmetic_specs/4` | 5127-5146 | Direct dynamic AST |
| `date_time_specs/4` | 5191-5206 | Delegates and direct |
| `lower_upper_specs/4` | 5344-5346 | Delegates and direct |
| `like_ilike_specs/4` | 5452-5456 | Direct dynamic AST |

## Testing pattern

The existing tests use this approach:

1. Call the spec function with `binding_setup/1` helpers to get `ClauseSpec` structs.
2. Compile the specs into a temporary module via `compile_specs_module!/1`.
3. Call `module.compose/3` with appropriate inputs.
4. Assert the returned dynamic matches an expected `Ecto.Query.dynamic`.

Functions that delegate via `compose` need `base_op_specs/4` included in the compiled module so the delegation target exists.

## Steps

Each step adds one test to `test/ecto_shorts/compiler/expr_builder_group_specs_test.exs`, runs the file, and confirms it passes.

1. **`list_semantic_specs/2`** - Test `{:==, [1, 2]}` coerces to `:in`. Needs `base_op_specs` for delegation.
2. **`aggregate_specs/4`** - Test `{:avg, {:>, 10}}`. Needs `base_op_specs` for alias delegation.
3. **`all_specs/4`** - Test `{:>, {:all, subquery}}`. Needs `base_op_specs` for delegation.
4. **`any_specs/4`** - Test `{:>, {:any, subquery}}`. Needs `base_op_specs` for delegation.
5. **`arithmetic_specs/4`** - Test `{:>, {:+, [:views, 10]}}`. Direct compilation.
6. **`date_time_specs/4`** - Test `{:>=, {:datetime, [{:add, payload}]}}`. Needs `base_op_specs` for delegation.
7. **`lower_upper_specs/4`** - Test `{:lower, "foo"}` and `{:==, {:lower, "foo"}}`. Needs `base_op_specs` for delegation.
8. **`like_ilike_specs/4`** - Test `{:like, "foo"}` and `{:like, ["foo", "bar"]}`. Direct compilation.

After all 8 steps, regenerate coverage and confirm the missed count dropped.
