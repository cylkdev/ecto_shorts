# ExecPlan: Polish QueryBinding, Normalizer, Logger, Docs

## Purpose
Five cleanup tasks:
1. Add `@moduledoc` / `@doc` to `EctoShorts.QueryBinding`
2. Extract normalization logic from `DynamicExpressions.Adapters.Postgres` into a
   `DynamicExpressions.Adapters.Postgres.Normalizer` sub-module with its own tests
3. Delete stale empty test directories (`compiler/`, `generator/`, `dynamics/postgres/`)
4. Verify `EctoShorts.Actions` docs are accurate and current
5. Replace all vague `"Expected ..., got:"` Logger messages with specific descriptions

## Non-goals
- No behaviour or API changes
- No changes to test coverage for existing code paths

## Progress

- [ ] 1. QueryBinding docs
- [ ] 2. Normalizer sub-module + tests
- [ ] 3. Delete stale empty dirs
- [ ] 4. Actions doc audit
- [ ] 5. Logger message cleanup

## Steps

### 1. QueryBinding docs
- Replace `@moduledoc false` with a real moduledoc explaining the module's
  responsibility (compile-time query binding contracts and AST helpers).
- Add `@doc` to every public function.

### 2. Normalizer sub-module
- Create `lib/ecto_shorts/dynamics/adapters/postgres/normalizer.ex`
  containing all private `normalize_*` functions from `postgres.ex`, made
  public under the `Normalizer` module.
- Remove the private functions from `postgres.ex`; call `Normalizer.*`
  instead.
- Create `test/ecto_shorts/dynamics/adapters/postgres/normalizer_test.exs`
  testing the normalizer functions directly.

### 3. Delete empty dirs
- `test/ecto_shorts/compiler/`
- `test/ecto_shorts/generator/`
- `test/ecto_shorts/dynamics/postgres/`

### 4. Actions doc audit
- `:dynamic_adapter` shared option says `EctoShorts.Dynamic` - should
  be `EctoShorts.Adapter.DynamicExpression`.
- `insert_all/3` return type table says `{:ok, {count, nil, [struct]}}` - 
  trailing nil column is wrong formatting.
- Everything else is accurate.

### 5. Logger cleanup
Fix every `"Expected ..., got:"` message with a specific description of
what the valid input shapes are. Files: `last.ex`, `lock.ex`, `sub_query.ex`,
`select.ex`, `order_by.ex`.

## Verification
- `mix test` - 11 doctests, 721+ tests, 0 failures
