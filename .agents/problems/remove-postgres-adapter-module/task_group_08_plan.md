# Task Group 08 Plan — Predictable `<Caller>.Compiled` module pattern

## Goal (one sentence)
Update `EctoShorts.QueryBuilder.Dynamics.Compiler` so `use ...Compiler` in a module `X` defines (1) a nested `X.Compiled` module containing generated `dynamic_field_expr/3` clauses and (2) the public `X.dynamic_field_expr/3` entrypoint that delegates into `X.Compiled`.

## Constraints / non-goals
- Keep ADR-0007’s “no `@after_compile`, no `Module.create/3`” rule.
- Do not reintroduce `kind` or emitter indirection.
- Do not change query semantics.
- Follow “one module per file” (no defining `X.Specs` in the same file as `X`).
- Cancel the “router function argument” idea. No function callbacks in `convert_to_dynamic`.
- Use “Solution 1” for adapter separation: `Dynamics` is the engine; the adapter module owns operator keys and routing into expression modules.

## Key technical constraint (important)
`__using__/1` runs during macro expansion. A runtime `def clause_specs(...)` **defined in the same module `X`** cannot reliably be *executed* during `use` expansion because `X` is not compiled/loaded yet.

So to generate clauses at compile time (without callbacks), clause specs must come from a **separate specs module** that is compiled before `X` expands `use ...Compiler`.

## Proposed end state

### `EctoShorts.QueryBuilder.Dynamics.Compiler`
- `__using__/1`:
  - determines `caller_module = __CALLER__.module` (this is `X`)
  - determines `compiled_module = Module.concat(caller_module, Compiled)` (this is `X.Compiled`)
  - resolves a required `:specs` option to a compiled module (convention: `X.Specs`)
  - computes `clause_asts` from `specs_module.clause_specs/4` + binding patterns
  - returns a quoted block that defines, in order:
    1) `defmodule unquote(compiled_module) do ... end` containing imports + `unquote_splicing(clause_asts)`
    2) `@doc false` + `def dynamic_field_expr/3` in the caller module that delegates to `compiled_module.dynamic_field_expr/3`

No `@compile` directives are needed in end-user modules; the macro expansion defines `X.Compiled` and the delegator explicitly.

### Adapter separation (Solution 1)
We keep adapter separation by ensuring:

- Adapter-specific rules live in the adapter module (`EctoShorts.QueryBuilder.Dynamics.Postgres`).
- `EctoShorts.QueryBuilder.Dynamics` is responsible only for the generic reduction/merge algorithm and boolean group handling.
- `EctoShorts.QueryBuilder.Dynamics` does **not** own a “common operator list” and does **not** do Scalar-vs-Array routing.

#### Adapter API (explicit, minimal; no behaviours)
`EctoShorts.QueryBuilder.Dynamics.Postgres` exposes **exactly two** functions:

- `operators/0` - returns the list of adapter-level operator keys
- `build_dynamic_expression/4` - routes to `CommonExpr` / `ScalarExpr` / `ArrayExpr` and returns the built `dynamic()` expression

```elixir
defmodule EctoShorts.QueryBuilder.Dynamics.Postgres do
  @moduledoc false

  def operators do
    [:ids, :before, :after, :start_date, :end_date]
  end

  def build_dynamic_expression(source, binding_selector, key, expr) do
    # This function does the routing in its own pass.
    #
    # It receives enough context (`source` + `key` + `expr`) to decide:
    # - CommonExpr (adapter operator keys, ex `key in operators()`)
    # - ArrayExpr vs ScalarExpr (schema type reflection when applicable)
    #
    # Returns a `dynamic()` expression.
  end
end
```

#### Dynamics flow (engine stays dumb)
```elixir
defmodule EctoShorts.QueryBuilder.Dynamics do
  @moduledoc false

  alias EctoShorts.QueryBuilder.Dynamics.Postgres

  def convert_to_dynamic(source, binding_selector, args) do
    # normalize + reduce args
    #
    # when encountering {key, value}:
    #   - if `key in Postgres.operators()`, delegate operator handling to:
    #       Postgres.build_dynamic_expression(source, binding_selector, key, item)
    #     (no schema query_fields validation)
    #
    #   - else, keep the existing schema query_fields validation + warning behavior,
    #     and delegate predicate building to:
    #       Postgres.build_dynamic_expression(source, binding_selector, key, expr)
    #     where `expr` is the same shape we already pass today
    #     (`value` or `{op, value}` after normalization).
  end
end
```

### Call site shape
For an expression module like `EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr`:

#### Specs module (separate file; one module per file)
```elixir
defmodule EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr.Specs do
  @moduledoc false

  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    # returns list of clause specs (maps/keywords/ClauseSpec)
  end
end
```

#### Expression module (end user only writes `use ...Compiler`)
```elixir
defmodule EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr do
  @moduledoc false

  use EctoShorts.QueryBuilder.Dynamics.Compiler,
    specs: EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr.Specs,
    max_positional_bindings: 10
end
```

#### Generated module + function (macro expansion result; end user does not write this)
```elixir
defmodule EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr.Compiled do
  @moduledoc false

  import Ecto.Query
  require Ecto.Query

  # many def dynamic_field_expr/3 clauses ...
end

@doc false
def dynamic_field_expr(binding_selector, key, expr) do
  EctoShorts.QueryBuilder.Dynamics.Postgres.ArrayExpr.Compiled.dynamic_field_expr(binding_selector, key, expr)
end
```

## Flow (start → end)
### Compile time
1) `X` expands `use EctoShorts.QueryBuilder.Dynamics.Compiler, specs: X.Specs`.
2) The compiler macro ensures `X.Specs` is compiled and exports `clause_specs/4`.
3) The compiler macro computes binding patterns via `BindingHelpers.query_var_and_binding_heads/2`.
4) For each binding pattern, the macro calls `X.Specs.clause_specs/4` to get clause specs, converts them to clause ASTs, and emits them into `X.Compiled`.
5) The macro emits `X.dynamic_field_expr/3` delegating to `X.Compiled.dynamic_field_expr/3`.

### Runtime
1) Callers invoke `X.dynamic_field_expr(binding_selector, key, expr)`.
2) The generated delegator forwards to `X.Compiled.dynamic_field_expr/3`, where the pattern-matching clauses select the correct Ecto dynamic expression.

## Files/modules to touch (bounded)
- `lib/ecto_shorts/query_builder/dynamics/compiler.ex`
- `lib/ecto_shorts/query_builder/dynamics.ex` (remove `@common_operators` + scalar/array routing; use `Postgres.operators/0` + `Postgres.build_dynamic_expression/4`)
- `lib/ecto_shorts/query_builder/dynamics/postgres.ex` (replace `Compiled.*` ownership with `operators/0` + `build_dynamic_expression/4`)
- `lib/ecto_shorts/query_builder/dynamics/postgres/{array_expr,scalar_expr,common_expr}.ex` (adopt the new `X.Compiled` pattern; no manual delegators; no `@compile` directives)
- `lib/ecto_shorts/query_builder/dynamics/postgres/specs/*.ex` to migrate specs into `EctoShorts.QueryBuilder.Dynamics.Postgres.{CommonExpr,ScalarExpr,ArrayExpr}.Specs` modules (one module per file).
- Tests that currently assume `Postgres.Compiled.*` modules exist.
- `.agents/adr/0008-predictable-compiled-module-naming.md`

## Acceptance criteria
- Using the compiler in `X` results in `X.Compiled` being defined.
- `X.dynamic_field_expr/3` exists without requiring user-defined boilerplate.
- `EctoShorts.QueryBuilder.Dynamics` no longer hardcodes “common operator” keys; the adapter module owns them.
- `EctoShorts.QueryBuilder.Dynamics.Postgres` exports only `operators/0` and `build_dynamic_expression/4`.
- No `@after_compile`, `__after_compile__/2`, or `Module.create/3` anywhere in `lib/ecto_shorts/query_builder/dynamics/**`.
- Focused tests pass.

## Checklist
- [ ] ADR-0008 approved (predictable `<Caller>.Compiled` naming; clause specs live in a separate `X.Specs` module).
- [ ] Update `lib/ecto_shorts/query_builder/dynamics/compiler.ex` `__using__/1` to:
  - define `defmodule <Caller>.Compiled` and emit clauses inside it
  - define `dynamic_field_expr/3` in the caller module, delegating to `<Caller>.Compiled.dynamic_field_expr/3`
- [ ] Update `lib/ecto_shorts/query_builder/dynamics.ex` to:
  - use `Postgres.operators/0` to detect adapter operator keys
  - delegate predicate building to `Postgres.build_dynamic_expression/4`
  - keep existing boolean-group reduction and query_fields warning behavior
- [ ] Update `lib/ecto_shorts/query_builder/dynamics/postgres.ex` to:
  - remove `Compiled.*` modules
  - export only `operators/0` and `build_dynamic_expression/4`
- [ ] Migrate Postgres clause specs into `...Postgres.{CommonExpr,ScalarExpr,ArrayExpr}.Specs` modules.
- [ ] Update Postgres expression modules to `use ...Compiler` (no manual `dynamic_field_expr/3`, no `@compile` directives).
- [ ] Update tests to assert the new compiled-module location and behavior.
- [ ] Run `mix format`, `mix compile`, and focused `mix test`.
