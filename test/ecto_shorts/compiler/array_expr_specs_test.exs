defmodule EctoShorts.Dynamics.Adapters.Postgres.ArrayExprSpecsTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Compiler.ClauseBuilder
  alias EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.Specs.Core, as: ArrayExprSpecs
  alias EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.Specs.Aggregate, as: AggregateSpecs

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defp compile_specs_module!(specs) do
    clause_asts = Enum.map(specs, &ClauseBuilder.clause_ast/1)

    module =
      Module.concat([__MODULE__, :"Tmp#{System.unique_integer([:positive])}"])

    quoted =
      quote do
        defmodule unquote(module) do
          import Ecto.Query
          require Ecto.Query

          unquote_splicing(clause_asts)
        end
      end

    Code.compile_quoted(quoted)
    module
  end

  defp binding_setup(context) do
    binding_head_ast = quote(do: {:as, nil})
    target_binding_var = Macro.var(:q, context)
    binding_body_asts = [quote(do: unquote(target_binding_var))]

    {binding_head_ast, target_binding_var, binding_body_asts}
  end

  test "lower_upper_specs/4 builds the unnest fragments" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ArrayExprSpecs.lower_upper_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )

    module = compile_specs_module!(specs)

    key = :tags

    expected =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE lower(t) = ?
          )
          """,
          field(q, ^key),
          ^"elixir"
        )
      )

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:lower, "elixir"})
    )
  end

  test "list_semantic_specs/2 coerces not == with list" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ArrayExprSpecs.list_semantic_specs(__MODULE__, binding_head_ast, target_binding_var, binding_body_asts) ++
        ArrayExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :tags

    expected = dynamic([q], field(q, ^key) != ^["a", "b"])

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:not, {:==, ["a", "b"]}})
    )
  end

  test "nil_specs/4 builds nil comparison clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ArrayExprSpecs.nil_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )

    module = compile_specs_module!(specs)

    key = :tags

    expected = dynamic([q], is_nil(field(q, ^key)))

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:==, nil})
    )
  end

  test "aggregate_specs/4 builds aggregate comparison clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      AggregateSpecs.aggregate_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) ++
        ArrayExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :tags

    expected = dynamic([q], count(field(q, ^key)) > ^1)

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:count, {:>, 1}})
    )
  end

  test "like_ilike_specs/4 builds like/ilike clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ArrayExprSpecs.like_ilike_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )

    module = compile_specs_module!(specs)

    key = :tags
    patterns = ["%foo%"]

    expected =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE t LIKE ANY (?)
          )
          """,
          field(q, ^key),
          ^patterns
        )
      )

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:like, "foo"})
    )
  end

  test "alias_op_specs/4 composes with base_op_specs/4" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ArrayExprSpecs.alias_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        ) ++
        ArrayExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :scores

    expected =
      dynamic(
        [q],
        fragment("? < ANY(?)", ^10, field(q, ^key))
      )

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:gt, 10})
    )
  end

  test "alias_op_specs/4 composes with base_op_specs/4 for :ne" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ArrayExprSpecs.alias_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        ) ++
        ArrayExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :scores
    expected = dynamic([q], ^10 not in field(q, ^key))

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:ne, 10})
    )
  end
end
