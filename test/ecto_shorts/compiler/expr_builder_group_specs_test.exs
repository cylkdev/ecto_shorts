defmodule EctoShorts.Dynamics.Adapters.Postgres.ExprGroupSpecsTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Compiler.ClauseBuilder
  alias EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr.Specs, as: ArrayExprSpecs
  alias EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs, as: ScalarExprSpecs

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

  test "scalar nil_specs/4 builds clauses without needing other groups" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ScalarExprSpecs.nil_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )

    module = compile_specs_module!(specs)

    key = :archived_at

    expected_is_nil = dynamic([q], is_nil(field(q, ^key)))
    expected_not_nil = dynamic([q], not is_nil(field(q, ^key)))

    assert_dynamic(
      expected_is_nil,
      module.apply_dynamic_expr({:as, nil}, key, {:eq, nil})
    )

    assert_dynamic(
      expected_not_nil,
      module.apply_dynamic_expr({:as, nil}, key, {:!=, nil})
    )
  end

  test "scalar alias_op_specs/2 composes with base_op_specs/4" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ScalarExprSpecs.alias_op_specs(__MODULE__, binding_head_ast) ++
        ScalarExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :age
    expected = dynamic([q], field(q, ^key) > ^1)

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:gt, 1})
    )
  end

  test "array lower_upper_specs/4 builds the unnest fragments" do
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

  test "array alias_op_specs/2 composes with base_op_specs/4" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ArrayExprSpecs.alias_op_specs(__MODULE__, binding_head_ast) ++
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
end
