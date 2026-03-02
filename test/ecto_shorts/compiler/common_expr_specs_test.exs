defmodule EctoShorts.Dynamics.Adapters.Postgres.CommonExprSpecsTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Compiler.ClauseBuilder
  alias EctoShorts.Dynamics.Adapters.Postgres.CommonExpr.Specs, as: CommonExprSpecs

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

  test "clause_specs/4 builds operator clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      CommonExprSpecs.clause_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )

    module = compile_specs_module!(specs)

    expected_ids = dynamic([q], field(q, :id) in ^[1, 2])

    assert_dynamic(
      expected_ids,
      module.apply_dynamic_expr({:as, nil}, :ids, [1, 2])
    )
  end
end
