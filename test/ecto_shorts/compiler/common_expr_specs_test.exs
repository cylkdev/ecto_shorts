defmodule EctoShorts.Dynamics.Postgres.CommonExprBuildersTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Generator.Builder
  alias EctoShorts.Dynamics.Postgres.CommonExprBuilder

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defp compile_specs_module!(clause_asts) do
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

  test "named_clause_asts/2 builds operator clauses" do
    clause_asts =
      Builder.named_clause_asts(CommonExprBuilder,
        context: __MODULE__,
        operators: [:ids]
      )

    module = compile_specs_module!(clause_asts)

    expected_ids = dynamic([q], field(q, :id) in ^[1, 2])

    assert_dynamic(
      expected_ids,
      module.dynamic_expr({:as, nil}, :ids, nil, [1, 2])
    )
  end
end
