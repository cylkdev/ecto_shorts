defmodule EctoShorts.CommonFilters.Update do
  alias EctoShorts.Compiler
  alias EctoShorts.Utils

  alias Ecto.Query
  require Ecto.Query

  {_, binding_patterns} =
    Compiler.query_binding_contracts(10, __MODULE__)

  def build_query(:update, _source, query, selected_binding, term, _opts) do
    apply_update_expr(query, selected_binding, Utils.normalize_params(term))
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_update_expr(query, unquote(quoted_binding_head), expr) do
      Query.update(query, [unquote_splicing(quoted_binding_body)], ^expr)
    end
  end

  defp apply_update_expr(query, _selected_binding, expr) do
    Query.update(query, ^expr)
  end
end
