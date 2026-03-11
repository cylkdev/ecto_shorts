defmodule EctoShorts.CommonFilters.Update do
  alias EctoShorts.Compiler
  alias Ecto.Query

  require Ecto.Query

  # @supported_ops [:set, :inc]

  {_, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(:update, _source, query, selected_binding, term, _opts) do
    apply_update_expr(query, selected_binding, normalize_update_entries(term))
  end

  defp normalize_update_entries(entries)
       when (is_map(entries) and not is_struct(entries)) or is_list(entries) do
    Enum.map(entries, fn
      {op, value} when is_map(value) and not is_struct(value) ->
        {op, Map.to_list(value)}

      {op, value} ->
        {op, value}
    end)
  end

  defp normalize_update_entries(entries) do
    entries
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
