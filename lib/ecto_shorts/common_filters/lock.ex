defmodule EctoShorts.CommonFilters.Lock do
  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query

  {_, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(:lock, _source, query, selected_binding, expr, _opts) do
    apply_lock(query, selected_binding, normalize_lock_expr(expr))
  end

  defp normalize_lock_expr(expr) when is_map(expr) and not is_struct(expr) do
    expr
    |> Map.to_list()
    |> normalize_lock_expr()
  end

  defp normalize_lock_expr(expr) when is_list(expr) do
    if Keyword.keyword?(expr) do
      case Keyword.get(expr, :name) do
        :for_update -> :for_update
        :for_share -> :for_share
        _ -> expr
      end
    else
      expr
    end
  end

  defp normalize_lock_expr(expr), do: expr

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_lock(query, unquote(quoted_binding_head), :for_update) do
      Query.lock(query, [unquote_splicing(quoted_binding_body)], "FOR UPDATE")
    end

    defp apply_lock(query, unquote(quoted_binding_head), :for_share) do
      Query.lock(query, [unquote_splicing(quoted_binding_body)], "FOR SHARE")
    end
  end

  defp apply_lock(query, _selected_binding, :for_update), do: Query.lock(query, "FOR UPDATE")
  defp apply_lock(query, _selected_binding, :for_share), do: Query.lock(query, "FOR SHARE")
end
