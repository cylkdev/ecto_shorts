defmodule EctoShorts.CommonFilters.Lock do
  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query

  {_, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(:lock, _source, query, selected_binding, params, _opts) do
    lock_name =
      if Keyword.keyword?(params) and Keyword.has_key?(params, :name) do
        params[:name]
      else
        params
      end

    apply_lock(query, selected_binding, lock_name)
  end

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
