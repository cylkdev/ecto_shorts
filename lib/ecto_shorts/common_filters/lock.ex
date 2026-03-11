defmodule EctoShorts.CommonFilters.Lock do
  alias Ecto.Query
  alias EctoShorts.Compiler
  alias EctoShorts.Logger
  alias EctoShorts.QueryProvider

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.Lock"

  {_, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(:lock, _source, query, selected_binding, params, opts) do
    {lock_name, lock_values} = normalize_lock_params(params)

    apply_lock(query, selected_binding, lock_name, lock_values, opts)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_lock(query, unquote(quoted_binding_head), :for_update, _lock_values, _opts) do
      Query.lock(query, [unquote_splicing(quoted_binding_body)], "FOR UPDATE")
    end

    defp apply_lock(query, unquote(quoted_binding_head), :for_share, _lock_values, _opts) do
      Query.lock(query, [unquote_splicing(quoted_binding_body)], "FOR SHARE")
    end
  end

  defp apply_lock(query, _selected_binding, :for_update, _lock_values, _opts),
    do: Query.lock(query, "FOR UPDATE")

  defp apply_lock(query, _selected_binding, :for_share, _lock_values, _opts),
    do: Query.lock(query, "FOR SHARE")

  defp apply_lock(query, selected_binding, lock_name, lock_values, opts) do
    case resolve_lock_expr(selected_binding, lock_name, lock_values, opts) do
      {:ok, query_builder_fun} ->
        query_builder_fun.(query)

      :error ->
        query
    end
  end

  defp normalize_lock_params(params) when is_map(params) and not is_struct(params) do
    {Map.get(params, :name), Map.get(params, :values, %{})}
  end

  defp normalize_lock_params(params) when is_list(params) do
    if Keyword.keyword?(params) and Keyword.has_key?(params, :name) do
      {params[:name], Keyword.get(params, :values, %{})}
    else
      {params, %{}}
    end
  end

  defp normalize_lock_params(params), do: {params, %{}}

  defp resolve_lock_expr(selected_binding, lock_key, lock_params, opts) do
    case QueryProvider.build_fragment_expression(selected_binding, lock_key, lock_params, opts) do
      nil ->
        :error

      {:ok, query_builder_fun} when is_function(query_builder_fun, 1) ->
        {:ok, query_builder_fun}

      {:error, reason} ->
        Logger.warning(
          @logger_prefix,
          "Lock expression callback returned error for key #{inspect(lock_key)}: #{inspect(reason)}"
        )

        :error

      other ->
        Logger.warning(
          @logger_prefix,
          "Expected lock expression callback to return {:ok, query_builder_fun} | {:error, reason} | nil, got: #{inspect(other)}"
        )

        :error
    end
  end
end
