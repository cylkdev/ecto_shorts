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
    case params[:name] do
      nil -> query
      name -> build_lock(query, selected_binding, name, params[:values] || %{}, opts)
    end
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp build_lock(query, unquote(quoted_binding_head), :for_update, _values, _opts) do
      Query.lock(query, [unquote_splicing(quoted_binding_body)], "FOR UPDATE")
    end

    defp build_lock(query, unquote(quoted_binding_head), :for_share, _values, _opts) do
      Query.lock(query, [unquote_splicing(quoted_binding_body)], "FOR SHARE")
    end
  end

  defp build_lock(query, selected_binding, name, values, opts) do
    case QueryProvider.resolve_query_expression(selected_binding, name, values, opts) do
      nil ->
        query

      {:ok, callback} when is_function(callback, 1) ->
        callback.(query)

      {:error, reason} ->
        Logger.warning(
          @logger_prefix,
          "Lock expression callback returned error for #{inspect(name)}: #{inspect(reason)}"
        )

        query

      other ->
        Logger.warning(
          @logger_prefix,
          "Expected lock expression callback to return {:ok, function} | {:error, reason} | nil, got: #{inspect(other)}"
        )

        query
    end
  end
end
