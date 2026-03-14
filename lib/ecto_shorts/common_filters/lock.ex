defmodule EctoShorts.CommonFilters.Lock do
  alias EctoShorts.Compiler
  alias EctoShorts.Config
  alias EctoShorts.Logger
  alias EctoShorts.QueryProvider

  alias Ecto.Query
  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.Lock"

  {_, binding_patterns} = Compiler.query_binding_contracts(10, __MODULE__)

  @spec build_query(
          :lock,
          term(),
          Ecto.Query.t(),
          {:as, atom()} | {:at, pos_integer()},
          term(),
          keyword()
        ) :: Ecto.Query.t()
  def build_query(:lock, _source, query, selected_binding, params, _opts) when is_binary(params) do
    build_raw_lock(query, selected_binding, params)
  end

  def build_query(:lock, _source, query, _selected_binding, params, _opts)
      when is_function(params, 1) do
    case params.(query) do
      next_query when is_struct(next_query, Ecto.Query) ->
        next_query

      other ->
        Logger.warning(
          @logger_prefix,
          "Expected lock callback to return an Ecto.Query, got: #{inspect(other)}"
        )

        query
    end
  end

  def build_query(:lock, _source, query, selected_binding, params, opts) do
    if (is_map(params) and not is_struct(params)) or Keyword.keyword?(params) do
      case params[:name] do
        nil -> query
        name -> build_lock(query, selected_binding, name, params, opts)
      end
    else
      Logger.warning(@logger_prefix, "Expected lock ..., got: #{inspect(params)}")
      query
    end
  end

  defp build_raw_lock(query, _selected_binding, expr) do
    Ecto.Query.Builder.Lock.apply(query, expr)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp build_lock(query, unquote(quoted_binding_head), :for_update, _values, _opts) do
      Query.lock(query, [unquote_splicing(quoted_binding_body)], "FOR UPDATE")
    end

    defp build_lock(query, unquote(quoted_binding_head), :for_share, _values, _opts) do
      Query.lock(query, [unquote_splicing(quoted_binding_body)], "FOR SHARE")
    end
  end

  defp query_provider(params, opts) do
    params[:query_provider] || opts[:query_provider] || Config.query_provider()
  end

  defp build_lock(query, selected_binding, custom_name, params, opts) do
    values = params[:values] || %{}

    case params
         |> query_provider(opts)
         |> QueryProvider.resolve_query_expression(selected_binding, custom_name, values, opts) do
      nil ->
        query

      {:ok, callback} ->
        if is_function(callback, 1) do
          case callback.(query) do
            next_query when is_struct(next_query, Ecto.Query) ->
              next_query

            other ->
              Logger.warning(
                @logger_prefix,
                "Expected ..., got: #{inspect(other)}"
              )

              query
          end
        else
          Logger.warning(
            @logger_prefix,
            "Expected ..., got: #{inspect(callback)}"
          )

          query
        end

      {:error, reason} ->
        Logger.warning(
          @logger_prefix,
          "Lock expression callback returned error for #{inspect(custom_name)}: #{inspect(reason)}"
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
