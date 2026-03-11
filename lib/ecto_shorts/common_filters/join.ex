defmodule EctoShorts.CommonFilters.Join do


  alias EctoShorts.Adapters.Postgres
  alias EctoShorts.CommonFilters
  alias EctoShorts.CommonSchema
  alias EctoShorts.Compiler
  alias EctoShorts.Logger
  alias EctoShorts.QueryProvider

  alias Ecto.Query
  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.Join"

  @hints (case Application.compile_env(:ecto_shorts, :hints) do
            nil ->
              []

            hints when is_list(hints) ->
              hints

            mod when is_atom(mod) ->
              mod.hints()

            term ->
              raise ArgumentError, "Expected :hints to be a list or module, got: #{inspect(term)}"
          end)

  @join_types [:association, :schema, :table, :query, :subquery, :fragment]

  {target_binding_var, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(:join, schema_source, query, selected_binding, params, opts)
      when is_map(params) and not is_struct(params) do
    build_query(:join, schema_source, query, selected_binding, Map.to_list(params), opts)
  end

  def build_query(:join, schema_source, query, selected_binding, params, opts) when is_list(params) do
    Enum.reduce(params, query, fn
      {join_type, join_options}, query_acc when join_type in @join_types ->
        reduce_join(schema_source, query_acc, selected_binding, {join_type, join_options}, opts)

      {key, join_options}, query_acc ->
        associations = CommonSchema.get_schema_reflection(schema_source, :associations) || []

        if key in associations do
          join_options =
            cond do
              is_map(join_options) and not is_struct(join_options) -> Map.to_list(join_options)
              is_list(join_options) -> join_options
              true -> []
            end

          reduce_join(
            schema_source,
            query_acc,
            selected_binding,
            {:association, Keyword.put(join_options, :source, key)},
            opts
          )
        else
          Logger.warning(
            @logger_prefix,
            "Expected join type to be one of #{inspect(@join_types)}, got: #{inspect(key)}"
          )

          query_acc
        end

      nested, query_acc when is_map(nested) and not is_struct(nested) ->
        build_query(:join, schema_source, query_acc, selected_binding, nested, opts)

      nested, query_acc when is_list(nested) ->
        if Keyword.keyword?(nested) do
          build_query(:join, schema_source, query_acc, selected_binding, nested, opts)
        else
          Enum.reduce(nested, query_acc, fn entry, inner_acc ->
            build_query(:join, schema_source, inner_acc, selected_binding, entry, opts)
          end)
        end

      other, query_acc ->
        Logger.warning(
          @logger_prefix,
          "Expected :join params to be a map or keyword list, got: #{inspect(other)}"
        )

        query_acc
    end)
  end

  def build_query(:join, _schema_source, query, _selected_binding, _params, _opts) do
    query
  end

  defp reduce_join(schema_source, query, selected_binding, {join_type, join_options}, opts)
       when is_map(join_options) and not is_struct(join_options) do
    reduce_join(schema_source, query, selected_binding, {join_type, Map.to_list(join_options)}, opts)
  end

  defp reduce_join(schema_source, query, selected_binding, {join_type, join_options}, opts)
       when is_list(join_options) do
    {op_source, join_options} = Keyword.pop(join_options, :source)

    if op_source !== nil do
      apply_join_expr(
        schema_source,
        query,
        selected_binding,
        {join_type, op_source, join_options},
        opts
      )
    else
      Logger.warning(
        @logger_prefix,
        "Expected join options to have a :source key, got: #{inspect(join_options)}"
      )

      query
    end
  end

  defp reduce_join(_schema_source, query, _selected_binding, {_join_type, join_options}, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected join options to be a map or keyword list, got: #{inspect(join_options)}"
    )

    query
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp apply_join_expr(
           schema_source,
           query,
           unquote(quoted_binding_head) = selected_binding,
           {:association, assoc_key, join_options},
           opts
         ) do
      case on_expr(schema_source, selected_binding, join_options[:on], opts) do
        {:ok, on_value} ->
          qualifier = join_options[:qualifier] || :inner
          prefix = join_options[:prefix]
          as = join_options[:as]
          hints = join_options[:hints]

          build_join(
            query,
            selected_binding,
            qualifier,
            {:association, assoc_key},
            as,
            on_value,
            prefix,
            hints
          )

        :error ->
          query
      end
    end

    defp apply_join_expr(
           schema_source,
           query,
           unquote(quoted_binding_head) = selected_binding,
           {:schema, target_schema, join_options},
           opts
         ) do
      case on_expr(schema_source, selected_binding, join_options[:on], opts) do
        {:ok, on_value} ->
          qualifier = join_options[:qualifier] || :inner
          prefix = join_options[:prefix]
          as = join_options[:as]
          hints = join_options[:hints]

          source =
            case target_schema do
              {table, schema}
              when is_binary(table) and table !== "" and is_atom(schema) and not is_nil(schema) ->
                {table, schema}

              schema when is_atom(schema) and not is_nil(schema) ->
                schema

              _ ->
                raise ArgumentError,
                      "Expected target schema to be an atom or a tuple of {table, schema}, got: #{inspect(target_schema)}"
            end

          build_join(
            query,
            selected_binding,
            qualifier,
            {:source, source},
            as,
            on_value,
            prefix,
            hints
          )

        :error ->
          query
      end
    end

    defp apply_join_expr(
           schema_source,
           query,
           unquote(quoted_binding_head) = selected_binding,
           {:table, table_name, join_options},
           opts
         ) do
      case on_expr(schema_source, selected_binding, join_options[:on], opts) do
        {:ok, on_value} ->
          qualifier = join_options[:qualifier] || :inner
          prefix = join_options[:prefix]
          as = join_options[:as]
          hints = join_options[:hints]

          build_join(
            query,
            selected_binding,
            qualifier,
            {:source, table_name},
            as,
            on_value,
            prefix,
            hints
          )

        :error ->
          query
      end
    end

    defp apply_join_expr(
           schema_source,
           query,
           unquote(quoted_binding_head) = selected_binding,
           {:query, source_query, join_options},
           opts
         ) do
      case on_expr(schema_source, selected_binding, join_options[:on], opts) do
        {:ok, on_value} ->
          qualifier = join_options[:qualifier] || :inner
          prefix = join_options[:prefix]
          as = join_options[:as]
          hints = join_options[:hints]

          unless is_struct(source_query, Ecto.Query) do
            raise ArgumentError, "Expected source query to be a struct, got: #{inspect(source_query)}"
          end

          build_join(
            query,
            selected_binding,
            qualifier,
            {:source, source_query},
            as,
            on_value,
            prefix,
            hints
          )

        :error ->
          query
      end
    end

    defp apply_join_expr(
           schema_source,
           query,
           unquote(quoted_binding_head) = selected_binding,
           {:subquery, params, join_options},
           opts
         ) do
      case on_expr(schema_source, selected_binding, join_options[:on], opts) do
        {:ok, on_value} ->
          qualifier = join_options[:qualifier] || :inner
          prefix = join_options[:prefix]
          as = join_options[:as]
          hints = join_options[:hints]

          subquery_source =
            if is_struct(params, Ecto.Query) or is_struct(params, Ecto.SubQuery) do
              params
            else
              {from_source, filter_params} = Keyword.pop(params, :from, schema_source)
              CommonFilters.convert_params_to_filter(from_source, filter_params, opts)
            end

          build_join(
            query,
            selected_binding,
            qualifier,
            {:subquery, subquery_source},
            as,
            on_value,
            prefix,
            hints
          )

        :error ->
          query
      end
    end

    defp apply_join_expr(
           schema_source,
           query,
           unquote(quoted_binding_head) = selected_binding,
           {:fragment, params, join_options},
           opts
         ) do
      case on_expr(schema_source, selected_binding, join_options[:on], opts) do
        {:ok, on_value} ->
          qualifier = join_options[:qualifier] || :inner
          prefix = join_options[:prefix]
          as = join_options[:as]
          hints = join_options[:hints]

          source_name = params[:name]
          source_values = params[:values]

          if is_nil(source_name) do
            raise ArgumentError, "Join source name is required, got: #{inspect(params)}"
          end

          if is_nil(source_values) do
            raise ArgumentError, "Join source values are required, got: #{inspect(params)}"
          end

          case resolve_expr_source(selected_binding, source_name, source_values, opts) do
            {:ok, source} ->
              build_join(
                query,
                selected_binding,
                qualifier,
                {:source, source},
                as,
                on_value,
                prefix,
                hints
              )

            :error ->
              query
          end

        :error ->
          query
      end
    end

    for {hint_key, hint_value} <- @hints do
      defp build_join(
             query,
             unquote(quoted_binding_head) = _selected_binding,
             qualifier,
             {:association, assoc_key},
             as,
             on,
             prefix,
             unquote(hint_key)
           ) do
        Query.join(
          query,
          qualifier,
          [unquote_splicing(quoted_binding_body)],
          joined in assoc(unquote(target_binding_var), ^assoc_key),
          as: ^as,
          on: ^on,
          prefix: ^prefix,
          hints: unquote(hint_value)
        )
      end

      defp build_join(
             query,
             unquote(quoted_binding_head) = _selected_binding,
             qualifier,
             {:source, source},
             as,
             on,
             prefix,
             unquote(hint_key)
           ) do
        Query.join(
          query,
          qualifier,
          [unquote_splicing(quoted_binding_body)],
          joined in ^source,
          as: ^as,
          on: ^on,
          prefix: ^prefix,
          hints: unquote(hint_value)
        )
      end

      defp build_join(
             query,
             unquote(quoted_binding_head) = _selected_binding,
             qualifier,
             {:subquery, subquery_source},
             as,
             on,
             prefix,
             unquote(hint_key)
           ) do
        Query.join(
          query,
          qualifier,
          [unquote_splicing(quoted_binding_body)],
          joined in subquery(subquery_source),
          as: ^as,
          on: ^on,
          prefix: ^prefix,
          hints: unquote(hint_value)
        )
      end
    end

    defp build_join(
           query,
           unquote(quoted_binding_head) = _selected_binding,
           qualifier,
           {:association, assoc_key},
           as,
           on,
           prefix,
           _hints
         ) do
      Query.join(
        query,
        qualifier,
        [unquote_splicing(quoted_binding_body)],
        joined in assoc(unquote(target_binding_var), ^assoc_key),
        as: ^as,
        on: ^on,
        prefix: ^prefix
      )
    end

    defp build_join(
           query,
           unquote(quoted_binding_head) = _selected_binding,
           qualifier,
           {:source, source},
           as,
           on,
           prefix,
           _hints
         ) do
      Query.join(
        query,
        qualifier,
        [unquote_splicing(quoted_binding_body)],
        joined in ^source,
        as: ^as,
        on: ^on,
        prefix: ^prefix
      )
    end

    defp build_join(
           query,
           unquote(quoted_binding_head) = _selected_binding,
           qualifier,
           {:subquery, subquery_source},
           as,
           on,
           prefix,
           _hints
         ) do
      Query.join(
        query,
        qualifier,
        [unquote_splicing(quoted_binding_body)],
        joined in subquery(subquery_source),
        as: ^as,
        on: ^on,
        prefix: ^prefix
      )
    end
  end

  defp resolve_expr_source(selected_binding, source_key, source_params, opts) do
    case QueryProvider.resolve_query_expression(selected_binding, source_key, source_params, opts) do
      nil ->
        :error

      {:ok, source} ->
        {:ok, source}

      {:error, reason} ->
        Logger.warning(
          @logger_prefix,
          "Join source callback returned error for key #{inspect(source_key)}: #{inspect(reason)}"
        )

        :error

      other ->
        Logger.warning(
          @logger_prefix,
          "Expected join source callback to return {:ok, source} | {:error, reason} | nil, got: #{inspect(other)}"
        )

        :error
    end
  end

  defp on_expr(schema_source, selected_binding, on_param, opts) do
    case on_param do
      true ->
        {:ok, true}

      nil ->
        {:ok, true}

      list when is_list(list) ->
        if Keyword.keyword?(list) do
          {:ok, build_on_dynamic(schema_source, selected_binding, list, opts)}
        else
          Logger.warning(
            @logger_prefix,
            "Expected :on to be a keyword list, map, or true, got: #{inspect(list)}"
          )

          :error
        end

      on_params when is_map(on_params) and not is_struct(on_params) ->
        {:ok, build_on_dynamic(schema_source, selected_binding, Map.to_list(on_params), opts)}

      %Ecto.Query.DynamicExpr{} = dyn ->
        {:ok, dyn}

      term ->
        Logger.warning(
          @logger_prefix,
          "Expected :on to be a keyword list, map, or true, got: #{inspect(term)}"
        )

        :error
    end
  end

  defp build_on_dynamic(schema_source, selected_binding, entries, opts) when is_list(entries) do
    Enum.reduce(entries, nil, fn {key, value}, acc ->
      dyn = Postgres.build_dynamic(schema_source, selected_binding, {key, value}, opts)
      merge_dynamic(acc, dyn)
    end)
  end

  defp merge_dynamic(nil, dyn), do: dyn
  defp merge_dynamic(dyn, nil), do: dyn
  defp merge_dynamic(left, right), do: Query.dynamic(^left and ^right)
end
