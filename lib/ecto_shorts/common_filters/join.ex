defmodule EctoShorts.CommonFilters.Join do
  @moduledoc false

  alias EctoShorts.Compiler
  alias EctoShorts.Config
  alias EctoShorts.Dynamics
  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonFilters

  alias Ecto.Query

  require Ecto.Query
  require EctoShorts.Compiler

  @logger_prefix "EctoShorts.CommonFilters.Join"

  @join_types [:association, :schema, :table, :query, :subquery, :fragment]
  @doc false

  def build(schema_source, :join, query, binding_selector, params, opts) when is_map(params) do
    build(schema_source, :join, query, binding_selector, Map.to_list(params), opts)
  end

  def build(schema_source, :join, query, binding_selector, list, opts) do
    Enum.reduce(list, query, fn
      {join_type, join_options}, q2 when join_type in @join_types ->
        reduce_join(schema_source, q2, binding_selector, {join_type, join_options}, opts)

      {key, join_options}, q2 ->
        assocs = CommonSchema.get_schema_reflection(schema_source, :associations) || []

        if key in assocs do
          reduce_join(
            schema_source,
            q2,
            binding_selector,
            {:association, Keyword.put(join_options, :source, key)},
            opts
          )
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected join type to be one of #{inspect(@join_types)}, got: #{inspect(key)}"
          )

          q2
        end

      nested, q2 when is_map(nested) ->
        build(schema_source, :join, q2, binding_selector, nested, opts)

      nested, q2 when is_list(nested) ->
        if Keyword.keyword?(nested) do
          build(schema_source, :join, q2, binding_selector, nested, opts)
        else
          Enum.reduce(nested, q2, &build(schema_source, :join, &2, binding_selector, &1, opts))
        end

      other, q2 ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected :join params to be a map or keyword list, got: #{inspect(other)}"
        )

        q2
    end)
  end

  defp reduce_join(schema_source, query, binding_selector, {join_type, join_options}, opts) do
    {op_source, join_options} = Keyword.pop(join_options, :source)

    if not is_nil(op_source) do
      build_join_expr(
        schema_source,
        query,
        binding_selector,
        {join_type, op_source, join_options},
        opts
      )
    else
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected join options to have a :source key, got: #{inspect(join_options)}"
      )

      query
    end
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp build_join_expr(
             schema_source,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:association, assoc_key, join_options},
             opts
           ) do
        on_value = on_expr(schema_source, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix]
        as = join_options[:as]

        joined_query =
          Query.join(
            query,
            qualifier,
            [unquote_splicing(quoted_binding_body)],
            joined in assoc(unquote(target_binding_var), ^assoc_key),
            as: ^as,
            on: ^on_value,
            prefix: ^prefix
          )

        apply_join_hints(joined_query, binding_selector, join_options[:hints] || [], opts)
      end

      defp build_join_expr(
             schema_source,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:schema, target_schema, join_options},
             opts
           ) do
        on_value = on_expr(schema_source, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix]
        as = join_options[:as]

        schema_source =
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

        joined_query =
          Query.join(
            query,
            qualifier,
            [unquote_splicing(quoted_binding_body)],
            joined in ^schema_source,
            as: ^as,
            on: ^on_value,
            prefix: ^prefix
          )

        apply_join_hints(joined_query, binding_selector, join_options[:hints] || [], opts)
      end

      defp build_join_expr(
             schema_source,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:table, table_name, join_options},
             opts
           ) do
        on_value = on_expr(schema_source, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix]
        as = join_options[:as]

        joined_query =
          Query.join(
            query,
            qualifier,
            [unquote_splicing(quoted_binding_body)],
            joined in ^table_name,
            as: ^as,
            on: ^on_value,
            prefix: ^prefix
          )

        apply_join_hints(joined_query, binding_selector, join_options[:hints] || [], opts)
      end

      defp build_join_expr(
             schema_source,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:query, source_query, join_options},
             opts
           ) do
        on_value = on_expr(schema_source, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix]
        as = join_options[:as]

        unless is_struct(source_query, Ecto.Query) do
          raise ArgumentError,
                "Expected source query to be a struct, got: #{inspect(source_query)}"
        end

        joined_query =
          Query.join(
            query,
            qualifier,
            [unquote_splicing(quoted_binding_body)],
            joined in ^source_query,
            as: ^as,
            on: ^on_value,
            prefix: ^prefix
          )

        apply_join_hints(joined_query, binding_selector, join_options[:hints] || [], opts)
      end

      defp build_join_expr(
             schema_source,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:subquery, params, join_options},
             opts
           ) do
        on_value = on_expr(schema_source, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix]
        as = join_options[:as]

        subquery_source =
          case params do
            %Ecto.Query{} = query ->
              query

            %Ecto.SubQuery{} = subquery ->
              subquery

            subquery_params ->
              from = subquery_params[:from] || schema_source
              filter_params = subquery_params[:query] || []
              CommonFilters.convert_params_to_filter(from, filter_params, opts)
          end

        Query.join(
          query,
          qualifier,
          [unquote_splicing(quoted_binding_body)],
          joined in subquery(subquery_source),
          as: ^as,
          on: ^on_value,
          prefix: ^prefix
        )
      end

      defp build_join_expr(
             schema_source,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:fragment, params, join_options},
             opts
           ) do
        on_value = on_expr(schema_source, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix]
        as = join_options[:as]

        source_name = params[:name]
        source_values = params[:values]

        if is_nil(source_name) do
          raise ArgumentError, "Join source name is required, got: #{inspect(params)}"
        end

        if is_nil(source_values) do
          raise ArgumentError, "Join source values are required, got: #{inspect(params)}"
        end

        case resolve_join_source_expr(binding_selector, source_name, source_values, opts) do
          {:ok, expr} ->
            joined_query =
              Query.join(
                query,
                qualifier,
                [unquote_splicing(quoted_binding_body)],
                joined in ^expr,
                as: ^as,
                on: ^on_value,
                prefix: ^prefix
              )

            apply_join_hints(joined_query, binding_selector, join_options[:hints] || [], opts)

          :error ->
            query
        end
      end
  end

  defp resolve_join_source_expr(binding_selector, source_key, source_params, opts) do
    mod = Keyword.get(opts, :join_source_module, Config.join_source_module())

    unless Code.ensure_loaded?(mod) and function_exported?(mod, :resolve_join_source, 3) do
      raise ArgumentError,
            "Expected join source module to have a resolve_join_source/3 function, got: #{inspect(mod)}"
    end

    case mod.resolve_join_source(binding_selector, source_key, source_params) do
      {:ok, source} ->
        {:ok, source}

      {:error, reason} ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Join source callback returned error for key #{inspect(source_key)}: #{inspect(reason)}"
        )

        :error

      other ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected join source callback to return {:ok, source} | {:error, reason}, got: #{inspect(other)}"
        )

        :error
    end
  end

  defp apply_join_hints(query, binding_selector, hints, opts) do
    mod = Keyword.get(opts, :join_source_module, Config.join_source_module())

    if Code.ensure_loaded?(mod) and function_exported?(mod, :build_hint, 3) do
      Enum.reduce(hints, query, fn hint_name, query_acc ->
        case mod.build_hint(query_acc, binding_selector, hint_name) do
          {:ok, %Ecto.Query{} = query} ->
            query

          {:error, reason} ->
            EctoShorts.Logger.warning(
              @logger_prefix,
              "Join hint callback returned error for hint #{inspect(hint_name)}: #{inspect(reason)}"
            )

            :error

          other ->
            EctoShorts.Logger.warning(
              @logger_prefix,
              "Expected join hint callback to return {:ok, source} | {:error, reason}, got: #{inspect(other)}"
            )

            :error
        end
      end)
    else
      query
    end
  end

  defp on_expr(schema_source, binding_selector, on_param, opts) do
    case on_param do
      true ->
        true

      nil ->
        true

      list when is_list(list) ->
        if Keyword.keyword?(list) do
          Dynamics.convert_to_dynamic(schema_source, binding_selector, list, opts)
        else
          EctoShorts.Logger.error(
            @logger_prefix,
            "Expected :on to be a keyword list, got: #{inspect(list)}"
          )

          true
        end

      on_params when is_map(on_params) ->
        Dynamics.convert_to_dynamic(schema_source, binding_selector, on_params, opts)

      term ->
        EctoShorts.Logger.error(
          @logger_prefix,
          "Expected :on to be a keyword list, map, or true, got: #{inspect(term)}"
        )

        true
    end
  end
end
