defmodule EctoShorts.CommonFilters.Join do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds join expressions from data-driven params.

  Supports six join source types: `:association`, `:schema`, `:table`,
  `:query`, `:subquery`, and `:fragment`. Each join entry must include a
  `:source` key identifying what to join against, plus optional `:qualifier`
  (default `:inner`), `:on`, `:as`, `:prefix`, and `:hints` keys.

  Invalid join payloads are logged and skipped for that join entry. This
  includes invalid `:on` payloads, which are not applied as `on: true`.

  Association keys found in the schema's associations list are automatically
  wrapped as `{:association, ...}` joins.
  """

  alias EctoShorts.CommonFilters
  alias EctoShorts.CommonSchema
  alias EctoShorts.Compiler
  alias EctoShorts.Dynamics
  alias EctoShorts.Logger
  alias EctoShorts.FragmentProvider

  alias Ecto.Query

  require Ecto.Query
  require EctoShorts.Compiler

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
          Logger.warning(
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
        Logger.warning(
          @logger_prefix,
          "Expected :join params to be a map or keyword list, got: #{inspect(other)}"
        )

        q2
    end)
  end

  defp reduce_join(schema_source, query, binding_selector, {join_type, join_options}, opts) do
    {op_source, join_options} = Keyword.pop(join_options, :source)

    if op_source !== nil do
      apply_join_expr(
        schema_source,
        query,
        binding_selector,
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

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp apply_join_expr(
             schema_source,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:association, assoc_key, join_options},
             opts
           ) do
        case on_expr(schema_source, binding_selector, join_options[:on], opts) do
          {:ok, on_value} ->
            qualifier = join_options[:qualifier] || :inner
            prefix = join_options[:prefix]
            as = join_options[:as]
            hints = join_options[:hints]

            build_join(
              query,
              binding_selector,
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
             unquote(quoted_binding_head) = binding_selector,
             {:schema, target_schema, join_options},
             opts
           ) do
        case on_expr(schema_source, binding_selector, join_options[:on], opts) do
          {:ok, on_value} ->
            qualifier = join_options[:qualifier] || :inner
            prefix = join_options[:prefix]
            as = join_options[:as]
            hints = join_options[:hints]

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

            build_join(
              query,
              binding_selector,
              qualifier,
              {:source, schema_source},
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
             unquote(quoted_binding_head) = binding_selector,
             {:table, table_name, join_options},
             opts
           ) do
        case on_expr(schema_source, binding_selector, join_options[:on], opts) do
          {:ok, on_value} ->
            qualifier = join_options[:qualifier] || :inner
            prefix = join_options[:prefix]
            as = join_options[:as]
            hints = join_options[:hints]

            build_join(
              query,
              binding_selector,
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
             unquote(quoted_binding_head) = binding_selector,
             {:query, source_query, join_options},
             opts
           ) do
        case on_expr(schema_source, binding_selector, join_options[:on], opts) do
          {:ok, on_value} ->
            qualifier = join_options[:qualifier] || :inner
            prefix = join_options[:prefix]
            as = join_options[:as]
            hints = join_options[:hints]

            unless is_struct(source_query, Ecto.Query) do
              raise ArgumentError,
                    "Expected source query to be a struct, got: #{inspect(source_query)}"
            end

            build_join(
              query,
              binding_selector,
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
             unquote(quoted_binding_head) = binding_selector,
             {:subquery, params, join_options},
             opts
           ) do
        case on_expr(schema_source, binding_selector, join_options[:on], opts) do
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
              binding_selector,
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
             unquote(quoted_binding_head) = binding_selector,
             {:fragment, params, join_options},
             opts
           ) do
        case on_expr(schema_source, binding_selector, join_options[:on], opts) do
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

            case resolve_expr_source(binding_selector, source_name, source_values, opts) do
              {:ok, expr} ->
                build_join(
                  query,
                  binding_selector,
                  qualifier,
                  {:source, expr},
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
               unquote(quoted_binding_head) = _binding_selector,
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
      end

      defp build_join(
             query,
             unquote(quoted_binding_head) = _binding_selector,
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

      for {hint_key, hint_value} <- @hints do
        defp build_join(
               query,
               unquote(quoted_binding_head) = _binding_selector,
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
      end

      defp build_join(
             query,
             unquote(quoted_binding_head) = _binding_selector,
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

      for {hint_key, hint_value} <- @hints do
        defp build_join(
               query,
               unquote(quoted_binding_head) = _binding_selector,
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
             unquote(quoted_binding_head) = _binding_selector,
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

  defp resolve_expr_source(binding_selector, source_key, source_params, opts) do
    case FragmentProvider.build_fragment_expression(binding_selector, source_key, source_params, opts) do
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
          "Expected join source callback to return {:ok, source} | {:error, reason}, got: #{inspect(other)}"
        )

        :error
    end
  end

  defp on_expr(schema_source, binding_selector, on_param, opts) do
    case on_param do
      true ->
        {:ok, true}

      nil ->
        {:ok, true}

      list when is_list(list) ->
        if Keyword.keyword?(list) do
          {:ok, Dynamics.convert_to_dynamic(schema_source, binding_selector, list, opts)}
        else
          Logger.warning(
            @logger_prefix,
            "Expected :on to be a keyword list, map, or true, got: #{inspect(list)}"
          )

          :error
        end

      on_params when is_map(on_params) ->
        {:ok, Dynamics.convert_to_dynamic(schema_source, binding_selector, on_params, opts)}

      term ->
        Logger.warning(
          @logger_prefix,
          "Expected :on to be a keyword list, map, or true, got: #{inspect(term)}"
        )

        :error
    end
  end
end
