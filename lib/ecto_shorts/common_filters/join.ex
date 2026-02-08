defmodule EctoShorts.CommonFilters.Join do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler
  alias EctoShorts.Config
  alias EctoShorts.Dynamics
  alias EctoShorts.CommonFilters

  require Ecto.Query
  require EctoShorts.Compiler

  @logger_prefix "EctoShorts.CommonFilters.Join"

  @join_types [:association, :schema, :table, :query, :subquery, :fragment]
  @doc false
  def build(_schema, _filter_key, query, _binding_selector, [], _opts) do
    query
  end

  def build(schema, filter_key, query, binding_selector, [head | _] = params_list, opts)
      when is_map(head) or is_list(head) do
    Enum.reduce(params_list, query, fn params, q2 ->
      build(schema, filter_key, q2, binding_selector, params, opts)
    end)
  end

  def build(schema, filter_key, query, binding_selector, params, opts)
      when is_map(params) and not is_struct(params) do
    build(schema, filter_key, query, binding_selector, Map.to_list(params), opts)
  end

  def build(schema, :join, query, binding_selector, params, opts) do
    if Keyword.has_key?(params, :type) do
      {join_type, join_options} = Keyword.pop(params, :type)

      if join_type in @join_types do
        reduce_join(schema, query, binding_selector, {join_type, join_options}, opts)
      else
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected join type to be one of #{@join_types}, got: #{inspect(join_type)}"
        )

        query
      end
    else
      Enum.reduce(params, query, fn
        {join_type, join_options}, q2 when join_type in @join_types ->
          reduce_join(schema, q2, binding_selector, {join_type, join_options}, opts)

        {key, _}, q2 ->
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected join type to be one of #{inspect(@join_types)}, got: #{inspect(key)}"
          )

          q2
      end)
    end
  end

  defp reduce_join(schema, query, binding_selector, {join_type, join_options}, opts) do
    {source, join_options} = Keyword.pop(join_options, :source)

    if not is_nil(source) do
      apply_join_expr(
        schema,
        query,
        binding_selector,
        {join_type, source, join_options},
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
      defp apply_join_expr(
             schema,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:association, assoc_key, join_options},
             opts
           ) do
        on_value = on_expr(schema, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix] || ""
        as = join_options[:as]

        Query.join(
          query,
          qualifier,
          [unquote_splicing(quoted_binding_body)],
          joined in assoc(unquote(target_binding_var), ^assoc_key),
          as: ^as,
          on: ^on_value,
          prefix: ^prefix
        )
      end

      defp apply_join_expr(
             schema,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:schema, target_schema, join_options},
             opts
           ) do
        on_value = on_expr(schema, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix] || ""
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

        Query.join(
          query,
          qualifier,
          [unquote_splicing(quoted_binding_body)],
          joined in ^schema_source,
          as: ^as,
          on: ^on_value,
          prefix: ^prefix
        )
      end

      defp apply_join_expr(
             schema,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:table, table_name, join_options},
             opts
           ) do
        on_value = on_expr(schema, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix] || ""
        as = join_options[:as]

        Query.join(
          query,
          qualifier,
          [unquote_splicing(quoted_binding_body)],
          joined in ^table_name,
          as: ^as,
          on: ^on_value,
          prefix: ^prefix
        )
      end

      defp apply_join_expr(
             schema,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:query, source_query, join_options},
             opts
           ) do
        on_value = on_expr(schema, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix] || ""
        as = join_options[:as]

        unless is_struct(source_query, Ecto.Query) do
          raise ArgumentError,
                "Expected source query to be a struct, got: #{inspect(source_query)}"
        end

        Query.join(
          query,
          qualifier,
          [unquote_splicing(quoted_binding_body)],
          joined in ^source_query,
          as: ^as,
          on: ^on_value,
          prefix: ^prefix
        )
      end

      defp apply_join_expr(
             schema,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:subquery, params, join_options},
             opts
           ) do
        on_value = on_expr(schema, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix] || ""
        as = join_options[:as]

        from_query =
          case params do
            %Ecto.Query{} = query ->
              query

            subquery_params ->
              source = subquery_params[:source]
              filter_params = subquery_params[:query] || []

              if is_nil(source) do
                raise ArgumentError, "Source is required, got: #{inspect(subquery_params)}"
              end

              CommonFilters.convert_params_to_filter(source, filter_params, opts)
          end

        Query.join(
          query,
          qualifier,
          [unquote_splicing(quoted_binding_body)],
          joined in subquery(from_query),
          as: ^as,
          on: ^on_value,
          prefix: ^prefix
        )
      end

      defp apply_join_expr(
             schema,
             query,
             unquote(quoted_binding_head) = binding_selector,
             {:fragment, params, join_options},
             opts
           ) do
        on_value = on_expr(schema, binding_selector, join_options[:on], opts)

        qualifier = join_options[:qualifier] || :inner
        prefix = join_options[:prefix] || ""
        as = join_options[:as]

        fragment_name = params[:name]
        fragment_values = params[:values]

        if is_nil(fragment_name) do
          raise ArgumentError, "Fragment name is required, got: #{inspect(params)}"
        end

        if is_nil(fragment_values) do
          raise ArgumentError, "Fragment values are required, got: #{inspect(params)}"
        end

        case resolve_fragment(binding_selector, fragment_name, fragment_values, opts) do
          {:ok, fragment} ->
            Query.join(
              query,
              qualifier,
              [unquote_splicing(quoted_binding_body)],
              joined in ^fragment,
              as: ^as,
              on: ^on_value,
              prefix: ^prefix
            )

          :error ->
            query
        end
      end
  end

  defp resolve_fragment(binding_selector, fragment_key, fragment_params, opts) do
    mod = Keyword.get(opts, :fragment_module, Config.fragment_module())

    unless function_exported?(mod, :fragment, 3) do
      raise ArgumentError,
            "Expected fragment module to have a fragment/3 function, got: #{inspect(mod)}"
    end

    case mod.fragment(binding_selector, fragment_key, fragment_params) do
      {:ok, fragment} ->
        {:ok, fragment}

      {:error, reason} ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Fragment callback returned error for key #{inspect(fragment_key)}: #{inspect(reason)}"
        )

        :error

      other ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected fragment callback to return {:ok, source} | {:error, reason}, got: #{inspect(other)}"
        )

        :error
    end
  end

  defp on_expr(schema, binding_selector, on_param, opts) do
    case on_param do
      true ->
        true

      nil ->
        true

      list when is_list(list) ->
        if Keyword.keyword?(list) do
          Dynamics.convert_to_dynamic(schema, binding_selector, list, opts)
        else
          EctoShorts.Logger.error(
            @logger_prefix,
            "Expected :on to be a keyword list, got: #{inspect(list)}"
          )

          true
        end

      on_params when is_map(on_params) ->
        Dynamics.convert_to_dynamic(schema, binding_selector, on_params, opts)

      term ->
        EctoShorts.Logger.error(
          @logger_prefix,
          "Expected :on to be a keyword list, map, or true, got: #{inspect(term)}"
        )

        true
    end
  end
end
