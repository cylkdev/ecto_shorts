defmodule EctoShorts.CommonFilters do
  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonQuery
  alias EctoShorts.QueryBuilder

  @binding_operators [:as, :at]
  @delegated_filters [:join, :offset, :limit, :select, :select_merge]

  @default_binding_selector {:as, nil}
  @default_filter :where

  def convert_params_to_filter(source, args, opts) do
    schema_source = CommonSchema.normalize_source(source)

    query = CommonSchema.to_query(source)

    entries =
      args
      |> list_wrap()
      |> Enum.map(&normalize_params/1)

    Enum.reduce(entries, query, fn entry, query_acc ->
      if is_map(entry) or Keyword.keyword?(entry) do
        reduce_params(
          schema_source,
          query_acc,
          @default_binding_selector,
          @default_filter,
          entry,
          opts
        )
      else
        raise ArgumentError, "Expected args to be a map or list, got: #{inspect(entry)}"
      end
    end)
  end

  defp reduce_params(source, query, bind_select, current_filter, {key, value}, opts) do
    cond do
      key in @delegated_filters ->
        apply_query_builder(source, query, bind_select, key, value, opts)

      key in @binding_operators ->
        Enum.reduce(value, query, fn {bind_to, params}, query_acc ->
          reduce_params(source, query_acc, {key, bind_to}, current_filter, params, opts)
        end)

      true ->
        build_schema_filters(source, query, bind_select, current_filter, {key, value}, opts)
    end
  end

  defp reduce_params(source, query, bind_select, current_filter, args, opts) do
    if is_map(args) do
      reduce_params(source, query, bind_select, current_filter, Map.to_list(args), opts)
    else
      if Keyword.keyword?(args) do
        Enum.reduce(args, query, fn {key, value}, query_acc ->
          reduce_params(source, query_acc, bind_select, current_filter, {key, value}, opts)
        end)
      else
        apply_query_builder(source, query, bind_select, current_filter, args, opts)
      end
    end
  end

  defp build_schema_filters(source, query, bind_select, current_filter, {key, value}, opts) do
    cond do
      is_map(value) ->
        reduce_params(
          source,
          query,
          bind_select,
          current_filter,
          {key, Map.to_list(value)},
          opts
        )

      is_list(value) ->
        if Keyword.keyword?(value) do
          Enum.reduce(value, query, fn {key2, value2}, query_acc ->
            reduce_params(
              source,
              query_acc,
              bind_select,
              current_filter,
              {key, {key2, value2}},
              opts
            )
          end)
        else
          apply_query_builder(source, query, bind_select, current_filter, {key, value}, opts)
        end

      true ->
        apply_query_builder(source, query, bind_select, current_filter, {key, value}, opts)
    end
  end

  defp apply_query_builder(source, query, bind_select, current_filter, args, opts) do
    source
    |> resolve_binding_source(query, bind_select)
    |> QueryBuilder.build_query(
      query,
      bind_select,
      current_filter,
      args,
      opts
    )
  end

  defp resolve_binding_source(source, _query, {:as, nil}) do
    source
  end

  defp resolve_binding_source(_source, query, {_bind_op, bind_to}) do
    CommonQuery.get_query_binding_source(query, bind_to)
  end

  defp list_wrap(term) do
    cond do
      is_map(term) ->
        [term]

      is_list(term) ->
        if Keyword.keyword?(term) do
          [term]
        else
          term
        end

      true ->
        [term]
    end
  end

  defp normalize_params({k, v}) when is_map(v) or is_list(v) do
    {k, normalize_params(v)}
  end

  defp normalize_params(map) when is_map(map) and not is_struct(map) do
    map
    |> Map.to_list()
    |> normalize_params()
  end

  defp normalize_params(list) when is_list(list) do
    cond do
      Keyword.keyword?(list) ->
        list
        |> sort_params()
        |> Enum.map(fn {k, v} -> {k, normalize_params(v)} end)

      true ->
        Enum.map(list, &normalize_params/1)
    end
  end

  defp normalize_params(term) do
    term
  end

  defp sort_params(params) do
    where_filters = Keyword.take(params, [:where])
    or_where_filters = Keyword.take(params, [:or_where])

    last_filter =
      case List.keyfind(params, :last, 0) do
        nil -> []
        last -> [last]
      end

    # Regular field filters should be processed with where_filters since they
    # become implicit WHERE clauses and must come before or_where filters
    field_filters = Keyword.drop(params, [:where, :or_where, :last])

    where_filters
    |> Kernel.++(field_filters)
    |> Kernel.++(or_where_filters)
    |> Kernel.++(last_filter)
  end
end

# defmodule EctoShorts.CommonFilters do
#   @moduledoc """
#   Provides an API that allows a data-driven approach to applying filters to Ecto queries.

#   ## Sources

#   A `source` is the starting point for a query. It can be one of the following:

#     - `Ecto.Query.t()` - An Ecto.Query struct
#     - `binary()` - The table name as a string
#     - `Ecto.Schema.t()` - An Ecto.Schema module
#     - `{binary(), Ecto.Schema.t()}` - The table name and the Ecto.Schema module
#     - `{nil, Ecto.Schema.t()}` - No table name and an Ecto.Schema module
#     - `{binary(), nil}` - The table name and no Ecto.Schema module

#   ## Schemaless Operations

#   See Ecto documentation for more information:

#   - [Schemaless Queries](https://hexdocs.pm/ecto/schemaless-queries.html)

#   ### Binding Parameters

#   In this library you can target different bindings in an Ecto query using:

#     - `:as` (named binding)
#       - `example`: %{as: %{post: %{where: %{published: true}}}}

#     - `:at` (index binding)
#       - `example`: %{at: %{1 => %{where: %{published: true}}}}

#   In other words the binding parameters are a map or keyword list that appears after
#   `:as` or `:at` and it’s a container of “which binding should get which filters”.
#   """
#   alias Ecto.Queryable
#   alias EctoShorts.CommonFilters.Pagination

#   alias EctoShorts.{
#     QueryBuilder,
#     CommonQuery,
#     Utils
#   }

#   @logger_prefix "EctoShorts.CommonFilters"

#   @binding_operators [:as, :at]
#   @boolean_operators [:and, :or]
#   @pagination_filters [:first, :last, :limit, :offset, :order_by, :preload]
#   @where_filters [:where, :or_where]
#   @delegated_filters [:join, :select, :select_merge]

#   @doc """
#   Convert a map of params to a query.

#   ### Examples

#       iex> EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{select: true})
#       #Ecto.Query<from p0 in EctoShorts.Schema.Post, select: p0>

#       iex> EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{select: [:id]})
#       #Ecto.Query<from p0 in EctoShorts.Schema.Post, select: [:id]>

#       iex> EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{select: :id})
#       #Ecto.Query<from p0 in EctoShorts.Schema.Post, select: p0.id>

#       iex> EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{select: %{custom_id: :id}})
#       #Ecto.Query<from p0 in EctoShorts.Schema.Post, select: %{custom_id: p0.id}>

#       iex> import Ecto.Query
#       ...> q = from u in EctoShorts.Schema.Post, as: :custom_alias
#       ...> EctoShorts.CommonFilters.convert_params_to_filter(q, %{as: %{custom_alias: %{where: %{first_name: %{==: "John"}}}}})
#       #Ecto.Query<from u0 in EctoShorts.Schema.Post, as: :custom_alias, where: u0.first_name == ^"John">

#       iex> import Ecto.Query
#       ...> q = from u in EctoShorts.Schema.Post
#       ...> EctoShorts.CommonFilters.convert_params_to_filter(q, %{at: %{1 => %{where: %{first_name: %{==: "John"}}}}})
#       #Ecto.Query<from u0 in EctoShorts.Schema.Post, as: :custom_alias, where: u0.first_name == ^"John">
#   """
#   def convert_params_to_filter(source, params, opts \\ [])

#   def convert_params_to_filter(source, params, opts) when is_map(params) do
#     convert_to_query_language(source, Map.to_list(params), opts)
#   end

#   def convert_params_to_filter(source, entries, opts) when is_list(entries) do
#     if Keyword.keyword?(entries) do
#       convert_to_query_language(source, entries, opts)
#     else
#       Enum.reduce(entries, source, fn params, acc ->
#         convert_to_query_language(acc, params, opts)
#       end)
#     end
#   end

#   defp convert_to_query_language(source, params, opts) do
#     {table_name, schema} = normalize_schema_source(source)

#     query = to_query(source, {table_name, schema})

#     # TODO: When querying by bare table name (schema is nil), callers may need to explicitly
#     # provide a :select (Ecto.Repo.all/2 does not auto-select fields without a schema).

#     if (is_map(params) and not is_struct(params)) or Utils.key_values?(params) do
#       params
#       |> normalize_params()
#       |> Enum.reduce(query, fn {_, _} = p, q ->
#         apply_query_builder_params(schema, p, q, opts)
#       end)
#     else
#       EctoShorts.Logger.warning(
#         @logger_prefix,
#         "Expected params to be a map or keyword list, got: #{inspect(params)}. " <>
#           "No filters were applied to the query."
#       )

#       query
#     end
#   end

#   defp to_query(source_arg, normalized_source) do
#     case {source_arg, normalized_source} do
#       {%Ecto.Query{} = query, _} ->
#         query

#       {_, {nil, nil}} ->
#         raise ArgumentError, """
#         Expected source to be one of the following:

#         - `Ecto.Query.t()` - An Ecto.Query struct
#         - `binary()` - The table name as a string
#         - `Ecto.Schema.t()` - An Ecto.Schema module
#         - `{binary(), Ecto.Schema.t()}` - The table name and the Ecto.Schema module
#         - `{nil, Ecto.Schema.t()}` - No table name and an Ecto.Schema module
#         - `{binary(), nil}` - The table name and no Ecto.Schema module

#         got:

#         #{inspect(source_arg)}
#         """

#       {_, {nil, schema}} ->
#         Queryable.to_query(schema)

#       {_, {table_name, nil}} when is_binary(table_name) ->
#         Queryable.to_query(table_name)

#       {_, {table_name, schema}} ->
#         Queryable.to_query({table_name, schema})
#     end
#   end

#   defp normalize_schema_source(%Ecto.Query{} = query) do
#     query
#     |> CommonQuery.get_query_source()
#     |> normalize_schema_source()
#   end

#   defp normalize_schema_source({nil, schema}) when is_atom(schema) do
#     {nil, schema}
#   end

#   defp normalize_schema_source({source, nil}) when is_binary(source) do
#     {source, nil}
#   end

#   defp normalize_schema_source({source, schema}) when is_binary(source) and is_atom(schema) do
#     {source, schema}
#   end

#   defp normalize_schema_source(schema) when is_atom(schema) do
#     {nil, schema}
#   end

#   defp normalize_schema_source(source) when is_binary(source) do
#     {source, nil}
#   end

#   defp normalize_schema_source(_) do
#     {nil, nil}
#   end

#   defp apply_query_builder_params(schema, {key, value}, query, opts)
#        when key in @pagination_filters do
#     apply_pagination_filter(schema, key, value, query, opts)
#   end

#   defp apply_query_builder_params(schema, {bool_op, values}, query, opts)
#        when bool_op in @boolean_operators and is_list(values) do
#     apply_query_builder(
#       schema,
#       nil,
#       query,
#       {:as, nil},
#       {bool_op, values},
#       opts
#     )
#   end

#   defp apply_query_builder_params(schema, {bind_op, binding_params}, query, opts)
#        when bind_op in @binding_operators do
#     if Utils.key_values?(binding_params) do
#       Enum.reduce(binding_params, query, fn {bind_to, filter_params}, updated_query ->
#         if binding_selector?(bind_op, bind_to) do
#           apply_binding_filter_param(
#             schema,
#             bind_op,
#             bind_to,
#             filter_params,
#             updated_query,
#             opts
#           )
#         else
#           EctoShorts.Logger.warning(
#             @logger_prefix,
#             invalid_binding_selector_message(bind_op, bind_to)
#           )

#           updated_query
#         end
#       end)
#     else
#       EctoShorts.Logger.warning(
#         @logger_prefix,
#         "Expected binding params for #{inspect(bind_op)} to be a map or keyword list, got: #{inspect(binding_params)}"
#       )

#       query
#     end
#   end

#   defp apply_query_builder_params(
#          schema,
#          {filter, {level_1_key, {level_2_key, {level_3_key, filter_value}}}},
#          query,
#          opts
#        )
#        when is_map(filter_value) or is_list(filter_value) do
#     if Utils.key_values?(filter_value) and level_2_key not in @boolean_operators do
#       Enum.reduce(filter_value, query, fn value, updated_query ->
#         apply_query_builder_params(
#           schema,
#           {filter, {level_1_key, {level_2_key, {level_3_key, value}}}},
#           updated_query,
#           opts
#         )
#       end)
#     else
#       apply_query_builder(
#         schema,
#         filter,
#         query,
#         {:as, nil},
#         {level_1_key, {level_2_key, {level_3_key, filter_value}}},
#         opts
#       )
#     end
#   end

#   defp apply_query_builder_params(
#          schema,
#          {filter, {level_1_key, {level_2_key, term}}},
#          query,
#          opts
#        )
#        when is_map(term) or is_list(term) do
#     if Utils.key_values?(term) and level_2_key not in @boolean_operators do
#       Enum.reduce(term, query, fn value, updated_query ->
#         apply_query_builder_params(
#           schema,
#           {filter, {level_1_key, {level_2_key, value}}},
#           updated_query,
#           opts
#         )
#       end)
#     else
#       apply_query_builder(
#         schema,
#         filter,
#         query,
#         {:as, nil},
#         {level_1_key, {level_2_key, term}},
#         opts
#       )
#     end
#   end

#   defp apply_query_builder_params(schema, {filter, {level_1_key, {level_2_key, value}}}, query, opts) do
#     apply_query_builder(
#       schema,
#       filter,
#       query,
#       {:as, nil},
#       {level_1_key, {level_2_key, value}},
#       opts
#     )
#   end

#   defp apply_query_builder_params(schema, {filter, {field, term}}, query, opts) do
#     if Utils.key_values?(term) do
#       is_association? = is_atom(schema) and schema_association?(schema, field)

#       if is_association? and (is_nil(filter) or filter in @where_filters) do
#         join_and_apply_assoc_filters(
#           schema,
#           query,
#           {:as, nil},
#           {filter, {field, term}},
#           opts
#         )
#       else
#         Enum.reduce(term, query, fn value, updated_query ->
#           apply_query_builder_params(schema, {filter, {field, value}}, updated_query, opts)
#         end)
#       end
#     else
#       apply_query_builder(
#         schema,
#         filter,
#         query,
#         {:as, nil},
#         {field, term},
#         opts
#       )
#     end
#   end

#   defp apply_query_builder_params(schema, {filter, filter_term}, query, opts)
#        when filter in @delegated_filters do
#     apply_query_builder(schema, filter, query, {:as, nil}, filter_term, opts)
#   end

#   defp apply_query_builder_params(schema, {filter, filter_term}, query, opts)
#        when is_nil(filter) or filter in @where_filters do
#     if Utils.key_values?(filter_term) do
#       Enum.reduce(filter_term, query, fn value, updated_query ->
#         apply_query_builder_params(schema, {filter, value}, updated_query, opts)
#       end)
#     else
#       EctoShorts.Logger.warning(
#         @logger_prefix,
#         "Expected #{inspect(filter)} to be a map or keyword list, got: #{inspect(filter_term)}"
#       )

#       query
#     end
#   end

#   defp apply_query_builder_params(schema, {filter, {boolean_operator, filter_values}}, query, opts)
#        when (is_nil(filter) or filter in @where_filters) and
#               boolean_operator in @boolean_operators and
#               is_list(filter_values) do
#     apply_query_builder(
#       schema,
#       filter,
#       query,
#       {:as, nil},
#       {boolean_operator, filter_values},
#       opts
#     )
#   end

#   defp apply_query_builder_params(schema, {key, value}, query, opts) do
#     apply_query_builder_params(schema, {nil, %{key => value}}, query, opts)
#   end

#   defp apply_binding_filter_param(
#          schema,
#          bind_op,
#          bind_to,
#          nested_params,
#          query,
#          opts
#        ) do
#     if Utils.key_values?(nested_params) do
#       Enum.reduce(nested_params, query, fn {filter_key, filter_value}, updated_query ->
#         apply_schema_binding_filter(
#           schema,
#           bind_op,
#           bind_to,
#           filter_key,
#           filter_value,
#           updated_query,
#           opts
#         )
#       end)
#     else
#       EctoShorts.Logger.warning(
#         @logger_prefix,
#         "Expected nested params for #{inspect(bind_op)} #{inspect(bind_to)} to be a map or keyword list, got: #{inspect(nested_params)}"
#       )

#       query
#     end
#   end

#   defp apply_schema_binding_filter(
#          schema,
#          bind_op,
#          bind_to,
#          filter,
#          {field, {operator, value}},
#          query,
#          opts
#        ) do
#     apply_query_builder(
#       schema,
#       filter,
#       query,
#       {bind_op, bind_to},
#       {field, {operator, value}},
#       opts
#     )
#   end

#   defp apply_schema_binding_filter(
#          schema,
#          bind_op,
#          bind_to,
#          filter,
#          {field, filter_params},
#          query,
#          opts
#        )
#        when (is_map(filter_params) or is_list(filter_params)) and
#               (is_nil(filter) or filter in @where_filters) do
#     if Utils.key_values?(filter_params) and not is_nil(schema) and
#          schema_association?(schema, field) do
#       join_and_apply_assoc_filters(
#         schema,
#         query,
#         {bind_op, bind_to},
#         {filter, {field, filter_params}},
#         opts
#       )
#     else
#       Enum.reduce(filter_params, query, fn value, updated_query ->
#         apply_schema_binding_filter(
#           schema,
#           bind_op,
#           bind_to,
#           filter,
#           {field, value},
#           updated_query,
#           opts
#         )
#       end)
#     end
#   end

#   defp apply_schema_binding_filter(
#          schema,
#          bind_op,
#          bind_to,
#          filter,
#          {field, term},
#          query,
#          opts
#        ) do
#     apply_query_builder(
#       schema,
#       filter,
#       query,
#       {bind_op, bind_to},
#       {field, term},
#       opts
#     )
#   end

#   defp apply_schema_binding_filter(
#          schema,
#          bind_op,
#          bind_to,
#          filter,
#          select_value,
#          query,
#          opts
#        )
#        when filter in @delegated_filters do
#     apply_query_builder(
#       schema,
#       filter,
#       query,
#       {bind_op, bind_to},
#       select_value,
#       opts
#     )
#   end

#   defp apply_schema_binding_filter(
#          schema,
#          bind_op,
#          bind_to,
#          filter,
#          term,
#          query,
#          opts
#        )
#        when is_nil(filter) or filter in @where_filters do
#     if Utils.key_values?(term) do
#       Enum.reduce(term, query, fn {_, _} = value, updated_query ->
#         apply_schema_binding_filter(
#           schema,
#           bind_op,
#           bind_to,
#           filter,
#           value,
#           updated_query,
#           opts
#         )
#       end)
#     else
#       EctoShorts.Logger.warning(
#         @logger_prefix,
#         "Expected params for binding #{inspect(bind_op)} #{inspect(bind_to)} to be a map or keyword list, got: #{inspect(term)}"
#       )

#       query
#     end
#   end

#   defp apply_schema_binding_filter(
#          schema,
#          bind_op,
#          bind_to,
#          field,
#          field_value,
#          query,
#          opts
#        )
#        when is_atom(field) do
#     apply_schema_binding_filter(
#       schema,
#       bind_op,
#       bind_to,
#       nil,
#       {field, field_value},
#       query,
#       opts
#     )
#   end

#   defp apply_schema_binding_filter(
#          _schema,
#          bind_op,
#          bind_to,
#          filter_key,
#          filter_value,
#          query,
#          _opts
#        ) do
#     EctoShorts.Logger.warning(
#       @logger_prefix,
#       "Unexpected filter key/value for binding #{inspect(bind_op)} #{inspect(bind_to)}, got: #{inspect({filter_key, filter_value})}"
#     )

#     query
#   end

#   defp apply_pagination_filter(schema, filter, filter_value, query, _opts)
#        when is_map(filter_value) or is_list(filter_value) do
#     if Utils.key_values?(filter_value) do
#       Enum.reduce(filter_value, query, fn {key, value}, updated_query ->
#         Pagination.build_query(schema, updated_query, filter, {key, value})
#       end)
#     else
#       Pagination.build_query(schema, query, filter, filter_value)
#     end
#   end

#   defp apply_pagination_filter(schema, filter, filter_value, query, _opts) do
#     Pagination.build_query(schema, query, filter, filter_value)
#   end

#   defp join_and_apply_assoc_filters(
#          schema,
#          query,
#          {bind_op, bind_to},
#          {_filter, {assoc_key, normalized_params}},
#          opts
#        )
#        when is_map(normalized_params) or is_list(normalized_params) do
#     if Utils.key_values?(normalized_params) do
#       assoc_schema = EctoShorts.SchemaHelpers.get_related_schema(schema, assoc_key)

#       updated_query =
#         apply_query_builder(
#           schema,
#           :join,
#           query,
#           {bind_op, bind_to},
#           {:association, assoc_key, Utils.enum_take(normalized_params, [:as, :on, :type])},
#           opts
#         )

#       {next_bind_op, next_bind_to} =
#         if Utils.enum_has_key?(normalized_params, :as) do
#           {:as, Utils.enum_get(normalized_params, :as, nil)}
#         else
#           {:at, CommonQuery.query_binding_count(updated_query)}
#         end

#       apply_binding_filter_param(
#         assoc_schema,
#         next_bind_op,
#         next_bind_to,
#         Utils.enum_drop(normalized_params, [:as, :on, :type]),
#         updated_query,
#         opts
#       )
#     else
#       EctoShorts.Logger.warning(
#         @logger_prefix,
#         "Expected association params for #{inspect(assoc_key)} to be a map or keyword list, got: #{inspect(normalized_params)}"
#       )

#       query
#     end
#   end

#   defp apply_query_builder(
#          schema,
#          filter,
#          query,
#          {bind_op, bind_to},
#          filter_value,
#          opts
#        ) do
#     if binding_selector?(bind_op, bind_to) do
#       case resolve_binding_source({bind_op, bind_to}, schema, query) do
#         {:ok, binding_schema} ->
#           QueryBuilder.build_query(
#             binding_schema,
#             filter,
#             query,
#             {bind_op, bind_to},
#             filter_value,
#             opts
#           )

#         :error ->
#           EctoShorts.Logger.warning(
#             @logger_prefix,
#             "Could not resolve schema for binding #{inspect(bind_op)} #{inspect(bind_to)} in query: #{inspect(query)}"
#           )

#           query
#       end
#     else
#       EctoShorts.Logger.warning(
#         @logger_prefix,
#         invalid_binding_selector_message(bind_op, bind_to)
#       )

#       query
#     end
#   end

#   defp resolve_binding_source({bind_op, bind_to}, schema, query) do
#     case {bind_op, bind_to} do
#       {:as, nil} ->
#         {:ok, schema}

#       {_op, target} ->
#         case CommonQuery.get_query_binding_source(query, target) do
#           {_source, resolved_schema} ->
#             {:ok, resolved_schema}

#           _ ->
#             EctoShorts.Logger.warning(
#               @logger_prefix,
#               "Binding #{inspect(bind_op)} #{inspect(bind_to)} not found in query: #{inspect(query)}"
#             )

#             :error
#         end
#     end
#   end

#   defp schema_association?(schema, field) do
#     field in schema.__schema__(:associations)
#   end

#   defp normalize_params({k, v}) when is_map(v) or is_list(v) do
#     {k, normalize_params(v)}
#   end

#   defp normalize_params(map) when is_map(map) and not is_struct(map) do
#     map
#     |> Map.to_list()
#     |> normalize_params()
#   end

#   defp normalize_params(list) when is_list(list) do
#     cond do
#       Keyword.keyword?(list) ->
#         list
#         |> sort_filter_params()
#         |> Enum.map(fn {k, v} -> {k, normalize_params(v)} end)

#       Utils.key_values?(list) ->
#         list
#         |> sort_filter_params()
#         |> Enum.map(fn {k, v} -> {k, normalize_params(v)} end)

#       true ->
#         Enum.map(list, &normalize_params/1)
#     end
#   end

#   defp normalize_params(term) do
#     term
#   end

#   # ********************************** IMPORTANT **********************************
#   #
#   # Parameter ordering is critical for correct SQL generation due to how Ecto
#   # composes query clauses. `Ecto.Query.or_where/2` behaves differently depending
#   # on whether a `WHERE` clause already exists in the query:
#   #
#   # `Ecto.Query.or_where/2` behaves differently based on query state:
#   #
#   #   * If no WHERE clause exists: creates the initial WHERE clause
#   #   * If a WHERE clause exists: adds an OR condition to the existing clause
#   #
#   # The processing order must be:
#   #
#   #   1. `where` - Sets the base `WHERE` clause.
#   #   2. `params` - Given parameters are processed next which can contain implicit `where` filters.
#   #   3. `or_where` - The `:or_where` filters then add OR conditions to this base clause.
#   #   4. `last` - The `:last` filter is processed last.
#   #
#   # Let's explore two scenarios:
#   #
#   # Given params `%{published: true, or_where: %{published: false}}`
#   #
#   # Scenario 1:
#   #
#   # If the order is `published: true` -> `or_where: %{published: false}` it does
#   # the same thing as:
#   #
#   #     schema
#   #     |> Query.where([p], p.published == true)      # Creates: WHERE (published = true)
#   #     |> Query.or_where([p], p.published == false)  # Adds: OR (published = false)
#   #     #Ecto.Query<from p0 in Post, where: p0.published == true or p0.published == false>
#   #
#   # This gives us the expected result.
#   #
#   # Scenario 2:
#   #
#   # If the order is `or_where: %{published: false}` -> `published: true`:
#   #
#   #     schema
#   #     |> Query.or_where([p], p.published == false)  # Creates: WHERE (published = false)
#   #     |> Query.where([p], p.published == true)      # Adds: AND (published = true)
#   #     #Ecto.Query<from p0 in Post, where: p0.published == false, where: p0.published == true>
#   #
#   # This is incorrect because `Query.or_where/2` creates an initial WHERE clause
#   # when none exists, and subsequent `Query.where/2` calls AND new conditions
#   # together. Regular fields and `:where` filters must be processed before
#   # `:or_where` to ensure a base WHERE clause exists for `or_where/2` to
#   # correctly add OR conditions.
#   defp sort_filter_params(params) do
#     where_filters = Utils.enum_take(params, [:where])
#     or_where_filters = Utils.enum_take(params, [:or_where])

#     last_filter =
#       case List.keyfind(params, :last, 0) do
#         nil -> []
#         last -> [last]
#       end

#     # Regular field filters should be processed with where_filters since they
#     # become implicit WHERE clauses and must come before or_where filters
#     field_filters =
#       params
#       |> Utils.enum_drop([:where, :or_where, :last])
#       |> Enum.map(fn term -> {:where, term} end)

#     where_filters
#     |> Kernel.++(field_filters)
#     |> Kernel.++(or_where_filters)
#     |> Kernel.++(last_filter)
#   end

#   defp binding_selector?(:as, selector), do: is_nil(selector) or is_atom(selector)
#   defp binding_selector?(:at, selector), do: is_integer(selector)

#   defp invalid_binding_selector_message(:as, selector) do
#     "Expected named binding selector to be an atom, got: #{inspect(selector)}"
#   end

#   defp invalid_binding_selector_message(:at, selector) do
#     "Expected positional binding selector to be an integer, got: #{inspect(selector)}"
#   end
# end
