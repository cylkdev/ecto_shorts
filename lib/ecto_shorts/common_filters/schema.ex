defmodule EctoShorts.CommonFilters.Schema do
  @moduledoc false

  alias EctoShorts.CommonFilters.{
    CommonQueryBuilders,
    CommonQueryBuilders.Helpers
  }

  @query_filters ~w(where or_where)a

  def build_query(query, schema_module, params, current_binding) do
    Enum.reduce(params, query, fn {key, value}, query ->
      build_query(query, schema_module, key, value, current_binding)
    end)
  end

  def build_query(query, schema_module, key, value, current_binding) when key in @query_filters do
    build_filter_expr(query, schema_module, key, value, current_binding)
  end

  def build_query(query, schema_module, key, value, current_binding) do
    cond do
      Helpers.association?(schema_module, key) ->
        build_assoc_filter(query, schema_module, key, value, current_binding)

      Helpers.query_field?(schema_module, key) ->
        build_array_or_schema_filter(query, schema_module, key, value, current_binding)

      true ->
        query
    end
  end

  defp build_assoc_filter(query, schema_module, key, value, current_binding) do
    CommonQueryBuilders.join_association(query, current_binding, schema_module, key, value, fn
      query, assoc_schema_module, params, join_binding_alias ->
        build_query(query, assoc_schema_module, params, join_binding_alias)
    end)
  end

  defp build_array_or_schema_filter(query, schema_module, key, value, current_binding) do
    if Helpers.field_type_of_array?(schema_module, key) do
      build_array_filter(query, schema_module, key, value, current_binding)
    else
      build_schema_filter(query, schema_module, key, value, current_binding)
    end
  end

  defp build_array_filter(query, schema_module, key, value, current_binding) do
    CommonQueryBuilders.traverse_filter_params(
      query,
      schema_module,
      key,
      value,
      current_binding,
      fn
        query, current_binding, schema_module, schema_field, operator, value ->
          if is_list(value) do
            CommonQueryBuilders.where_array(
              query,
              current_binding,
              schema_module,
              schema_field,
              operator,
              value
            )
          else
            CommonQueryBuilders.where_array(
              query,
              current_binding,
              schema_module,
              value,
              operator,
              schema_field
            )
          end
      end
    )
  end

  defp build_schema_filter(query, schema_module, key, value, current_binding) do
    CommonQueryBuilders.traverse_filter_params(
      query,
      schema_module,
      key,
      value,
      current_binding,
      fn
        query, current_binding, schema_module, schema_field, operator, value ->
          CommonQueryBuilders.where_schema(
            query,
            current_binding,
            schema_module,
            schema_field,
            operator,
            value
          )
      end
    )
  end

  defp build_filter_expr(query, schema_module, key, value, current_binding) do
    CommonQueryBuilders.traverse_filter_params(
      query,
      schema_module,
      key,
      value,
      current_binding,
      fn
        query, current_binding, schema_module, filter, schema_field, value ->
          apply_schema_filter(
            query,
            current_binding,
            schema_module,
            filter,
            schema_field,
            value
          )
      end
    )
  end

  defp apply_schema_filter(
         query,
         current_binding,
         schema_module,
         :where,
         schema_field,
         {operator, value}
       ) do
    CommonQueryBuilders.where_schema(
      query,
      current_binding,
      schema_module,
      schema_field,
      operator,
      value
    )
  end

  defp apply_schema_filter(
         query,
         current_binding,
         schema_module,
         :where,
         schema_field,
         value
       ) do
    CommonQueryBuilders.where_schema(
      query,
      current_binding,
      schema_module,
      schema_field,
      :==,
      value
    )
  end

  # @doc """
  # ...
  # """
  # def query_expression(
  #       query,
  #       schema_module,
  #       :with_named_binding,
  #       {binding_alias, params},
  #       _current_binding
  #     ) do
  #   named_binding = named_binding(binding_alias)

  #   Enum.reduce(params, query, fn {schema_field, value}, query ->
  #     query_expression(query, schema_module, schema_field, value, named_binding)
  #   end)
  # end

  # def query_expression(query, schema_module, :with_named_binding, params, current_binding) do
  #   Enum.reduce(params, query, fn {binding_alias, params}, query ->
  #     query_expression(
  #       query,
  #       schema_module,
  #       :with_named_binding,
  #       {binding_alias, params},
  #       current_binding
  #     )
  #   end)
  # end

  # def query_expression(query, schema_module, :join, {schema_field, params}, current_binding)
  #     when is_list(params) do
  #   query_expression(
  #     query,
  #     schema_module,
  #     :join,
  #     {schema_field, Map.new(params)},
  #     current_binding
  #   )
  # end

  # def query_expression(query, _schema_module, :join, {:subquery, params}, current_binding) do
  #   {from, params} = Map.pop(params, :from)

  #   {as, params} = Map.pop(params, :as)

  #   join_binding_alias =
  #     with nil <- as do
  #       from
  #       |> CommonSchemas.get_schema_queryable()
  #       |> Module.split()
  #       |> List.last()
  #       |> Macro.underscore()
  #       |> named_binding()
  #     end

  #   QueryApi.join(query, {current_binding, join_binding_alias}, :subquery, from, params)
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :join,
  #       {:association, {schema_field, params}},
  #       current_binding
  #     ) do
  #   with_association(query, schema_module, schema_field, fn query, assoc_schema_module ->
  #     {as, params} = Map.pop(params, :as)

  #     join_binding_alias = as || named_binding(schema_field)

  #     join_params = Map.take(params, [:on, :qualifier, :prefix])

  #     filter_params = Map.drop(params, [:on, :qualifier, :prefix])

  #     query
  #     |> QueryApi.join(
  #       {current_binding, join_binding_alias},
  #       :association,
  #       schema_field,
  #       join_params
  #     )
  #     |> query_expression(assoc_schema_module, :where, filter_params, join_binding_alias)
  #   end)
  # end

  # def query_expression(query, schema_module, :join, {:association, params}, current_binding) do
  #   Enum.reduce(params, query, fn {schema_field, value}, query ->
  #     query_expression(
  #       query,
  #       schema_module,
  #       :join,
  #       {:association, {schema_field, value}},
  #       current_binding
  #     )
  #   end)
  # end

  # def query_expression(query, schema_module, :join, params, current_binding) do
  #   Enum.reduce(params, query, fn {schema_field, value}, query ->
  #     query_expression(query, schema_module, :join, {schema_field, value}, current_binding)
  #   end)
  # end

  # def query_expression(query, _schema_module, :select, true, current_binding) do
  #   QueryApi.select(query, current_binding, true)
  # end

  # def query_expression(query, _schema_module, :select, {type, fields}, current_binding) do
  #   QueryApi.select(query, current_binding, type, fields)
  # end

  # def query_expression(query, schema_module, :select, params, current_binding)
  #     when is_map(params) do
  #   Enum.reduce(params, query, fn {type, fields}, query ->
  #     query_expression(query, schema_module, :select, {type, fields}, current_binding)
  #   end)
  # end

  # def query_expression(query, schema_module, :select, values, current_binding)
  #     when is_list(values) do
  #   if Keyword.keyword?(values) do
  #     Enum.reduce(values, query, fn value, query ->
  #       query_expression(query, schema_module, :select, value, current_binding)
  #     end)
  #   else
  #     QueryApi.select(query, current_binding, values)
  #   end
  # end

  # # select_merge

  # def query_expression(query, _schema_module, :select_merge, true, current_binding) do
  #   QueryApi.select_merge(query, current_binding, true)
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :select_merge,
  #       {schema_field, value},
  #       current_binding
  #     ) do
  #   with_query_field(query, schema_module, schema_field, fn query ->
  #     QueryApi.select_merge(query, current_binding, schema_field, value)
  #   end)
  # end

  # def query_expression(query, schema_module, :select_merge, params, current_binding)
  #     when is_map(params) do
  #   Enum.reduce(params, query, fn {key, value}, query ->
  #     query_expression(query, schema_module, :select_merge, {key, value}, current_binding)
  #   end)
  # end

  # def query_expression(query, schema_module, :select_merge, values, current_binding)
  #     when is_list(values) do
  #   if Keyword.keyword?(values) do
  #     Enum.reduce(values, query, fn {key, value}, query ->
  #       query_expression(query, schema_module, :select_merge, {key, value}, current_binding)
  #     end)
  #   else
  #     case values do
  #       [params | _] when is_list(params) or is_map(params) ->
  #         Enum.reduce(values, query, fn value, query ->
  #           query_expression(query, schema_module, :select_merge, value, current_binding)
  #         end)

  #       values ->
  #         QueryApi.select_merge(query, current_binding, values)
  #     end
  #   end
  # end

  # # or_where

  # def query_expression(
  #       query,
  #       schema_module,
  #       :or_where,
  #       {schema_field, {operator, value}},
  #       current_binding
  #     ) do
  #   with_query_field(query, schema_module, schema_field, fn query ->
  #     schema_field = field_source(schema_module, schema_field)

  #     QueryApi.where(query, current_binding, schema_field, operator, value)
  #   end)
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :or_where,
  #       {schema_field, values},
  #       current_binding
  #     )
  #     when is_list(values) do
  #   if Keyword.keyword?(values) do
  #     Enum.reduce(values, query, fn {operator, value}, query ->
  #       query_expression(
  #         query,
  #         schema_module,
  #         :or_where,
  #         {schema_field, {operator, value}},
  #         current_binding
  #       )
  #     end)
  #   else
  #     with_query_field(query, schema_module, schema_field, fn query ->
  #       QueryApi.where(query, current_binding, schema_field, :==, values)
  #     end)
  #   end
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :or_where,
  #       {schema_field, params},
  #       current_binding
  #     )
  #     when is_map(params) do
  #   Enum.reduce(params, query, fn {operator, value}, query ->
  #     query_expression(
  #       query,
  #       schema_module,
  #       :or_where,
  #       {schema_field, {operator, value}},
  #       current_binding
  #     )
  #   end)
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :or_where,
  #       {schema_field, value},
  #       current_binding
  #     ) do
  #   with_query_field(query, schema_module, schema_field, fn query ->
  #     schema_field = field_source(schema_module, schema_field)

  #     if field_type_of_array?(schema_module, schema_field) do
  #       QueryApi.where(query, current_binding, value, :in, schema_field)
  #     else
  #       QueryApi.where(query, current_binding, schema_field, :==, value)
  #     end
  #   end)
  # end

  # def query_expression(query, schema_module, :or_where, params, current_binding) do
  #   Enum.reduce(params, query, fn {schema_field, value}, query ->
  #     query_expression(
  #       query,
  #       schema_module,
  #       :or_where,
  #       {schema_field, value},
  #       current_binding
  #     )
  #   end)
  # end

  # # where

  # def query_expression(
  #       query,
  #       schema_module,
  #       :where,
  #       {schema_field, {op, values}},
  #       current_binding
  #     ) when op in [:not, :!=] and is_list(values) do
  #   if Keyword.keyword?(values) do
  #     Enum.reduce(values, query, fn {key, value}, query ->
  #       query_expression(
  #         query,
  #         schema_module,
  #         :where,
  #         {schema_field, {{:!=, key}, value}},
  #         current_binding
  #       )
  #     end)
  #   else
  #     with_query_field(query, schema_module, schema_field, fn query ->
  #       schema_field = field_source(schema_module, schema_field)

  #       if field_type_of_array?(schema_module, schema_field) do
  #         QueryApi.where(query, current_binding, schema_field, :!=, values)
  #       else
  #         QueryApi.where(query, current_binding, schema_field, {:not, :in}, values)
  #       end
  #     end)
  #   end
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :where,
  #       {schema_field, {op, value}},
  #       current_binding
  #     ) when op in [:not, :!=] do
  #   with_query_field(query, schema_module, schema_field, fn query ->
  #     schema_field = field_source(schema_module, schema_field)

  #     if field_type_of_array?(schema_module, schema_field) do
  #       QueryApi.where(query, current_binding, value, {:!=, :in}, schema_field)
  #     else
  #       QueryApi.where(query, current_binding, schema_field, :!=, value)
  #     end
  #   end)
  # end

  # def query_expression(query, schema_module, :where, {schema_field, {op, params}}, current_binding) when is_map(params) do
  #   query_expression(query, schema_module, :where, {schema_field, {op, Map.to_list(params)}}, current_binding)
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :where,
  #       {schema_field, {operator, value}},
  #       current_binding
  #     ) do
  #   with_query_field(query, schema_module, schema_field, fn query ->
  #     schema_field = field_source(schema_module, schema_field)

  #     QueryApi.where(query, current_binding, schema_field, operator, value)
  #   end)
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :where,
  #       {schema_field, params},
  #       current_binding
  #     )
  #     when is_map(params) do
  #   Enum.reduce(params, query, fn {operator, value}, query ->
  #     query_expression(
  #       query,
  #       schema_module,
  #       :where,
  #       {schema_field, {operator, value}},
  #       current_binding
  #     )
  #   end)
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :where,
  #       {schema_field, value},
  #       current_binding
  #     )
  #     when is_list(value) do
  #   if Keyword.keyword?(value) do
  #     Enum.reduce(value, query, fn {operator, value}, query ->
  #       query_expression(
  #         query,
  #         schema_module,
  #         :where,
  #         {schema_field, {operator, value}},
  #         current_binding
  #       )
  #     end)
  #   else
  #     with_query_field(query, schema_module, schema_field, fn query ->
  #       schema_field = field_source(schema_module, schema_field)

  #       if field_type_of_array?(schema_module, schema_field) do
  #         QueryApi.where(query, current_binding, schema_field, :==, value)
  #       else
  #         QueryApi.where(query, current_binding, schema_field, :in, value)
  #       end
  #     end)
  #   end
  # end

  # def query_expression(
  #       query,
  #       schema_module,
  #       :where,
  #       {schema_field, value},
  #       current_binding
  #     ) do
  #   with_query_field(query, schema_module, schema_field, fn query ->
  #     schema_field = field_source(schema_module, schema_field)

  #     if field_type_of_array?(schema_module, schema_field) do
  #       QueryApi.where(query, current_binding, value, :in, schema_field)
  #     else
  #       QueryApi.where(query, current_binding, schema_field, :==, value)
  #     end
  #   end)
  # end

  # def query_expression(query, schema_module, :where, params, current_binding) do
  #   Enum.reduce(params, query, fn {schema_field, value}, query ->
  #     query_expression(query, schema_module, :where, {schema_field, value}, current_binding)
  #   end)
  # end

  # def query_expression(query, schema_module, schema_field, value, current_binding) do
  #   query_expression(query, schema_module, :where, {schema_field, value}, current_binding)
  # end

  # defp with_query_field(query, schema_module, schema_field, fun) do
  #   if query_field?(schema_module, schema_field) do
  #     fun.(query)
  #   else
  #     EctoShorts.Utils.Logger.warning(
  #       __MODULE__,
  #       "expected key to be a query field for the schema #{inspect(schema_module)}, got: #{inspect(schema_field)}"
  #     )

  #     query
  #   end
  # end

  # defp with_association(query, schema_module, schema_field, fun) do
  #   if association?(schema_module, schema_field) do
  #     assoc_schema_module = ecto_association_schema!(schema_module, schema_field)

  #     fun.(query, assoc_schema_module)
  #   else
  #     EctoShorts.Utils.Logger.warning(
  #       __MODULE__,
  #       "expected key to be an association for the schema #{inspect(schema_module)}, got: #{inspect(schema_field)}"
  #     )

  #     query
  #   end
  # end
end
