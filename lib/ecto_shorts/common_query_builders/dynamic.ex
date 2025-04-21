defmodule EctoShorts.CommonFilters.CommonQueryBuilders.Dynamic do
  alias Ecto.Query

  require Ecto.Query

  def where(dynamic_module, query, current_binding, key, context, value) do
    dyn_expr = dynamic_module.where(current_binding, key, context, value)

    Query.where(query, ^dyn_expr)
  end

  def traverse_filter_params(query, schema_module, key, params, current_binding, fun) do
    traverse_params(query, schema_module, key, params, current_binding, fun)
  end

  defp traverse_params(
         query,
         schema_module,
         key,
         {context, {:parent_as, {parent_binding, value}}},
         current_binding,
         fun
       ) do
    traverse_params(
      query,
      schema_module,
      key,
      {context, value},
      {current_binding, parent_binding},
      fun
    )
  end

  defp traverse_params(
         query,
         schema_module,
         key,
         {context, {:parent_as, params}},
         current_binding,
         fun
       )
       when is_list(params) or is_map(params) do
    Enum.reduce(params, query, fn {parent_binding, value}, query ->
      traverse_params(
        query,
        schema_module,
        key,
        {context, {:parent_as, {parent_binding, value}}},
        current_binding,
        fun
      )
    end)
  end

  defp traverse_params(
         query,
         schema_module,
         key,
         {context, values},
         current_binding,
         fun
       )
       when is_list(values) do
    if Keyword.keyword?(values) do
      Enum.reduce(values, query, fn {filter, value}, query ->
        traverse_params(
          query,
          schema_module,
          key,
          {context, {filter, value}},
          current_binding,
          fun
        )
      end)
    else
      fun.(query, current_binding, schema_module, key, context, values)
    end
  end

  defp traverse_params(
         query,
         schema_module,
         key,
         {context, params},
         current_binding,
         fun
       )
       when is_map(params) do
    Enum.reduce(params, query, fn {filter, value}, query ->
      traverse_params(
        query,
        schema_module,
        key,
        {context, {filter, value}},
        current_binding,
        fun
      )
    end)
  end

  defp traverse_params(
         query,
         schema_module,
         key,
         {:parent_as, value},
         current_binding,
         fun
       ) do
    traverse_params(
      query,
      schema_module,
      key,
      {:==, {:parent_as, value}},
      current_binding,
      fun
    )
  end

  defp traverse_params(
         query,
         schema_module,
         key,
         {context, value},
         current_binding,
         fun
       ) do
    fun.(query, current_binding, schema_module, key, context, value)
  end

  defp traverse_params(query, schema_module, key, values, current_binding, fun)
       when is_list(values) do
    if Keyword.keyword?(values) do
      Enum.reduce(values, query, fn {context, value}, query ->
        traverse_params(query, schema_module, key, {context, value}, current_binding, fun)
      end)
    else
      fun.(query, current_binding, schema_module, key, :==, values)
    end
  end

  defp traverse_params(query, schema_module, key, params, current_binding, fun)
       when is_map(params) do
    Enum.reduce(params, query, fn {context, value}, query ->
      traverse_params(query, schema_module, key, {context, value}, current_binding, fun)
    end)
  end

  defp traverse_params(query, schema_module, key, value, current_binding, fun) do
    fun.(query, current_binding, schema_module, key, :==, value)
  end
end
