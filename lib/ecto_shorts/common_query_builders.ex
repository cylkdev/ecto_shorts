defmodule EctoShorts.CommonFilters.CommonQueryBuilders do
  alias EctoShorts.CommonSchemas

  alias EctoShorts.CommonFilters.CommonQueryBuilders.{
    API,
    Dynamic,
    Helpers
  }

  def traverse_filter_params(query, schema_module, key, params, current_binding, fun) do
    Dynamic.traverse_filter_params(query, schema_module, key, params, current_binding, fun)
  end

  def where_array(query, current_binding, _schema_module, key, operator, value) do
    dynamic_where(Dynamic.Array, query, current_binding, key, operator, value)
  end

  def where_schema(query, current_binding, _schema_module, key, operator, value) do
    dynamic_where(Dynamic.Schema, query, current_binding, key, operator, value)
  end

  def dynamic_where(dynamic_module, query, current_binding, key, operator, value) do
    Dynamic.where(dynamic_module, query, current_binding, key, operator, value)
  end

  def join_association(query, current_binding, schema_module, key, params, fun) do
    assoc_schema_module = Helpers.ecto_association_schema(schema_module, key)

    {as, params} = Map.pop(params, :as)

    assoc_binding_alias = as || Helpers.named_binding_atom(key)

    query
    |> API.join_association({current_binding, assoc_binding_alias}, key, params)
    |> fun.(
      assoc_schema_module,
      Map.drop(params, [:on, :qualifier, :prefix]),
      assoc_binding_alias
    )
  end

  def join_subquery(query, current_binding, _schema_module, from, params, fun) do
    {as, params} = Map.pop(params, :as)

    subquery_schema_module = CommonSchemas.get_schema_queryable(from)

    subquery_binding_alias =
      with nil <- as do
        Helpers.module_to_named_binding_atom(subquery_schema_module)
      end

    query
    |> API.join_subquery({current_binding, subquery_binding_alias}, from, params)
    |> fun.(
      subquery_schema_module,
      Map.drop(params, [:on, :qualifier, :prefix]),
      subquery_binding_alias
    )
  end
end
