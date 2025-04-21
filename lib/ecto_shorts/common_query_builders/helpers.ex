defmodule EctoShorts.CommonFilters.CommonQueryBuilders.Helpers do
  def named_binding_string(key) do
    "ecto_shorts_#{key}"
  end

  def named_binding_atom(key) do
    key
    |> named_binding_string()
    |> String.to_atom()
  end

  def module_to_named_binding_string(module) do
    module
    |> last_module_alias_underscored()
    |> named_binding_string()
  end

  def module_to_named_binding_atom(module) do
    module
    |> last_module_alias_underscored()
    |> named_binding_atom()
  end

  defp last_module_alias_underscored(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
  end

  def ecto_association_schema(schema_module, key) do
    case schema_module.__schema__(:association, key) do
      %_{through: [field1, field2]} ->
        schema_module
        |> ecto_association_schema(field1)
        |> ecto_association_schema(field2)

      %{related: related} ->
        related
    end
  end

  def field_source(schema_module, key) do
    schema_module.__schema__(:field_source, key) || key
  end

  def query_field?(schema_module, key) do
    key in schema_module.__schema__(:query_fields)
  end

  def association?(schema_module, key) do
    key in schema_module.__schema__(:associations)
  end

  def field_type_of_array?(schema_module, key) do
    case schema_module.__schema__(:type, key) do
      {:array, _} -> true
      _ -> false
    end
  end
end
