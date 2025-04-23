defmodule EctoShorts.QueryBuilders do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """
  alias EctoShorts.QueryBuilders.{Common, Schema}

  def common_filter?(key), do: key in Common.filters()

  def schema_filter?(key), do: key in Schema.filters()

  def build_common_query(query, current_binding, schema_module, key, value) do
    Common.build_query(query, current_binding, schema_module, key, value)
  end

  def build_schema_query(query, current_binding, schema_module, key, value) do
    Schema.build_query(query, current_binding, schema_module, key, value)
  end
end
