defmodule EctoShorts.QueryBuilder do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Specifies the query builder API required from adapters.
  """

  @type t :: module()

  @type key :: atom()
  @type value :: any()
  @type source :: binary()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}

  @default_adapter EctoShorts.CommonFilters

  @doc """
  ...
  """
  @callback build_query(
              query :: query() | queryable() | source_queryable(),
              current_binding :: atom() | nil,
              schema_module :: module(),
              key :: atom(),
              value :: any()
            ) :: query() | queryable()

  @doc """
  ...
  """
  @spec build_query(
          query :: query() | queryable() | source_queryable(),
          current_binding :: atom() | nil,
          schema_module :: module(),
          key :: atom(),
          value :: any(),
          opts :: keyword()
        ) :: query() | queryable()
  def build_query(query, current_binding, schema_module, key, value, opts) do
    adapter(opts).build_query(query, current_binding, schema_module, key, value)
  end

  defp adapter(opts) do
    opts[:query_builder_adapter] || @default_adapter
  end
end
