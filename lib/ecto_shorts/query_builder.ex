defmodule EctoShorts.QueryBuilder do
  @moduledoc """
  Specifies the query builder API required from adapters.
  """
  @moduledoc since: "2.5.0"

  @type t :: module()

  @type key :: atom()
  @type value :: any()
  @type source :: binary()
  @type query :: Ecto.Query.t()
  @type queryable :: Ecto.Queryable.t()
  @type source_queryable :: {source(), queryable()}

  @doc """
  ...
  """
  @callback build_query(
              query :: query() | queryable() | source_queryable(),
              schema_module :: module(),
              key :: atom(),
              value :: any(),
              current_binding :: atom() | nil
            ) :: query() | queryable()

  @doc """
  ...
  """
  @spec build_query(
          query :: query() | queryable() | source_queryable(),
          schema_module :: module(),
          key :: atom(),
          value :: any(),
          current_binding :: atom() | nil
        ) :: query() | queryable()
  @spec build_query(
          query :: query() | queryable() | source_queryable(),
          schema_module :: module(),
          key :: atom(),
          value :: any(),
          current_binding :: atom() | nil,
          opts :: keyword()
        ) :: query() | queryable()
  def build_query(query, schema_module, key, value, current_binding, opts \\ []) do
    query_builder_adapter!(opts).build_query(query, schema_module, key, value, current_binding)
  end

  defp query_builder_adapter!(opts) do
    with nil <- opts[:query_builder_adapter] do
      raise KeyError, "key :query_builder_adapter not found in #{inspect(opts)}"
    end
  end
end
