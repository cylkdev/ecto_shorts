defmodule EctoShorts.QueryBuilder do
  @moduledoc false

  alias EctoShorts.QueryBuilder.Postgres

  @callback build_query(
              filter :: any(),
              schema :: any(),
              query :: any(),
              binding_selector :: {:at, pos_integer()} | {:as, atom()},
              term :: any(),
              opts :: keyword()
            ) :: Ecto.Query.t()

  def build_query(
        schema,
        filter,
        query,
        binding_selector,
        join_params,
        opts \\ []
      ) do
    adapter(opts).build_query(schema, filter, query, binding_selector, join_params, opts)
  end

  defp adapter(opts) do
    opts[:query_builder] || Postgres
  end
end
