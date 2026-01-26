defmodule EctoShorts.QueryBuilder do
  @moduledoc false

  alias EctoShorts.QueryBuilder.Postgres

  @callback build_query(
              source :: any(),
              query :: any(),
              binding_selector :: {:at, pos_integer()} | {:as, atom()},
              current_filter :: any(),
              args :: any(),
              opts :: keyword()
            ) :: Ecto.Query.t()

  def build_query(source, query, binding_selector, current_filter, args, opts \\ []) do
    adapter(opts).build_query(source, query, binding_selector, current_filter, args, opts)
  end

  defp adapter(opts) do
    opts[:query_builder] || Postgres
  end
end
