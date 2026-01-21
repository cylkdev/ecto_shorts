defmodule EctoShorts.QueryBuilder do
  @callback build_query(
              filter :: any(),
              schema :: any(),
              query :: any(),
              binding_selector :: {:at, pos_integer()} | {:as, atom()},
              term :: any(),
              opts :: keyword()
            ) :: Ecto.Query.t()

  def build_query(adapter, filter, schema, query, binding_selector, term, opts) do
    adapter.build_query(filter, schema, query, binding_selector, term, opts)
  end
end
