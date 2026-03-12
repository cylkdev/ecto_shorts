defmodule EctoShorts.QueryBuilder do
  @moduledoc false
  @callback build_query(
              filter :: atom(),
              source :: term(),
              query :: Ecto.Query.t(),
              selected_binding :: {:as, atom()} | {:at, pos_integer()},
              term :: term(),
              opts :: keyword()
            ) :: Ecto.Query.t()

  def build_query(module, filter, source, query, selected_binding, term, opts) do
    module.build_query(filter, source, query, selected_binding, term, opts)
  end
end
