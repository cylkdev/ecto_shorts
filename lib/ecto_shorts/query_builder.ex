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
end
