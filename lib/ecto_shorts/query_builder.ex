defmodule EctoShorts.QueryBuilder do
  @moduledoc false

  @callback build_query(
              source :: any(),
              query :: any(),
              binding_selector :: {:at, pos_integer()} | {:as, atom()},
              current_filter :: any(),
              args :: any(),
              opts :: keyword()
            ) :: Ecto.Query.t()
end
