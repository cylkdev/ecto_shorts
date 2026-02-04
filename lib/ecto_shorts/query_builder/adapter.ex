defmodule EctoShorts.QueryBuilder.Adapter do
  @moduledoc """
  Defines the contract for query builder adapters.

  An adapter receives the current query state and applies one filter step.
  """

  @doc """
  Builds a query by applying the given filter and args.
  """
  @callback build_query(
              source :: any(),
              query :: Ecto.Query.t(),
              binding_selector :: {:at, pos_integer()} | {:as, atom() | nil},
              current_filter :: any(),
              args :: any(),
              opts :: keyword()
            ) :: Ecto.Query.t()
end
