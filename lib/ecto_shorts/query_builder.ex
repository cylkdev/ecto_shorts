defmodule EctoShorts.QueryBuilder do
  @moduledoc """
  Builds Ecto queries by applying a query builder adapter.

  This module is the public entrypoint for query building. It delegates to an
  adapter module that implements `EctoShorts.QueryBuilder.Adapter`.
  """

  @default_adapter EctoShorts.QueryBuilder.Adapters.Postgres

  @doc """
  Builds a query by delegating to the configured query builder adapter.

  Pass the adapter in `opts[:query_builder]`. When absent, this uses the default
  adapter for this library.
  """
  @spec build_query(
          source :: any(),
          query :: Ecto.Query.t(),
          binding_selector :: {:at, pos_integer()} | {:as, atom() | nil},
          current_filter :: any(),
          args :: any(),
          opts :: keyword()
        ) :: Ecto.Query.t()
  def build_query(source, query, binding_selector, current_filter, args, opts \\ []) do
    adapter = Keyword.get(opts, :query_builder, @default_adapter)
    adapter.build_query(source, query, binding_selector, current_filter, args, opts)
  end
end
