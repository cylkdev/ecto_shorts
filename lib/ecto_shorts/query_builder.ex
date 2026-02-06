defmodule EctoShorts.QueryBuilder do
  @moduledoc """
  Builds Ecto queries by applying query builder stages.

  This module is the public entrypoint for applying a single query-building
  step (filter) to an `Ecto.Query`.

  Database-specific behavior is handled at expression compilation time (see
  `EctoShorts.QueryBuilder.Dynamics`).
  """

  alias EctoShorts.QueryBuilder.Stages.{Filters, Joins, Selects}

  @doc """
  Builds a query by applying the given filter and args.

  This function routes query-building requests to the appropriate stage:

    * `:join` uses `EctoShorts.QueryBuilder.Stages.Joins`
    * `:select` and `:select_merge` use `EctoShorts.QueryBuilder.Stages.Selects`
    * all other filters use `EctoShorts.QueryBuilder.Stages.Filters`
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
    case current_filter do
      :join ->
        Joins.build(source, query, binding_selector, args)

      filter when filter in [:select, :select_merge] ->
        Selects.build(filter, source, query, binding_selector, args)

      filter ->
        Filters.build(source, filter, query, binding_selector, args, opts)
    end
  end
end
