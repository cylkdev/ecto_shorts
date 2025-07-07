defmodule EctoShorts.DynamicBuilder do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Defines a behavior and interface for building dynamic Ecto query
  expressions based on the adapter in use (e.g., Postgres).

  In Ecto, the `dynamic/2` macro is used to build flexible queries where
  parts of the expression depend on input at runtime . This module provides
  an API for building these expressions for different database backends.

  You can implement the `EctoShorts.DynamicBuilder` behavior in your own
  adapter module to customize how dynamic conditions are generated based
  on things like schema, field name, and value type.
  """

  @doc """
  Defines a callback to implement custom logic for building a dynamic query expression.

  It receives:
    - `dyn`: the current dynamic expression being built or `nil`.
    - `binding`: the binding index (e.g. 0 for the main schema)
    - `schema`: the module for the schema being queried
    - `key`: the field name (e.g. `:name`)
    - `value`: the filter value (e.g. a string or a special operator like `%{ilike: "foo"}`)

  Returns an updated dynamic expression.
  """
  @callback build_dynamic(any(), any(), any(), any(), any(), any()) :: any()

  @doc """
  Delegates to the given adapter module to build a dynamic query expression
  based on the provided schema, field, and value.

  This function acts as the main entry point for constructing `dynamic/2`
  expressions across different database backends. It relies on the adapter
  implementing the `EctoShorts.DynamicBuilder` behaviour and calling
  `create_dynamic/6` internally.

  The returned expression can be used in Ecto queries with `where/3`,
  `or_where/3`, or similar macros.

  ## Parameters

    * `adapter` – A module that implements the `EctoShorts.DynamicBuilder` behaviour.
    * `schema` – The Ecto schema module for the query.
    * `dyn` – The current dynamic expression (or `nil` if starting a new one).
    * `binding` – The alias or index representing the query binding (e.g. `:post` or `nil`).
    * `condition` – Logical operator (`:and` or `:or`) to merge expressions.
    * `key` – The schema field to filter on.
    * `value` – The value or `{operator, value}` tuple to filter by.

  ## Examples

      iex> EctoShorts.DynamicBuilder.build_dynamic(
      ...>   EctoShorts.DynamicBuilders.Postgres,
      ...>   nil,
      ...>   :post,
      ...>   :and,
      ...>   EctoShorts.Schemas.Post,
      ...>   :title,
      ...>   "example"
      ...> )

      iex> EctoShorts.DynamicBuilder.build_dynamic(
      ...>   EctoShorts.DynamicBuilders.Postgres,
      ...>   nil,
      ...>   :post,
      ...>   :and,
      ...>   EctoShorts.Schemas.Post,
      ...>   :title,
      ...>   {:ilike, "example"}
      ...> )

  """
  def build_dynamic(
        adapter,
        source,
        dyn,
        binding,
        condition,
        key,
        value
      ) do
    adapter.build_dynamic(
      source,
      dyn,
      binding,
      condition,
      key,
      value
    )
  end
end
