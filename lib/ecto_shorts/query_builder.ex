defmodule EctoShorts.QueryBuilder do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Specifies the query builder API required from adapters.
  """

  @type t :: module()

  @type source :: binary()

  @type query :: Ecto.Query.t()

  @type queryable :: Ecto.Queryable.t()

  @type source_queryable :: {source(), queryable()}

  @type binding :: atom()

  @type key :: atom()

  @type value :: any()

  @type opts :: keyword()

  @default_adapter EctoShorts.CommonFilters

  @doc """
  ...
  """
  @callback build_query(
              query() | queryable() | source_queryable(),
              binding() | nil,
              queryable(),
              key(),
              value()
            ) :: query() | queryable()

  @doc """
  ...
  """
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding() | nil,
          queryable(),
          key(),
          value()
        ) :: query() | queryable()
  @spec build_query(
          query() | queryable() | source_queryable(),
          binding() | nil,
          queryable(),
          key(),
          value(),
          opts()
        ) :: query() | queryable()
  def build_query(query, current_binding, schema_module, key, value, opts \\ []) do
    adapter(opts).build_query(query, current_binding, schema_module, key, value)
  end

  @doc """
  Recursively applies a function to each element or
  key-value pair in the input, traversing lists and maps.

  The function is called as `fun.(query, value)` or
  `fun.(query, {key, value})` depending on the input shape.

  ## Deep Nesting and Key Expansion

  When the input is a nested map or keyword list, `apply_expressions/3` will
  recursively expand the structure, passing deeply nested key-value pairs as tuples.
  For example, given a nested map like `%{a: %{b: %{c: 1}}}`, the function will ultimately call
  `fun.(query, {:a, {:b, {:c, 1}}})`. This expansion pattern makes it easy to work with
  arbitrarily nested data structures, as each level of nesting is preserved in the
  tuple structure passed to your function.

  This approach is especially useful for building queries from complex filter
  or select parameters, where you want to preserve the path of keys leading to a value.

  ## Example

  ```elixir

      params = %{user: %{profile: %{age: 30}}}

      EctoShorts.QueryBuilder.QueryBuilder.apply_expressions(query, params, fn query, {key_path, value} ->
        # ...
      end)

  ```
  """
  @spec apply_expressions(
          term :: any(),
          value :: any(),
          fun :: (term :: any(), value :: any() -> any())
        ) :: any()
  def apply_expressions(term, {key, values}, fun) when is_list(values) do
    if Keyword.keyword?(values) or has_params?(values) do
      Enum.reduce(values, term, fn value, term ->
        apply_expressions(term, value, fun)
      end)
    else
      fun.(term, {key, values})
    end
  end

  def apply_expressions(term, {key, params}, fun) when is_map(params) do
    Enum.reduce(params, term, fn value, term ->
      apply_expressions(term, {key, value}, fun)
    end)
  end

  def apply_expressions(term, {key, value}, fun) do
    fun.(term, {key, value})
  end

  def apply_expressions(term, values, fun) when is_list(values) do
    if Keyword.keyword?(values) or has_params?(values) do
      Enum.reduce(values, term, fn value, term ->
        apply_expressions(term, value, fun)
      end)
    else
      fun.(term, values)
    end
  end

  def apply_expressions(term, params, fun) when is_map(params) do
    Enum.reduce(params, term, fn {key, value}, term ->
      apply_expressions(term, {key, value}, fun)
    end)
  end

  def apply_expressions(term, value, fun) do
    fun.(term, value)
  end

  defp has_params?([head | _]) when is_list(head) or is_map(head), do: true
  defp has_params?(_), do: false

  defp adapter(opts) do
    opts[:query_builder_adapter] || @default_adapter
  end
end
