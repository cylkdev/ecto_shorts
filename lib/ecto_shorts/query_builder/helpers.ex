defmodule EctoShorts.QueryBuilder.Helpers do
  @moduledoc since: "2.5.0"
  @moduledoc """
  # EctoShorts.QueryBuilder.Helpers

  Utility functions for recursively applying query expressions to Ecto queries.

  The primary function, `apply_expressions/3`, is used to traverse and apply a
  changes to a given term.

  ## Functionality

  `apply_expressions/3` recursively traverses lists, maps, and key-value tuples,
  applying the given function to each value or pair. This enables flexible,
  composable query construction from arbitrarily nested parameter structures,
  such as those found in filters, select fields, or association preloads.

  ## Usage

  This helper is most commonly used in modules like `EctoShorts.QueryBuilder.Common`,
  `EctoShorts.QueryBuilder.Schema`, and `EctoShorts.QueryBuilder.QueryExpressions` to
  reduce code duplication when building up Ecto queries from dynamic, user-provided
  parameters.

  ### Example

      params = %{status: "active", roles: ["admin", "user"]}

      query = MySchema

      EctoShorts.QueryBuilder.Helpers.apply_expressions(query, params, fn query, {key, value} ->
        # Apply a filter, select, or other transformation
        MyModule.filter(query, key, value)
      end)

  This will apply the function to each key-value pair, handling nested lists or maps as needed.

  The function is also used internally by higher-level query builders to support flexible query DSLs.
  """

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

      params = %{user: %{profile: %{age: 30}}}
      apply_expressions(query, params, fn query, {key_path, value} ->
        # key_path will be {:user, {:profile, {:age, 30}}}
        # value will be {:user, {:profile, {:age, 30}}} at the leaf
        ...
      end)

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
end
