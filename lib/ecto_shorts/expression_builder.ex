defmodule EctoShorts.ExpressionBuilder do
  @moduledoc since: "2.5.0"
  @moduledoc """
  A utility module for transforming nested maps and keyword lists
  into flat structures that are easier to work with in dynamic query
  composition.

  This module is typically used when you need to walk over nested
  filter parameters and convert them into a list of expressions that
  can be processed sequentially—such as building dynamic `where`
  clauses or extracting paths in deeply nested input data.

  The primary entry point is `apply_expression/3`, which walks a
  nested input and applies a function to each flattened key-value
  pair in a consistent and predictable order.

  ## Example Use Case

  Suppose you want to process this filter input:

      %{a: %{b: 1}, c: 2, d: [4, 5, 6], e: %{f: %{g: 7}}}

  You can call:

      EctoShorts.ExpressionBuilder.apply_expressions([], input, fn pair, acc ->
        [pair | acc]
      end)

  And receive:

      [
        {:e, {:f, {:g, 7}}},
        {:d, [4, 5, 6]},
        {:a, {:b, 1}},
        {:c, 2}
      ]

  This flattening makes it easy to iterate and transform the data
  into query filters or other expressions.
  """

  @doc """
  Applies a function to each flattened key-value expression from the
  input value.

  The `expr` input can be any term. This function will recursively
  flatten it deeply nested maps or keyword lists and apply the given
  function to each pair.

  The order of traversal ensures nested values are grouped by their
  top-level keys, preserving enough structure to retain context
  (e.g., `{:a, {:b, 1}}`).

  This function accepts:

    * A starting accumulator
    * Some input (like a map, keyword list or integer)
    * A function that is called for each item

  ## Examples

      iex> EctoShorts.ExpressionBuilder.apply_expressions(
      ...>   [],
      ...>   %{a: %{b: 1}, c: 2, d: [4, 5, 6], e: %{f: %{g: 7}}},
      ...>   fn pair, acc -> [pair | acc] end
      ...> )
      [
        {:e, {:f, {:g, 7}}},
        {:d, [4, 5, 6]},
        {:a, {:b, 1}},
        {:c, 2}
      ]
  """
  @spec apply_expressions(
          acc :: any(),
          value :: any(),
          fun :: (acc :: any(), value :: any() -> any())
        ) :: any()
  def apply_expressions(acc, value, fun) do
    value
    |> flatten_params()
    |> Enum.reduce(acc, &fun.(&1, &2))
  end

  defp flatten_params({key, [{_, _} | _] = list}) do
    list
    |> flatten_params()
    |> Enum.map(&{key, &1})
  end

  defp flatten_params({key, params}) when is_map(params) do
    flatten_params({key, Map.to_list(params)})
  end

  defp flatten_params([head | tail]) do
    flatten_params(head) ++ flatten_params(tail)
  end

  defp flatten_params(params) when is_map(params) do
    params
    |> Map.to_list()
    |> flatten_params()
  end

  defp flatten_params(value) do
    List.wrap(value)
  end
end
