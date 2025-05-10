defmodule EctoShorts.ExpressionBuilder do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Provides an API for traversing maps, keyword lists, and similar data
  structures and turning them into a flat list of key-value pairs.

  This module is typically used when you want to take a data structure
  like a set of parameters and work with each key-value pair
  individually.

  You can pass in a map, a keyword list, or any nested combination of
  them. The traversal goes through each layer, combining inner values
  with their parent keys so that the context isn't lost.

  For example, if you pass in this map:

      %{post: %{profile: %{age: 30}}, active: true}

  You’ll get a result like:

      [
        {:post, {:profile, {:age, 30}}},
        {:active, true}
      ]

  You can control whether the flattened results keep their original
  order. By default, the order is not preserved to improve performance,
  but you can enable it using the `:ordered_expressions` option.

  Structs are treated as whole values and are not flattened. This means
  you can safely include values like `%EctoShorts.Schema.Post{}` in your input without
  having their internal fields traversed or transformed.
  """

  alias EctoShorts.Config

  @ordered_expressions false

  @doc """
  Flattens the input data and applies a function to each key-value pair.

  This function traverses the entire structure of the given input, flattens
  it, and calls the given function for each result. You can use this to
  build up a value using an accumulator.

  Nested values are grouped under their top-level keys so you can retain
  context. For example:

      %{post: %{profile: %{age: 30}}}

  Will produce:

      [{:post, {:profile, {:age, 30}}}]

  ## Options

    * `:ordered_expressions` — if set to `true`, preserves the order of
      traversal. If `false` (default), order is not guaranteed but the
      operation is faster.

  ## Examples

  Collect all key-value pairs into a list:

      iex> EctoShorts.ExpressionBuilder.apply_expression(
      ...>   [],
      ...>   %{post: %{title: "hello_world"}, age: 30},
      ...>   fn pair, acc -> [pair | acc] end
      ...> )
      [
        {:post, {:title, "hello_world"}},
        {:age, 30}
      ]

  Use it to build up a dynamic expression:

      iex> import Ecto.Query
      ...> EctoShorts.ExpressionBuilder.apply_expression(
      ...>    EctoShorts.Schema.Post,
      ...>    %{id: 1, title: "hello_world"},
      ...>    fn {key, val}, query ->
      ...>      from p in query, where: field(p, ^key) == ^val
      ...>    end
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.title == ^"hello_world", where: p0.id == ^1>

  Structs are preserved as-is:

      iex> EctoShorts.ExpressionBuilder.apply_expression(
      ...>   [],
      ...>   %{post: %EctoShorts.Schema.Post{id: 1}},
      ...>   fn pair, acc -> [pair | acc] end
      ...> )
      [
        {:post, %EctoShorts.Schema.Post{id: 1}}
      ]
  """
  @spec apply_expression(
          any(),
          map() | keyword() | any(),
          (any(), any() -> any())
        ) :: any()
  @spec apply_expression(
          any(),
          map() | keyword() | any(),
          (any(), any() -> any()),
          keyword()
        ) :: any()
  def apply_expression(acc, data, fun, opts \\ []) do
    data
    |> flatten_params(opts)
    |> Enum.reduce(acc, &fun.(&1, &2))
  end

  defp flatten_params(data, opts) do
    with res <- do_flatten(data, []) do
      if ordered_expressions?(opts) do
        Enum.reverse(res)
      else
        res
      end
    end
  end

  defp do_flatten([], acc), do: acc

  defp do_flatten([head | tail], acc),
    do: do_flatten(tail, do_flatten(head, acc))

  defp do_flatten({key, %_{} = struct}, acc),
    do: [{key, struct} | acc]

  defp do_flatten({key, map}, acc) when is_map(map),
    do: map |> Map.to_list() |> Enum.reduce(acc, fn kv, acc -> do_flatten({key, kv}, acc) end)

  defp do_flatten({key, [{_, _} | _] = list}, acc),
    do: Enum.reduce(list, acc, fn item, acc -> do_flatten({key, item}, acc) end)

  defp do_flatten({key, val}, acc),
    do: [{key, val} | acc]

  defp do_flatten(map, acc) when is_map(map),
    do: map |> Map.to_list() |> do_flatten(acc)

  defp do_flatten(val, acc),
    do: [val | acc]

  defp ordered_expressions?(opts) do
    opts[:ordered_expressions] ||
      Config.ordered_expressions() ||
      @ordered_expressions
  end
end
