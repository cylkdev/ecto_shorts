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

  @type acc :: any()
  @type input :: any()
  @type callback :: (any(), any() -> any())
  @type opts :: keyword()

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

      # Collect all key-value pairs into a list.

      iex> EctoShorts.ExpressionBuilder.apply_expressions(
      ...>   [],
      ...>   %{post: %{title: "hello_world"}, age: 30},
      ...>   fn pair, acc -> [pair | acc] end
      ...> )
      [
        {:post, {:title, "hello_world"}},
        {:age, 30}
      ]

      # Use it to build up a expression.

      iex> import Ecto.Query
      ...> EctoShorts.ExpressionBuilder.apply_expressions(
      ...>    EctoShorts.Schema.Post,
      ...>    %{id: 1, title: "hello_world"},
      ...>    fn {key, val}, query ->
      ...>      from p in query, where: field(p, ^key) == ^val
      ...>    end
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.title == ^"hello_world", where: p0.id == ^1>

      # Structs are preserved as-is.

      iex> EctoShorts.ExpressionBuilder.apply_expressions(
      ...>   [],
      ...>   %{post: %EctoShorts.Schema.Post{id: 1}},
      ...>   fn pair, acc -> [pair | acc] end
      ...> )
      [
        {:post, %EctoShorts.Schema.Post{id: 1}}
      ]

      # Keyword lists are flattened.

      iex> EctoShorts.ExpressionBuilder.apply_expressions(
      ...>   [],
      ...>   [title: "hello", author: "admin"],
      ...>   fn pair, acc -> [pair | acc] end
      ...> )
      [
        {:author, "admin"},
        {:title, "hello"}
      ]

      # Non-keyword lists are preserved.

      iex> EctoShorts.ExpressionBuilder.apply_expressions(
      ...>   [],
      ...>   %{tags: ["elixir", "ecto"]},
      ...>   fn pair, acc -> [pair | acc] end
      ...> )
      [
        {:tags, ["elixir", "ecto"]}
      ]
  """
  @spec apply_expressions(acc(), input(), callback()) :: acc()
  @spec apply_expressions(acc(), input(), callback(), opts()) :: acc()
  def apply_expressions(acc, input, fun, opts \\ []) when is_function(fun, 2) do
    input
    |> flatten_input(opts)
    |> do_apply(acc, fun)
  end

  defp do_apply([], acc, _fun) do
    acc
  end

  defp do_apply([head | todo], acc, fun) do
    with acc <- fun.(head, acc) do
      do_apply(todo, acc, fun)
    end
  end

  defp flatten_input(input, opts) do
    with acc <- do_flatten(input, []) do
      if ordered_expressions?(opts) do
        Enum.reverse(acc)
      else
        acc
      end
    end
  end

  # stop expansion
  defp do_flatten([], acc) do
    acc
  end

  # flatten keyword lists
  defp do_flatten([{_, _} = head | tail] = _kwd, acc) do
    do_flatten(tail, do_flatten(head, acc))
  end

  # flatten single tuples
  defp do_flatten({key, %_{} = struct}, acc) do
    [{key, struct} | acc]
  end

  defp do_flatten({key, map}, acc) when is_map(map) do
    do_flatten({key, Map.to_list(map)}, acc)
  end

  # stop expansion
  defp do_flatten({_key, []}, acc) do
    acc
  end

  # flatten list of keyword tuples
  defp do_flatten({key, [{_, _} = head | tail]}, acc) do
    with acc <- do_flatten({key, head}, acc) do
      do_flatten({key, tail}, acc)
    end
  end

  # return any other value as-is
  defp do_flatten({key, val}, acc) do
    [{key, val} | acc]
  end

  # flatten top-level maps
  defp do_flatten(map, acc) when is_map(map) do
    map
    |> Map.to_list()
    |> do_flatten(acc)
  end

  # for all other types include them as-is
  defp do_flatten(val, acc) do
    [val | acc]
  end

  defp ordered_expressions?(opts) do
    opts[:ordered_expressions] || @ordered_expressions
  end
end
