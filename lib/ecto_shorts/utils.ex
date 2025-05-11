defmodule EctoShorts.Utils do
  @moduledoc false

  @type schema_data :: Ecto.Schema.t()

  @doc """
  Converts an Ecto struct into a plain map that’s safe to encode as JSON.

  Removes internal Ecto metadata fields (`__meta__`, `__schema__`) and
  any associations defined on the schema. This results in a flat map
  containing only a subset of the struct's fields.

  ## Example

      iex> to_jsonable_map(%User{
      ...>   id: 1,
      ...>   name: "Fira",
      ...>   __meta__: #Ecto.Schema.Metadata<>,
      ...>   posts: [%EctoShorts.Schema.Post{title: "First"}]
      ...> })
      %{id: 1, name: "Fira"}
  """
  @spec to_jsonable_map(schema_data()) :: map()
  def to_jsonable_map(%{__meta__: %{schema: queryable}} = struct) do
    struct
    |> Map.from_struct()
    |> Map.drop([:__schema__, :__meta__])
    |> Map.drop(queryable.__schema__(:associations))
  end

  @doc """
  Reduces an enumerable by applying a function that returns `{:ok, value}` or
  `{:error, reason}` to each item, accumulating successes and errors separately.

  If all elements return `{:ok, value}`, returns `{:ok, list_of_values}`.
  If any element returns `{:error, term}`, returns `{:error, list_of_terms}`.

  You can set the initial accumulator for successful values by `value_acc` or
  the initial accumulator for errors by `error_acc`.

  ### Examples

      iex> SharedUtils.Enum.reduce_all([1], fn v -> {:ok, v} end)
      {:ok, [1]}

      iex> SharedUtils.Enum.reduce_all(["error"], fn v -> {:error, v} end)
      {:error, ["error"]}
  """
  @spec reduce_all(enum :: Enum.t(), fun :: function()) :: {:ok, list()} | {:error, list()}
  def reduce_all(enum, fun) do
    case Enum.reduce(enum, {[], []}, &reduce_eval(&1, fun, &2)) do
      {values, []} -> {:ok, Enum.reverse(values)}
      {_, errors} -> {:error, Enum.reverse(errors)}
    end
  end

  defp reduce_eval(term, fun, {values, errors}) do
    case fun.(term) do
      {:error, e} -> {values, [e | errors]}
      {:ok, v} -> {[v | values], errors}
      term -> raise "expected {:ok, term()} or {:error, term()}, got: #{inspect(term)}"
    end
  end

  def underscore_last_module_alias(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
  end
end
