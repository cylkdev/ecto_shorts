defmodule EctoShorts.Utils do
  @moduledoc false

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
  @spec reduce_all(enum :: Enum.t(), fun :: function(), value_acc :: list(), error_acc :: list()) ::
          {:ok, list()} | {:error, list()}
  def reduce_all(enum, fun, value_acc \\ [], error_acc \\ []) do
    case Enum.reduce(enum, {value_acc, error_acc}, &reduce_eval(&1, fun, &2)) do
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
