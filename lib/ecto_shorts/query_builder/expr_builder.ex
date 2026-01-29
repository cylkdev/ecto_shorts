defmodule EctoShorts.QueryBuilder.ExprBuilder do
  @moduledoc false

  @doc """
  Normalize a term into a list of key-value pairs.

  ## Examples

    iex> EctoShorts.QueryBuilder.ExprBuilder.expand(%{title: "A"})
    [{:title, "A"}]
  """
  def expand(term) do
    term
    |> do_normalize([])
    |> Enum.reverse()
  end

  defp do_normalize(term, acc) when is_map(term) and not is_struct(term) do
    term |> Map.to_list() |> do_normalize(acc)
  end

  defp do_normalize([], acc), do: acc

  defp do_normalize(list, acc) when is_list(list) do
    if flatten?(list) do
      Enum.reduce(list, acc, fn entry, acc_inner ->
        do_normalize(entry, acc_inner)
      end)
    else
      [list | acc]
    end
  end

  defp do_normalize({k, v}, acc) when is_map(v) and not is_struct(v) do
    do_normalize({k, Map.to_list(v)}, acc)
  end

  defp do_normalize({k, v}, acc) when is_list(v) do
    if flatten?(v) do
      v
      |> expand()
      |> Enum.map(&{k, &1})
      |> do_normalize(acc)
    else
      [{k, v} | acc]
    end
  end

  defp do_normalize(v, acc) do
    [v | acc]
  end

  defp flatten?(list) do
    Keyword.keyword?(list) or Enum.any?(list, &is_map/1)
  end
end
