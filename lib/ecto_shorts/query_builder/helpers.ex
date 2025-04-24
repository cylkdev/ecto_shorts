defmodule EctoShorts.QueryBuilder.Helpers do
  @moduledoc since: "2.5.0"
  @moduledoc """
  ...
  """

  @doc """
  ...
  """
  def apply_expressions(query, {key, values}, fun) when is_list(values) do
    if Keyword.keyword?(values) or has_params?(values) do
      Enum.reduce(values, query, fn value, query ->
        apply_expressions(query, value, fun)
      end)
    else
      fun.(query, {key, values})
    end
  end

  def apply_expressions(query, {key, params}, fun) when is_map(params) do
    Enum.reduce(params, query, fn value, query ->
      apply_expressions(query, {key, value}, fun)
    end)
  end

  def apply_expressions(query, {key, value}, fun) do
    fun.(query, {key, value})
  end

  def apply_expressions(query, values, fun) when is_list(values) do
    if Keyword.keyword?(values) or has_params?(values) do
      Enum.reduce(values, query, fn value, query ->
        apply_expressions(query, value, fun)
      end)
    else
      fun.(query, values)
    end
  end

  def apply_expressions(query, params, fun) when is_map(params) do
    Enum.reduce(params, query, fn {key, value}, query ->
      apply_expressions(query, {key, value}, fun)
    end)
  end

  def apply_expressions(query, value, fun) do
    fun.(query, value)
  end

  defp has_params?([head | _]) when is_list(head) or is_map(head), do: true
  defp has_params?(_), do: false
end
