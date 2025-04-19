defmodule EctoShorts.CommonFiltersRc.QueryExpressionBuilder do
  @moduledoc false

  def traverse_params(dyn \\ nil, condition \\ :and, current_binding \\ nil, params, fun)

  def traverse_params(dyn, _condition, current_binding, {operator, value}, fun)
      when operator in [:or, :or_where] do
    if Keyword.keyword?(value) do
      traverse_params(dyn, :or, current_binding, [value], fun)
    else
      traverse_params(dyn, :or, current_binding, List.wrap(value), fun)
    end
  end

  def traverse_params(
        dyn,
        condition,
        current_binding,
        {:parent_as, {parent_binding, {parent_key, {operator, value}}}},
        fun
      ) do
    fun.(
      dyn,
      condition,
      current_binding,
      {:parent_as, parent_binding, parent_key},
      operator,
      value
    )
  end

  def traverse_params(
        dyn,
        condition,
        current_binding,
        {:parent_as, {parent_binding, {parent_key, params}}},
        fun
      ) do
    Enum.reduce(params, dyn, fn {operator, value}, dyn ->
      traverse_params(
        dyn,
        condition,
        current_binding,
        {:parent_as, {parent_binding, {parent_key, {operator, value}}}},
        fun
      )
    end)
  end

  def traverse_params(
        dyn,
        condition,
        current_binding,
        {:parent_as, {parent_binding, params}},
        fun
      ) do
    Enum.reduce(params, dyn, fn {parent_key, params}, dyn ->
      traverse_params(
        dyn,
        condition,
        current_binding,
        {:parent_as, {parent_binding, {parent_key, params}}},
        fun
      )
    end)
  end

  def traverse_params(dyn, condition, current_binding, {:parent_as, params}, fun) do
    Enum.reduce(params, dyn, fn {parent_binding, params}, dyn ->
      traverse_params(
        dyn,
        condition,
        current_binding,
        {:parent_as, {parent_binding, params}},
        fun
      )
    end)
  end

  def traverse_params(dyn, condition, current_binding, {key, {operator, values}}, fun)
      when is_list(values) or is_map(values) do
    Enum.reduce(values, dyn, fn value, dyn ->
      traverse_params(dyn, condition, current_binding, {key, {operator, value}}, fun)
    end)
  end

  def traverse_params(dyn, condition, current_binding, {key, {operator, value}}, fun) do
    fun.(dyn, condition, current_binding, key, operator, value)
  end

  def traverse_params(dyn, condition, current_binding, {key, values}, fun)
      when is_list(values) or is_map(values) do
    Enum.reduce(values, dyn, fn value, dyn ->
      traverse_params(dyn, condition, current_binding, {key, value}, fun)
    end)
  end

  def traverse_params(dyn, condition, current_binding, {key, value}, fun) do
    fun.(dyn, condition, current_binding, key, :==, value)
  end

  def traverse_params(dyn, condition, current_binding, values, fun) do
    Enum.reduce(values, dyn, fn value, dyn ->
      traverse_params(dyn, condition, current_binding, value, fun)
    end)
  end
end
