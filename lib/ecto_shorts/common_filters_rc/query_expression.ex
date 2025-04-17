defmodule EctoShorts.CommonFiltersRc.QueryExpression do
  @moduledoc since: "2.5.0"
  @moduledoc """
  `EctoShorts.CommonFiltersRc.QueryExpression`

  This module provides a standardized api for composing ecto query expressions.
  """
  alias Ecto.Query

  require Ecto.Query

  def named_binding_atom(key) do
    key |> named_binding_string() |> String.to_atom()
  end

  def named_binding_string(key), do: "ecto_shorts_#{key}"


  # Enum.reduce(params, Query.dynamic(true), fn {key, value}, dyn ->
  #   Query.dynamic(
  #     [{^join_binding, q}],
  #     ^dyn and field(join_binding, ^left_field) == field(parent_as(^alias_atom), ^right_field)
  #   )
  #   {key, Query.field(parent_as(^key), ^value)}

  #   # Ecto.Query.dynamic([{^current_binding, q}], field(q, ^key) == field(parent_as(^parent_binding), ^parent_field))
  # end)

  # def dynamic(dyn, filter, params, current_binding) when is_map(params) do
  #   Enum.reduce(params, dyn, fn {key, value}, dyn ->
  #     dynamic(dyn, filter, {key, value}, current_binding)
  #   end)
  # end

  # def dynamic(dyn, left_field, {:parent_as, params}, current_binding) when is_map(params) do
  #   Enum.reduce(params, dyn, fn {parent_as_binding, right_field}, dyn ->
  #     dynamic(dyn, left_field, {:parent_as, {parent_as_binding, right_field}}, current_binding)
  #   end)
  # end

  # def dynamic(dyn, left_field, {:parent_as, {parent_as_binding, right_field}}, current_binding) do
  #   new_dyn = Query.dynamic([{^current_binding, q}], field(q, ^left_field) == field(parent_as(^parent_as_binding), ^right_field))

  #   merge_dynamic(dyn, new_dyn)
  # end

  # def dynamic(dyn, left_field, {:self, params}, current_binding) when is_map(params) do
  #   Enum.reduce(params, dyn, fn {key, value}, dyn ->
  #     dynamic(dyn, left_field, {:self, {key, value}}, current_binding)
  #   end)
  # end

  # def dynamic(dyn, left_field, {:self, {:==, right_field}}, current_binding) do
  #   new_dyn = Query.dynamic([{^current_binding, q}], field(q, ^left_field) == field(q, ^right_field))

  #   merge_dynamic(dyn, new_dyn)
  # end

  # def dynamic(dyn, left_field, {:self, right_field}, current_binding) do
  #   dynamic(dyn, left_field, {:self, {:==, right_field}}, current_binding)
  # end

  # def dynamic(dyn, key, :==, value, current_binding) do
  #   new_dyn = Query.dynamic([{^current_binding, q}], field(q, ^key) == ^value)

  #   merge_dynamic(dyn, new_dyn)
  # end

  def merge_dynamic(nil, dyn), do: dyn
  def merge_dynamic(dyn_a, dyn_b), do: Query.dynamic(^dyn_a and ^dyn_b)

  def from(query, params) do
    as = params[:as]

    Query.from(query, as: ^as)
  end

  def join(query, :association, assoc_key, params, join_binding, current_binding) do
    qual = Map.get(params, :qualifier, :left)

    prefix = Map.get(params, :prefix)

    on = true

    if current_binding do
      Query.with_named_binding(query, join_binding, fn query, join_binding ->
        Query.join(query, qual, [{^current_binding, q}], assoc(q, ^assoc_key),
          as: ^join_binding,
          on: ^on,
          prefix: ^prefix
        )
      end)
    else
      Query.with_named_binding(query, join_binding, fn query, join_binding ->
        Query.join(query, qual, [q], assoc(q, ^assoc_key),
          as: ^join_binding,
          on: ^on,
          prefix: ^prefix
        )
      end)
    end
  end

  def join(query, :subquery, from, params, join_binding, current_binding) do
    qual = Map.get(params, :qualifier, :left)

    prefix = Map.get(params, :prefix)

    on = true

    cond do
      not_nil?(current_binding) and not_nil?(join_binding) ->
        Query.with_named_binding(query, join_binding, fn query, join_binding ->
          Query.join(query, qual, [{^current_binding, q}], subquery(from),
            as: ^join_binding,
            on: ^on,
            prefix: ^prefix
          )
        end)

      not_nil?(current_binding) ->
        Query.join(query, qual, [{^current_binding, q}], subquery(from),
          on: ^on,
          prefix: ^prefix
        )

      not_nil?(join_binding) ->
        Query.join(query, qual, [q], subquery(from),
          as: ^join_binding,
          on: ^on,
          prefix: ^prefix
        )

      true ->
        Query.join(query, qual, [q], subquery(from), on: ^on, prefix: ^prefix)

    end
  end

  def select(query, current_binding) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], q)
    else
      Query.select(query, [q], q)
    end
  end

  def select(query, :map, keys, current_binding) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], map(q, ^keys))
    else
      Query.select(query, [q], map(q, ^keys))
    end
  end

  def select(query, :struct, keys, current_binding) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], struct(q, ^keys))
    else
      Query.select(query, [q], struct(q, ^keys))
    end
  end

  def select(query, key, value, current_binding) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], field(q, ^key) == ^value)
    else
      Query.select(query, [q], field(q, ^key) == ^value)
    end
  end

  # select_merge

  def select_merge(query, current_binding) do
    if current_binding do
      Query.select_merge(query, [{^current_binding, q}], q)
    else
      Query.select_merge(query, [q], q)
    end
  end

  def select_merge(query, keys, current_binding) do
    if current_binding do
      Query.select_merge(query, [{^current_binding, q}], map(q, ^keys))
    else
      Query.select_merge(query, [q], map(q, ^keys))
    end
  end

  def select_merge(query, key, value, current_binding) do
    if current_binding do
      Query.select_merge(query, [{^current_binding, q}], %{^key => field(q, ^value)})
    else
      Query.select_merge(query, [q], %{^key => field(q, ^value)})
    end
  end

  # or_where

  def or_where(query, key, :!=, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) != field(parent_as(^parent_binding), ^parent_field))
    else
      Query.or_where(query, [q], field(q, ^key) != field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def or_where(query, key, :==, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) == field(parent_as(^parent_binding), ^parent_field))
    else
      Query.or_where(query, [q], field(q, ^key) == field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def or_where(query, key, :>, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) > field(parent_as(^parent_binding), ^parent_field))
    else
      Query.or_where(query, [q], field(q, ^key) > field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def or_where(query, key, :<, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) < field(parent_as(^parent_binding), ^parent_field))
    else
      Query.or_where(query, [q], field(q, ^key) < field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def or_where(query, key, :>=, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) >= field(parent_as(^parent_binding), ^parent_field))
    else
      Query.or_where(query, [q], field(q, ^key) >= field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def or_where(query, key, :<=, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) <= field(parent_as(^parent_binding), ^parent_field))
    else
      Query.or_where(query, [q], field(q, ^key) <= field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def or_where(query, key, :!=, nil, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], not is_nil(field(q, ^key)))
    else
      Query.or_where(query, [q], not is_nil(field(q, ^key)))
    end
  end

  def or_where(query, key, :==, nil, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], is_nil(field(q, ^key)))
    else
      Query.or_where(query, [q], is_nil(field(q, ^key)))
    end
  end

  def or_where(query, key, :!=, value, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) != ^value)
    else
      Query.or_where(query, [q], field(q, ^key) != ^value)
    end
  end

  def or_where(query, key, :==, value, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) == ^value)
    else
      Query.or_where(query, [q], field(q, ^key) == ^value)
    end
  end

  def or_where(query, key, :>, value, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) > ^value)
    else
      Query.or_where(query, [q], field(q, ^key) > ^value)
    end
  end

  def or_where(query, key, :<, value, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) < ^value)
    else
      Query.or_where(query, [q], field(q, ^key) < ^value)
    end
  end

  def or_where(query, key, :>=, value, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) >= ^value)
    else
      Query.or_where(query, [q], field(q, ^key) >= ^value)
    end
  end

  def or_where(query, key, :<=, value, current_binding) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], field(q, ^key) <= ^value)
    else
      Query.or_where(query, [q], field(q, ^key) <= ^value)
    end
  end

  # where

  def where(query, key, :!=, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) != field(parent_as(^parent_binding), ^parent_field))
    else
      Query.where(query, [q], field(q, ^key) != field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def where(query, key, :==, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) == field(parent_as(^parent_binding), ^parent_field))
    else
      Query.where(query, [q], field(q, ^key) == field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def where(query, key, :>, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) > field(parent_as(^parent_binding), ^parent_field))
    else
      Query.where(query, [q], field(q, ^key) > field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def where(query, key, :<, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) < field(parent_as(^parent_binding), ^parent_field))
    else
      Query.where(query, [q], field(q, ^key) < field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def where(query, key, :>=, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) >= field(parent_as(^parent_binding), ^parent_field))
    else
      Query.where(query, [q], field(q, ^key) >= field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def where(query, key, :<=, {:parent_as, {parent_binding, parent_field}}, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) <= field(parent_as(^parent_binding), ^parent_field))
    else
      Query.where(query, [q], field(q, ^key) <= field(parent_as(^parent_binding), ^parent_field))
    end
  end

  def where(query, key, :!=, nil, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], not is_nil(field(q, ^key)))
    else
      Query.where(query, [q], not is_nil(field(q, ^key)))
    end
  end

  def where(query, key, :==, nil, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], is_nil(field(q, ^key)))
    else
      Query.where(query, [q], is_nil(field(q, ^key)))
    end
  end

  def where(query, key, :!=, value, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) != ^value)
    else
      Query.where(query, [q], field(q, ^key) != ^value)
    end
  end

  def where(query, key, :==, value, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) == ^value)
    else
      Query.where(query, [q], field(q, ^key) == ^value)
    end
  end

  def where(query, key, :>, value, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) > ^value)
    else
      Query.where(query, [q], field(q, ^key) > ^value)
    end
  end

  def where(query, key, :<, value, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) < ^value)
    else
      Query.where(query, [q], field(q, ^key) < ^value)
    end
  end

  def where(query, key, :>=, value, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) >= ^value)
    else
      Query.where(query, [q], field(q, ^key) >= ^value)
    end
  end

  def where(query, key, :<=, value, current_binding) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], field(q, ^key) <= ^value)
    else
      Query.where(query, [q], field(q, ^key) <= ^value)
    end
  end

  defp not_nil?(term), do: is_nil(term) === false
end
