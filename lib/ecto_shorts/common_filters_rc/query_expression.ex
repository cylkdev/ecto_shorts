defmodule EctoShorts.CommonFiltersRc.QueryExpression do
  @moduledoc since: "2.5.0"
  @moduledoc """
  `EctoShorts.CommonFiltersRc.QueryExpression`

  This module provides a standardized api for composing ecto query expressions.
  """
  alias EctoShorts.CommonFiltersRc.QueryExpressionBuilder
  alias Ecto.Query

  require Ecto.Query

  def named_binding_atom(assoc_key) do
    assoc_key |> named_binding_string() |> String.to_atom()
  end

  def named_binding_string(assoc_key), do: "ecto_shorts_#{assoc_key}"

  def from(%{query: query} = params) do
    from(query, Map.delete(params, :query))
  end

  def from(query, params) do
    {from_params, params} =
      EctoShorts.Utils.pop(params, [
        :as,
        :join,
        :limit,
        :offset,
        :group_by,
        :order_by,
        :where
      ])

    as = from_params[:as]

    join = from_params[:join]

    limit = from_params[:limit]

    offset = from_params[:offset]

    group_by = from_params[:group_by]

    order_by = from_params[:order_by]

    where = from_params[:where] || %{}

    if as do
      Query.with_named_binding(query, as, fn query, as ->
        query
        |> Query.from(as: ^as)
        |> maybe_join(as, join)
        |> maybe_limit(as, limit)
        |> maybe_offset(as, offset)
        |> maybe_group_by(as, group_by)
        |> maybe_order_by(as, order_by)
        |> where(as, Map.merge(params, where))
      end)
    else
      query
      |> Query.from()
      |> maybe_join(nil, join)
      |> maybe_limit(nil, limit)
      |> maybe_offset(nil, offset)
      |> maybe_group_by(nil, group_by)
      |> maybe_order_by(nil, order_by)
      |> where(nil, Map.merge(params, where))
    end
  end

  defp maybe_join(query, _current_binding, nil), do: query
  defp maybe_join(query, current_binding, value), do: join(query, current_binding, value)

  defp maybe_limit(query, _current_binding, nil), do: query
  defp maybe_limit(query, current_binding, value), do: limit(query, current_binding, value)

  defp maybe_offset(query, _current_binding, nil), do: query
  defp maybe_offset(query, current_binding, value), do: offset(query, current_binding, value)

  defp maybe_group_by(query, _current_binding, nil), do: query
  defp maybe_group_by(query, current_binding, value), do: group_by(query, current_binding, value)

  defp maybe_order_by(query, _current_binding, nil), do: query
  defp maybe_order_by(query, current_binding, value), do: order_by(query, current_binding, value)

  def exclude(query, field), do: Query.exclude(query, field)

  def subquery(query, opts), do: Query.subquery(query, opts)

  def limit(query, current_binding, value) do
    if current_binding do
      Query.limit(query, [{^current_binding, q}], ^value)
    else
      Query.limit(query, [q], ^value)
    end
  end

  def offset(query, current_binding, value) do
    if current_binding do
      Query.offset(query, [{^current_binding, q}], ^value)
    else
      Query.offset(query, [q], ^value)
    end
  end

  def group_by(query, current_binding, value) do
    if current_binding do
      Query.group_by(query, [{^current_binding, q}], ^value)
    else
      Query.group_by(query, [q], ^value)
    end
  end

  def order_by(query, current_binding, value) do
    if current_binding do
      Query.order_by(query, [{^current_binding, q}], ^value)
    else
      Query.order_by(query, [q], ^value)
    end
  end

  def preload(query, current_binding, expr) do
    # TODO: support more of the functionality https://hexdocs.pm/ecto/Ecto.Query.html#preload/3
    if current_binding do
      Query.preload(query, [{^current_binding, q}], ^expr)
    else
      Query.preload(query, [q], ^expr)
    end
  end

  def join(query, current_binding, {filter, params}) when filter in [:association, :subquery] do
    Enum.reduce(params, query, fn {assoc_key, params}, query ->
      join(query, current_binding, {filter, {assoc_key, params}})
    end)
  end

  def join(query, current_binding, {assoc_key, %{from: _} = params}) do
    join(query, current_binding, {:subquery, {assoc_key, params}})
  end

  def join(query, current_binding, {assoc_key, params}) do
    join(query, current_binding, {:association, {assoc_key, params}})
  end

  def join(query, current_binding, params) when is_list(params) or is_map(params) do
    Enum.reduce(params, query, fn {type, params}, query ->
      join(query, current_binding, {type, params})
    end)
  end

  def join(query, current_binding, {:subquery, params}) do
    {as, params} = Map.pop(params, :as)

    qual = params[:qualifier] || :left

    on =
      params
      |> Map.delete(:on)
      |> Map.merge(params[:on] || true)
      |> dynamic()

    prefix = params[:prefix]

    from =
      case Map.fetch!(params, :from) do
        params when is_list(params) or is_map(params) -> from(params)
        value -> value
      end

    if as do
      Query.with_named_binding(query, as, fn query, as ->
        if current_binding do
          Query.join(query, qual, [{^current_binding, q}], subquery(from),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        else
          Query.join(query, qual, [q], subquery(from), as: ^as, on: ^on, prefix: ^prefix)
        end
      end)
    else
      if current_binding do
        Query.join(query, qual, [{^current_binding, q}], subquery(from), on: ^on, prefix: ^prefix)
      else
        Query.join(query, qual, [q], subquery(from), on: ^on, prefix: ^prefix)
      end
    end
  end

  def join(query, current_binding, {:association, {assoc_key, params}}) do
    {as, params} = Map.pop(params, :as)

    qual = params[:qualifier] || :left

    on =
      params
      |> Map.delete(:on)
      |> Map.merge(params[:on] || true)
      |> dynamic()

    prefix = params[:prefix]

    if as do
      if current_binding do
        Query.with_named_binding(query, as, fn query, as ->
          Query.join(query, qual, [{^current_binding, q}], assoc(q, ^assoc_key),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end)
      else
        Query.with_named_binding(query, as, fn query, as ->
          Query.join(query, qual, [q], assoc(q, ^assoc_key),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end)
      end
    else
      if current_binding do
        Query.join(query, qual, [{^current_binding, q}], assoc(q, ^assoc_key),
          on: ^on,
          prefix: ^prefix
        )
      else
        Query.join(query, qual, [q], assoc(q, ^assoc_key),
          on: ^on,
          prefix: ^prefix
        )
      end
    end
  end

  def select(query, current_binding, true) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], q)
    else
      Query.select(query, [q], q)
    end
  end

  def select(query, current_binding, params) when is_map(params) do
    Enum.reduce(params, query, fn {assoc_key, value}, query ->
      select(query, current_binding, {assoc_key, value})
    end)
  end

  def select(query, current_binding, values) when is_list(values) do
    if Keyword.keyword?(values) do
      Enum.reduce(values, query, fn {assoc_key, value}, query ->
        select(query, current_binding, {assoc_key, value})
      end)
    else
      select(query, current_binding, {:struct, values})
    end
  end

  def select(query, current_binding, {:map, assoc_keys}) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], map(q, ^assoc_keys))
    else
      Query.select(query, [q], map(q, ^assoc_keys))
    end
  end

  def select(query, current_binding, {:struct, assoc_keys}) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], struct(q, ^assoc_keys))
    else
      Query.select(query, [q], struct(q, ^assoc_keys))
    end
  end

  def select(query, current_binding, {assoc_key, value}) do
    if current_binding do
      Query.select(query, [{^current_binding, q}], field(q, ^assoc_key) == ^value)
    else
      Query.select(query, [q], field(q, ^assoc_key) == ^value)
    end
  end

  # select_merge

  def select_merge(query, current_binding, params) when is_map(params) do
    Enum.reduce(params, query, fn {field_alias, value}, query ->
      select_merge(query, current_binding, {field_alias, value})
    end)
  end

  def select_merge(query, current_binding, {field_alias, params})
      when is_list(params) or is_map(params) do
    Enum.reduce(params, query, fn {filter, params}, query ->
      select_merge(query, current_binding, {field_alias, {filter, params}})
    end)
  end

  def select_merge(query, current_binding, {field_alias, {filter, params}})
      when is_list(params) or is_map(params) do
    Enum.reduce(params, query, fn {key, value}, query ->
      select_merge(query, current_binding, {field_alias, {filter, {key, value}}})
    end)
  end

  def select_merge(query, current_binding, true) do
    if current_binding do
      Query.select_merge(query, [{^current_binding, q}], q)
    else
      Query.select_merge(query, [q], q)
    end
  end

  def select_merge(query, current_binding, values) when is_list(values) do
    if Keyword.keyword?(values) do
      select_merge(query, current_binding, Map.new(values))
    else
      if current_binding do
        Query.select_merge(query, [{^current_binding, q}], map(q, ^values))
      else
        Query.select_merge(query, [q], map(q, ^values))
      end
    end
  end

  def select_merge(
        query,
        current_binding,
        {field_alias, {:parent_as, {parent_binding, parent_assoc_key}}}
      ) do
    if current_binding do
      Query.select_merge(query, [{^current_binding, q}], %{
        ^field_alias => field(parent_as(^parent_binding), ^parent_assoc_key)
      })
    else
      Query.select_merge(query, [q], %{
        ^field_alias => field(parent_as(^parent_binding), ^parent_assoc_key)
      })
    end
  end

  def select_merge(query, current_binding, {field_alias, assoc_key}) do
    if current_binding do
      Query.select_merge(query, [{^current_binding, q}], %{^field_alias => field(q, ^assoc_key)})
    else
      Query.select_merge(query, [q], %{^field_alias => field(q, ^assoc_key)})
    end
  end

  def or_where(query, current_binding, params) do
    if current_binding do
      Query.or_where(query, [{^current_binding, q}], ^dynamic(params))
    else
      Query.or_where(query, [q], ^dynamic(params))
    end
  end

  def where(query, current_binding, params) do
    if current_binding do
      Query.where(query, [{^current_binding, q}], ^dynamic(params))
    else
      Query.where(query, [q], ^dynamic(params))
    end
  end

  def dynamic(true), do: Query.dynamic([q], q)
  def dynamic(nil), do: dynamic(true)
  def dynamic(params), do: QueryExpressionBuilder.traverse_params(params, &dynamic/6)

  @doc """
  ...
  """
  def merge_dynamic(nil, _condition, dyn), do: dyn

  def merge_dynamic(dyn_a, :and, dyn_b), do: Query.dynamic([q], ^dyn_a and ^dyn_b)

  def merge_dynamic(dyn_a, :or, dyn_b), do: Query.dynamic([q], ^dyn_a or ^dyn_b)

  @doc """
  ...
  """
  def dynamic(dyn, condition, current_binding, key, operator, value) do
    merge_dynamic(dyn, condition, dynamic(current_binding, key, operator, value))
  end

  @doc """
  ...
  """
  def dynamic(current_binding, key, :not, value), do: dynamic(current_binding, key, :!=, value)

  def dynamic(current_binding, key, :in, value), do: dynamic(current_binding, key, :==, value)

  def dynamic(current_binding, key, :eq, value), do: dynamic(current_binding, key, :==, value)

  def dynamic(current_binding, key, :lt, value), do: dynamic(current_binding, key, :<, value)

  def dynamic(current_binding, key, :gt, value), do: dynamic(current_binding, key, :>, value)

  def dynamic(current_binding, key, :lte, value), do: dynamic(current_binding, key, :<=, value)

  def dynamic(current_binding, key, :gte, value), do: dynamic(current_binding, key, :>=, value)

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :ilike, key) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment(
          "LOWER(?) ILIKE LOWER(?)",
          field(q, ^key),
          field(parent_as(^parent_binding), ^parent_key)
        )
      )
    else
      Query.dynamic(
        [q],
        fragment(
          "LOWER(?) ILIKE LOWER(?)",
          field(q, ^key),
          field(parent_as(^parent_binding), ^parent_key)
        )
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :like, key) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment(
          "LOWER(?) LIKE LOWER(?)",
          field(q, ^key),
          field(parent_as(^parent_binding), ^parent_key)
        )
      )
    else
      Query.dynamic(
        [q],
        fragment(
          "LOWER(?) LIKE LOWER(?)",
          field(q, ^key),
          field(parent_as(^parent_binding), ^parent_key)
        )
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :!=, {:lower, key}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("LOWER(?)", field(q, ^key)) !=
          fragment("LOWER(?)", field(parent_as(^parent_binding), ^parent_key))
      )
    else
      Query.dynamic(
        [q],
        fragment("LOWER(?)", field(q, ^key)) !=
          fragment("LOWER(?)", field(parent_as(^parent_binding), ^parent_key))
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :!=, {:upper, key}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("UPPER(?)", field(q, ^key)) !=
          fragment("UPPER(?)", field(parent_as(^parent_binding), ^parent_key))
      )
    else
      Query.dynamic(
        [q],
        fragment("UPPER(?)", field(q, ^key)) !=
          fragment("UPPER(?)", field(parent_as(^parent_binding), ^parent_key))
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :==, {:lower, key}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("LOWER(?)", field(q, ^key)) ==
          fragment("LOWER(?)", field(parent_as(^parent_binding), ^parent_key))
      )
    else
      Query.dynamic(
        [q],
        fragment("LOWER(?)", field(q, ^key)) ==
          fragment("LOWER(?)", field(parent_as(^parent_binding), ^parent_key))
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :==, {:upper, key}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("UPPER(?)", field(q, ^key)) ==
          fragment("UPPER(?)", field(parent_as(^parent_binding), ^parent_key))
      )
    else
      Query.dynamic(
        [q],
        fragment("UPPER(?)", field(q, ^key)) ==
          fragment("UPPER(?)", field(parent_as(^parent_binding), ^parent_key))
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :!=, key) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        field(q, ^key) != field(parent_as(^parent_binding), ^parent_key)
      )
    else
      Query.dynamic(
        [q],
        field(q, ^key) != field(parent_as(^parent_binding), ^parent_key)
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :==, key) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        field(q, ^key) == field(parent_as(^parent_binding), ^parent_key)
      )
    else
      Query.dynamic(
        [q],
        field(q, ^key) == field(parent_as(^parent_binding), ^parent_key)
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :<, key) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        field(q, ^key) < field(parent_as(^parent_binding), ^parent_key)
      )
    else
      Query.dynamic(
        [q],
        field(q, ^key) < field(parent_as(^parent_binding), ^parent_key)
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :>, key) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        field(q, ^key) > field(parent_as(^parent_binding), ^parent_key)
      )
    else
      Query.dynamic(
        [q],
        field(q, ^key) > field(parent_as(^parent_binding), ^parent_key)
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :<=, key) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        field(q, ^key) <= field(parent_as(^parent_binding), ^parent_key)
      )
    else
      Query.dynamic(
        [q],
        field(q, ^key) <= field(parent_as(^parent_binding), ^parent_key)
      )
    end
  end

  def dynamic(current_binding, {:parent_as, parent_binding, parent_key}, :>=, key) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        field(q, ^key) >= field(parent_as(^parent_binding), ^parent_key)
      )
    else
      Query.dynamic(
        [q],
        field(q, ^key) >= field(parent_as(^parent_binding), ^parent_key)
      )
    end
  end

  def dynamic(current_binding, key, :!=, {:lower, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("LOWER(?)", field(q, ^key)) != ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic(current_binding, key, :!=, {:upper, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("UPPER(?)", field(q, ^key)) != ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^key)) != ^value)
    end
  end

  def dynamic(current_binding, key, :==, {:lower, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("LOWER(?)", field(q, ^key)) == ^value
      )
    else
      Query.dynamic([q], fragment("LOWER(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic(current_binding, key, :==, {:upper, value}) do
    if current_binding do
      Query.dynamic(
        [{^current_binding, q}],
        fragment("UPPER(?)", field(q, ^key)) == ^value
      )
    else
      Query.dynamic([q], fragment("UPPER(?)", field(q, ^key)) == ^value)
    end
  end

  def dynamic(current_binding, key, :ilike, value) do
    query_string = "%#{value}%"

    if current_binding do
      Query.dynamic([{^current_binding, q}], ilike(field(q, ^key), ^query_string))
    else
      Query.dynamic([q], ilike(field(q, ^key), ^query_string))
    end
  end

  def dynamic(current_binding, key, :like, value) do
    query_string = "%#{value}%"

    if current_binding do
      Query.dynamic([{^current_binding, q}], like(field(q, ^key), ^query_string))
    else
      Query.dynamic([q], like(field(q, ^key), ^query_string))
    end
  end

  def dynamic(current_binding, key, :!=, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^key) not in ^values)
    else
      Query.dynamic([q], field(q, ^key) not in ^values)
    end
  end

  def dynamic(current_binding, key, :==, values) when is_list(values) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^key) in ^values)
    else
      Query.dynamic([q], field(q, ^key) in ^values)
    end
  end

  def dynamic(current_binding, key, :!=, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^key) != ^value)
    else
      Query.dynamic([q], field(q, ^key) != ^value)
    end
  end

  def dynamic(current_binding, key, :==, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^key) == ^value)
    else
      Query.dynamic([q], field(q, ^key) == ^value)
    end
  end

  def dynamic(current_binding, key, :<, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^key) < ^value)
    else
      Query.dynamic([q], field(q, ^key) < ^value)
    end
  end

  def dynamic(current_binding, key, :>, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^key) > ^value)
    else
      Query.dynamic([q], field(q, ^key) > ^value)
    end
  end

  def dynamic(current_binding, key, :<=, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^key) <= ^value)
    else
      Query.dynamic([q], field(q, ^key) <= ^value)
    end
  end

  def dynamic(current_binding, key, :>=, value) do
    if current_binding do
      Query.dynamic([{^current_binding, q}], field(q, ^key) >= ^value)
    else
      Query.dynamic([q], field(q, ^key) >= ^value)
    end
  end
end
