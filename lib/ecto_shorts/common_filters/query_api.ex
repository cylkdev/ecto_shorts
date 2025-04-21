# defmodule EctoShorts.CommonFilters.CommonQueryBuildersApi do
#   @moduledoc since: "2.5.0"
#   @moduledoc """
#   `EctoShorts.CommonFilters.CommonQueryBuildersApi`

#   This module provides a standardized api for composing ecto query expressions.
#   """

#   alias EctoShorts.CommonFilters.CommonQueryBuildersApi.Dynamic
#   alias Ecto.Query

#   require Ecto.Query

#   # def exclude(query, field), do: Query.exclude(query, field)

#   # def subquery(query, opts), do: Query.subquery(query, opts)

#   # def limit(query, current_binding, value) do
#   #   if current_binding do
#   #     Query.limit(query, [{^current_binding, q}], ^value)
#   #   else
#   #     Query.limit(query, [q], ^value)
#   #   end
#   # end

#   # def offset(query, current_binding, value) do
#   #   if current_binding do
#   #     Query.offset(query, [{^current_binding, q}], ^value)
#   #   else
#   #     Query.offset(query, [q], ^value)
#   #   end
#   # end

#   # def group_by(query, current_binding, value) do
#   #   if current_binding do
#   #     Query.group_by(query, [{^current_binding, q}], ^value)
#   #   else
#   #     Query.group_by(query, [q], ^value)
#   #   end
#   # end

#   # def order_by(query, current_binding, value) do
#   #   if current_binding do
#   #     Query.order_by(query, [{^current_binding, q}], ^value)
#   #   else
#   #     Query.order_by(query, [q], ^value)
#   #   end
#   # end

#   # def preload(query, current_binding, expr) do
#   #   # TODO: support more of the functionality https://hexdocs.pm/ecto/Ecto.Query.html#preload/3
#   #   if current_binding do
#   #     Query.preload(query, [{^current_binding, q}], ^expr)
#   #   else
#   #     Query.preload(query, [q], ^expr)
#   #   end
#   # end

#   # def join(query, join_binding, filter, schema_field, params) when is_list(params) do
#   #   join(query, join_binding, filter, schema_field, Map.new(params))
#   # end

#   # def join(query, {current_binding, as}, :subquery, from, params) do
#   #   qual = params[:qualifier] || :left

#   #   prefix = params[:prefix]

#   #   on = dynamic(current_binding, params[:on] || true)

#   #   Query.with_named_binding(query, as, fn query, as ->
#   #     if current_binding do
#   #       Query.join(query, qual, [{^current_binding, q}], subquery(from),
#   #         as: ^as,
#   #         on: ^on,
#   #         prefix: ^prefix
#   #       )
#   #     else
#   #       Query.join(query, qual, [q], subquery(from), as: ^as, on: ^on, prefix: ^prefix)
#   #     end
#   #   end)
#   # end

#   # def join(query, current_binding, :subquery, from, params) do
#   #   qual = params[:qualifier] || :left

#   #   prefix = params[:prefix]

#   #   on = dynamic(current_binding, params[:on] || true)

#   #   if current_binding do
#   #     Query.join(query, qual, [{^current_binding, q}], subquery(from), on: ^on, prefix: ^prefix)
#   #   else
#   #     Query.join(query, qual, [q], subquery(from), on: ^on, prefix: ^prefix)
#   #   end
#   # end

#   # def join(query, {current_binding, as}, :association, schema_field, params) do
#   #   qual = params[:qualifier] || :left

#   #   prefix = params[:prefix]

#   #   on = dynamic(current_binding, params[:on] || true)

#   #   if current_binding do
#   #     Query.with_named_binding(query, as, fn query, as ->
#   #       Query.join(query, qual, [{^current_binding, q}], assoc(q, ^schema_field),
#   #         as: ^as,
#   #         on: ^on,
#   #         prefix: ^prefix
#   #       )
#   #     end)
#   #   else
#   #     Query.with_named_binding(query, as, fn query, as ->
#   #       Query.join(query, qual, [q], assoc(q, ^schema_field),
#   #         as: ^as,
#   #         on: ^on,
#   #         prefix: ^prefix
#   #       )
#   #     end)
#   #   end
#   # end

#   # def join(query, current_binding, :association, schema_field, params) do
#   #   qual = params[:qualifier] || :left

#   #   prefix = params[:prefix]

#   #   on = dynamic(current_binding, params[:on] || true)

#   #   if current_binding do
#   #     Query.join(query, qual, [{^current_binding, q}], assoc(q, ^schema_field),
#   #       on: ^on,
#   #       prefix: ^prefix
#   #     )
#   #   else
#   #     Query.join(query, qual, [q], assoc(q, ^schema_field),
#   #       on: ^on,
#   #       prefix: ^prefix
#   #     )
#   #   end
#   # end

#   # def select(query, current_binding, true) do
#   #   if current_binding do
#   #     Query.select(query, [{^current_binding, q}], q)
#   #   else
#   #     Query.select(query, [q], q)
#   #   end
#   # end

#   # def select(query, current_binding, values) do
#   #   select(query, current_binding, :struct, values)
#   # end

#   # def select(query, current_binding, :map, schema_fields) do
#   #   if current_binding do
#   #     Query.select(query, [{^current_binding, q}], map(q, ^schema_fields))
#   #   else
#   #     Query.select(query, [q], map(q, ^schema_fields))
#   #   end
#   # end

#   # def select(query, current_binding, :struct, schema_fields) do
#   #   if current_binding do
#   #     Query.select(query, [{^current_binding, q}], struct(q, ^schema_fields))
#   #   else
#   #     Query.select(query, [q], struct(q, ^schema_fields))
#   #   end
#   # end

#   # def select(query, current_binding, schema_field, value) do
#   #   if current_binding do
#   #     Query.select(query, [{^current_binding, q}], field(q, ^schema_field) == ^value)
#   #   else
#   #     Query.select(query, [q], field(q, ^schema_field) == ^value)
#   #   end
#   # end

#   # # select_merge

#   # def select_merge(query, current_binding, true) do
#   #   if current_binding do
#   #     Query.select_merge(query, [{^current_binding, q}], q)
#   #   else
#   #     Query.select_merge(query, [q], q)
#   #   end
#   # end

#   # def select_merge(query, current_binding, values) do
#   #   if current_binding do
#   #     Query.select_merge(query, [{^current_binding, q}], map(q, ^values))
#   #   else
#   #     Query.select_merge(query, [q], map(q, ^values))
#   #   end
#   # end

#   # def select_merge(
#   #       query,
#   #       current_binding,
#   #       key,
#   #       {:parent_as, {parent_binding, parent_schema_field}}
#   #     ) do
#   #   if current_binding do
#   #     Query.select_merge(query, [{^current_binding, q}], %{
#   #       ^key => field(parent_as(^parent_binding), ^parent_schema_field)
#   #     })
#   #   else
#   #     Query.select_merge(query, [q], %{
#   #       ^key => field(parent_as(^parent_binding), ^parent_schema_field)
#   #     })
#   #   end
#   # end

#   # def select_merge(query, current_binding, key, schema_field) do
#   #   if current_binding do
#   #     Query.select_merge(query, [{^current_binding, q}], %{^key => field(q, ^schema_field)})
#   #   else
#   #     Query.select_merge(query, [q], %{^key => field(q, ^schema_field)})
#   #   end
#   # end

#   # def or_where(query, current_binding, params) do
#   #   Query.or_where(query, ^dynamic(current_binding, params))
#   # end

#   # def or_where(query, current_binding, schema_field, operator, value) do
#   #   dyn_expr = dynamic(current_binding, schema_field, operator, value) || []

#   #   Query.or_where(query, ^dyn_expr)
#   # end

#   # def where(query, current_binding, params) do
#   #   Query.where(query, ^dynamic(current_binding, params))
#   # end

#   # def where(query, current_binding, schema_field, operator, value) do
#   #   dyn_expr = dynamic(current_binding, schema_field, operator, value) || []

#   #   Query.where(query, ^dyn_expr)
#   # end

#   # def dynamic(condition, current_binding, schema_field, :==, value) do
#   #   if current_binding do
#   #     Query.dynamic([{^current_binding, q}], field(q, ^schema_field) == ^value)
#   #   else
#   #     Query.dynamic([q], field(q, ^schema_field) == ^value)
#   #   end
#   # end

#   # defp merge_dynamic(nil, _, dyn), do: dyn
#   # defp merge_dynamic(dyn_a, :and, dyn_b), do: Query.dynamic([q], ^dyn_a and ^dyn_b)
#   # defp merge_dynamic(dyn_a, :or, dyn_b), do: Query.dynamic([q], ^dyn_a or ^dyn_b)

#   # def dynamic(value) do
#   #   dynamic(nil, value)
#   # end

#   # def dynamic(_current_binding, true) do
#   #   Query.dynamic([q], q)
#   # end

#   # def dynamic(current_binding, value) do
#   #   Dynamic.traverse_params(nil, current_binding, value, fn
#   #     dyn, current_binding, schema_field, operator, value ->
#   #       dynamic(dyn, :and, current_binding, schema_field, operator, value)
#   #   end)
#   # end

#   # def dynamic(current_binding, schema_field, value) do
#   #   Dynamic.traverse_params(nil, current_binding, {schema_field, value}, fn
#   #     dyn, current_binding, schema_field, operator, value ->
#   #       dynamic(dyn, :and, current_binding, schema_field, operator, value)
#   #   end)
#   # end

#   # def dynamic(current_binding, schema_field, operator, value) do
#   #   Dynamic.traverse_params(nil, current_binding, {schema_field, {operator, value}}, fn
#   #     dyn, current_binding, schema_field, operator, value ->
#   #       dynamic(dyn, :and, current_binding, schema_field, operator, value)
#   #   end)
#   # end

#   # @doc """
#   # ...
#   # """
#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       current_binding,
#   #       schema_field,
#   #       operator,
#   #       {:parent_as, {parent_binding, value}}
#   #     ) do
#   #   dynamic(
#   #     dyn,
#   #     condition,
#   #     {current_binding, parent_binding},
#   #     schema_field,
#   #     operator,
#   #     value
#   #   )
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, operator, {:parent_as, params}) do
#   #   Enum.reduce(params, dyn, fn {parent_binding, value}, dyn ->
#   #     dynamic(
#   #       dyn,
#   #       condition,
#   #       current_binding,
#   #       schema_field,
#   #       operator,
#   #       {:parent_as, {parent_binding, value}}
#   #     )
#   #   end)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :ilike,
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         fragment(
#   #           "? ILIKE '%' || ? || '%' OR ? ILIKE '%' || ? || '%'",
#   #           field(q, ^schema_field),
#   #           field(parent_as(^parent_binding), ^parent_schema_field),
#   #           field(parent_as(^parent_binding), ^parent_schema_field),
#   #           field(q, ^schema_field)
#   #         )
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         fragment(
#   #           "? ILIKE '%' || ? || '%' OR ? ILIKE '%' || ? || '%'",
#   #           field(q, ^schema_field),
#   #           field(parent_as(^parent_binding), ^parent_schema_field),
#   #           field(parent_as(^parent_binding), ^parent_schema_field),
#   #           field(q, ^schema_field)
#   #         )
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :ilike, value) do
#   #   query_string = "%#{value}%"

#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], ilike(field(q, ^schema_field), ^query_string))
#   #     else
#   #       Query.dynamic([q], ilike(field(q, ^schema_field), ^query_string))
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :like,
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         fragment(
#   #           "? LIKE '%' || ? || '%' OR ? LIKE '%' || ? || '%'",
#   #           field(q, ^schema_field),
#   #           field(parent_as(^parent_binding), ^parent_schema_field),
#   #           field(parent_as(^parent_binding), ^parent_schema_field),
#   #           field(q, ^schema_field)
#   #         )
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         fragment(
#   #           "? LIKE '%' || ? || '%' OR ? LIKE '%' || ? || '%'",
#   #           field(q, ^schema_field),
#   #           field(parent_as(^parent_binding), ^parent_schema_field),
#   #           field(parent_as(^parent_binding), ^parent_schema_field),
#   #           field(q, ^schema_field)
#   #         )
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :like, value) do
#   #   query_string = "%#{value}%"

#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], like(field(q, ^schema_field), ^query_string))
#   #     else
#   #       Query.dynamic([q], like(field(q, ^schema_field), ^query_string))
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       {:!=, :in},
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         field(q, ^schema_field) not in field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         field(q, ^schema_field) not in field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, value, {:!=, :in}, schema_field) when is_atom(schema_field) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], ^value not in field(q, ^schema_field))
#   #     else
#   #       Query.dynamic([q], ^value not in field(q, ^schema_field))
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, {:!=, :in}, values) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], field(q, ^schema_field) not in ^values)
#   #     else
#   #       Query.dynamic([q], field(q, ^schema_field) not in ^values)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :!=,
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         field(q, ^schema_field) != field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         field(q, ^schema_field) != field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :!=, value) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], field(q, ^schema_field) != ^value)
#   #     else
#   #       Query.dynamic([q], field(q, ^schema_field) != ^value)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :in,
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         field(q, ^schema_field) in field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         field(q, ^schema_field) in field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :in, values) when is_list(values) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], field(q, ^schema_field) in ^values)
#   #     else
#   #       Query.dynamic([q], field(q, ^schema_field) in ^values)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, value, :in, schema_field)
#   #     when is_atom(schema_field) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], ^value in field(q, ^schema_field))
#   #     else
#   #       Query.dynamic([q], ^value in field(q, ^schema_field))
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :==,
#   #       {:lower, parent_schema_field}
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         fragment("LOWER(?)", field(q, ^schema_field)) ==
#   #           fragment("LOWER(?)", field(parent_as(^parent_binding), ^parent_schema_field))
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         fragment("LOWER(?)", field(q, ^schema_field)) ==
#   #           fragment("LOWER(?)", field(parent_as(^parent_binding), ^parent_schema_field))
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :==, {:lower, value}) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         fragment("LOWER(?)", field(q, ^schema_field)) == ^value
#   #       )
#   #     else
#   #       Query.dynamic([q], fragment("LOWER(?)", field(q, ^schema_field)) == ^value)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :==,
#   #       {:upper, parent_schema_field}
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         fragment("UPPER(?)", field(q, ^schema_field)) ==
#   #           fragment("UPPER(?)", field(parent_as(^parent_binding), ^parent_schema_field))
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         fragment("UPPER(?)", field(q, ^schema_field)) ==
#   #           fragment("UPPER(?)", field(parent_as(^parent_binding), ^parent_schema_field))
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :==, {:upper, value}) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         fragment("UPPER(?)", field(q, ^schema_field)) == ^value
#   #       )
#   #     else
#   #       Query.dynamic([q], fragment("UPPER(?)", field(q, ^schema_field)) == ^value)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :==,
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         field(q, ^schema_field) == field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         field(q, ^schema_field) == field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :==, value) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], field(q, ^schema_field) == ^value)
#   #     else
#   #       Query.dynamic([q], field(q, ^schema_field) == ^value)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :<,
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         field(q, ^schema_field) < field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         field(q, ^schema_field) < field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :<, value) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], field(q, ^schema_field) < ^value)
#   #     else
#   #       Query.dynamic([q], field(q, ^schema_field) < ^value)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :>,
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         field(q, ^schema_field) > field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         field(q, ^schema_field) > field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :>, value) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], field(q, ^schema_field) > ^value)
#   #     else
#   #       Query.dynamic([q], field(q, ^schema_field) > ^value)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :<=,
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         field(q, ^schema_field) <= field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         field(q, ^schema_field) <= field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :<=, value) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], field(q, ^schema_field) <= ^value)
#   #     else
#   #       Query.dynamic([q], field(q, ^schema_field) <= ^value)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(
#   #       dyn,
#   #       condition,
#   #       {current_binding, parent_binding},
#   #       schema_field,
#   #       :>=,
#   #       parent_schema_field
#   #     ) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic(
#   #         [{^current_binding, q}],
#   #         field(q, ^schema_field) >= field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     else
#   #       Query.dynamic(
#   #         [q],
#   #         field(q, ^schema_field) >= field(parent_as(^parent_binding), ^parent_schema_field)
#   #       )
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, :>=, value) do
#   #   dyn_expr =
#   #     if current_binding do
#   #       Query.dynamic([{^current_binding, q}], field(q, ^schema_field) >= ^value)
#   #     else
#   #       Query.dynamic([q], field(q, ^schema_field) >= ^value)
#   #     end

#   #   merge_dynamic(dyn, condition, dyn_expr)
#   # end

#   # def dynamic(dyn, condition, current_binding, schema_field, operator, params) do
#   #   Enum.reduce(params, dyn, fn {modifier, value}, dyn ->
#   #     dynamic(dyn, condition, current_binding, schema_field, operator, {modifier, value})
#   #   end)
#   # end
# end
