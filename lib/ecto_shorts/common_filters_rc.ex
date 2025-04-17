defmodule EctoShorts.CommonFiltersRc do
  @moduledoc """
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{id: 1})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{id: %{==: 1}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{as: :post, with_named_binding: %{post: %{id: %{==: 1}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{as: :post, where: %{with_named_binding: %{post: %{id: %{==: 1}}}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{where: %{post_id: %{==: %{parent_as: %{post: :id}}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{where: [%{post_id: %{==: %{parent_as: %{post: :id}}}}]})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{select: true})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{select: [:id, :body]})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{select: [map: [:id, :body]]})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{select: [struct: [:id, :body]]})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{select: %{map: [:id, :body]}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{select: %{struct: [:id, :body]}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Comment, %{select: [:post_id], select_merge: [:id]})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{id: 1}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{id: %{>: 1}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{where: %{id: %{>: 1}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{where: [%{id: %{>: 1}}, %{id: %{<: 3}}]}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{or_where: %{id: %{>: 1}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{or_where: [%{id: %{>: 1}}, %{id: %{<: 3}}]}})
  """
  alias EctoShorts.CommonFiltersRc.Schema

  @doc """

  """
  def convert_params_to_filter(query, params \\ %{})

  def convert_params_to_filter(query, params) when params === %{}  do
    query
  end

  def convert_params_to_filter(query, params) when is_list(params) do
    convert_params_to_filter(query, Map.new(params))
  end

  def convert_params_to_filter(query, params) do
    Schema.build_query(query, params)
  end
end

# defmodule EctoShorts.CommonFiltersRc do
#   @moduledoc """
#   ## Examples

#   EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1})
#   EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{>: 1}})
#   EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: :comments}})
#   EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{as: :my_custom_binding}}}})


#   ```elixir
#   iex> EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1})
#   #Ecto.Query<from p0 in EctoShorts.Support.Schemas.Post, where: p0.id == ^1>
#   ```

#   ```elixir
#   iex> EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1, comments: %{id: 2}})
#   #Ecto.Query<from p0 in EctoShorts.Support.Schemas.Post,
#   left_join: c1 in assoc(p0, :comments), as: :ecto_shorts_post_comments,
#   where: c1.id == ^2, where: p0.id == ^1>
#   ```

#   ```elixir
#   iex> EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1, comments: %{id: 2, user: %{email: "foo@bar.com"}}})
#   #Ecto.Query<from p0 in EctoShorts.Support.Schemas.Post,
#   left_join: c1 in assoc(p0, :comments), as: :ecto_shorts_post_comments,
#   left_join: u2 in assoc(c1, :user), as: :ecto_shorts_comment_user,
#   where: c1.id == ^2, where: u2.email == ^"foo@bar.com", where: p0.id == ^1>
#   ```

#   ```elixir
#   iex> EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1, select: true})
#   #Ecto.Query<from p0 in EctoShorts.Support.Schemas.Post, where: p0.id == ^1, select: p0>
#   ```

#   ```elixir
#   iex> EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1, comments: %{select: true}})
#   #Ecto.Query<from p0 in EctoShorts.Support.Schemas.Post,
#   left_join: c1 in assoc(p0, :comments), as: :ecto_shorts_post_comments,
#   where: p0.id == ^1, select: c1>
#   ```

#   ```elixir
#   iex> EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1, select: [:title]})
#   #Ecto.Query<from p0 in EctoShorts.Support.Schemas.Post, where: p0.id == ^1,
#   select: struct(p0, [:title])>
#   ```

#   ```elixir
#   iex> EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1, select: %{map: [:title]}})
#   #Ecto.Query<from p0 in EctoShorts.Support.Schemas.Post, where: p0.id == ^1,
#   select: map(p0, [:title])>
#   ```

#   ```elixir
#   iex> EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: [:title], comments: %{select_merge: [:body]}})
#   ```
#   """

#   alias EctoShorts.CommonFiltersRc.Common
#   alias EctoShorts.CommonFiltersRc.Schema
#   alias EctoShorts.CommonSchemas
#   alias EctoShorts.CommonFiltersRc.QueryExpression

#   @comparison_filters ~w(
#     >
#     <
#     >=
#     <=
#     ==
#   )a

#   @query_filters ~w(
#     distinct
#     except
#     except_all
#     exclude
#     first
#     group_by
#     having
#     intersect
#     intersect_all
#     join
#     last
#     lock
#     offset
#     or_having
#     or_where
#     order_by
#     preload
#     prepend_order_by
#     put_query_prefix
#     recursive_ctes
#     reverse_order
#     select
#     select_merge
#     union
#     union_all
#     where
#     with_cte
#     with_named_binding
#     with_ties
#     windows
#   )a

#   @doc """

#   """
#   def convert_params_to_filter(query, params)

#   def convert_params_to_filter(query, params) when params === %{}  do
#     query
#   end

#   def convert_params_to_filter(query, params) when is_list(params) do
#     convert_params_to_filter(query, Map.new(params))
#   end

#   def convert_params_to_filter(query, params) do
#     {current_binding, params} = Map.pop(params, :with_named_binding)

#     Enum.reduce(params, CommonSchemas.get_schema_query(query), &reduce_schema_filter(&1, &2, current_binding))
#   end

#   defp reduce_schema_filter({key, value}, query, current_binding) do
#     build_schema_filter(
#       query,
#       CommonSchemas.get_schema_queryable(query),
#       {key, value},
#       current_binding
#     )
#   end

#   def build_schema_filter(query, schema_module, params, current_binding) when is_map(params) do
#     Enum.reduce(params, query, fn {key, value}, query ->
#       build_schema_filter(query, schema_module, {key, value}, current_binding)
#     end)
#   end

#   def build_schema_filter(query, schema_module, {key, value}, current_binding) do
#     if association?(schema_module, key) do
#       build_join_query(query, schema_module, {key, value}, current_binding)
#     else
#       build_schema_query(query, schema_module, {key, value}, current_binding)
#     end
#   end

#   defp build_join_query(query, schema_module, {assoc_key, params}, current_binding) do
#     ecto_assoc = ecto_association(schema_module, assoc_key)

#     assoc_schema_module = ecto_assoc.queryable

#     {join_params, params} = Map.pop(params, :join)

#     {assoc_binding, params} = Map.pop(params, :as)

#     assoc_binding = assoc_binding || :"#{QueryExpression.named_binding(assoc_key)}"

#     query
#     |> apply_join_expression(schema_module, current_binding, :association, {assoc_key, join_params}, assoc_binding)
#     |> build_schema_filter(assoc_schema_module, params, assoc_binding)
#   end

#   defp build_schema_query(query, schema_module, {key, params}, current_binding) when is_map(params) do
#     Enum.reduce(params, query, fn tuple, query ->
#       build_schema_query(query, schema_module, {key, tuple}, current_binding)
#     end)
#   end

#   defp build_schema_query(query, schema_module, {:join, params}, current_binding) when is_map(params) do
#     Enum.reduce(params, query, fn {key, value}, query ->
#       build_schema_query(query, schema_module, {:join, {key, value}}, current_binding)
#     end)
#   end

#   defp build_schema_query(query, schema_module, {:join, {:association, params}}, current_binding) when is_map(params) do
#     Enum.reduce(params, query, fn {key, value}, query ->
#       build_schema_query(query, schema_module, {:join, {:association, {key, value}}}, current_binding)
#     end)
#   end

#   defp build_schema_query(query, schema_module, {:join, {:association, {assoc_key, params}}}, current_binding) do
#     {assoc_binding, params} = Map.pop(params, :as)

#     apply_join_expression(query, schema_module, current_binding, :association, {assoc_key, params}, assoc_binding)
#   end

#   defp build_schema_query(query, schema_module, {:join, {:association, assoc_key}}, current_binding) do
#     apply_join_expression(query, schema_module, current_binding, :association, {assoc_key, %{}}, nil)
#   end

#   defp build_schema_query(query, _schema_module, {:select_merge, value}, current_binding) do
#     case value do
#       true ->
#         QueryExpression.select_merge(query, current_binding)

#       keys when is_list(keys) ->
#         QueryExpression.select_merge(query, current_binding, keys)

#       params when is_map(params) ->
#         Enum.reduce(params, query, fn {type, keys}, query ->
#           QueryExpression.select_merge(query, current_binding, type, keys)
#         end)

#     end
#   end

#   defp build_schema_query(query, _schema_module, {:select, value}, current_binding) do
#     case value do
#       true ->
#         QueryExpression.select(query, current_binding)

#       keys when is_list(keys) ->
#         QueryExpression.select(query, current_binding, keys)

#       params when is_map(params) ->
#         Enum.reduce(params, query, fn {type, keys}, query ->
#           QueryExpression.select(query, current_binding, type, keys)
#         end)

#     end
#   end

#   defp build_schema_query(query, _schema_module, {key, {operator, value}}, current_binding) do
#     QueryExpression.where(query, current_binding, key, operator, value)
#   end

#   defp build_schema_query(query, _schema_module, {key, value}, current_binding) do
#     QueryExpression.where(query, current_binding, key, :==, value)
#   end

#   defp apply_join_expression(query, _schema_module, current_binding, :association, {assoc_key, params}, assoc_binding) do
#     assoc_binding = assoc_binding || :"#{QueryExpression.named_binding(assoc_key)}"

#     QueryExpression.join(query, current_binding, assoc_key, assoc_binding, params)
#   end

#   defp association?(schema_module, key), do: key in schema_module.__schema__(:associations)
#   defp ecto_association(schema_module, key), do: schema_module.__schema__(:association, key)
# end
