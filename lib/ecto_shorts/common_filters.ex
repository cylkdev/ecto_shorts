defmodule EctoShorts.CommonFilters do
  @moduledoc """
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{where: %{as: :post}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: [1, 2, 3]})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{!=: 1}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{!=: [1, 2, 3]}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{!=: %{lower: "example"}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{!=: %{upper: "example"}}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{==: 1}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{==: [1, 2, 3]}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{==: %{lower: "example"}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{==: %{upper: "example"}}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{>: 1}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{<: 1}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{>=: 1}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{<=: 1}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{title: %{ilike: "example"}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{title: %{like: "example"}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{id: 1}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{id: %{==: 1}}}}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, id: %{==: %{parent_as: %{post: :id}}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, join: %{association: %{comments: %{post_id: %{==: %{parent_as: %{post: :id}}}}}}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{subquery: %{from: EctoShorts.Support.Schemas.Comment, on: %{id: 1}}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{subquery: %{from: EctoShorts.Support.Schemas.Comment, on: %{id: %{==: 1}}}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, join: %{subquery: %{from: EctoShorts.Support.Schemas.Comment, on: %{post_id: %{==: %{parent_as: %{post: :id}}}}}}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{preload: :post})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{preload: [post: :user]})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, with_named_binding: %{post: %{id: %{==: 1}}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, where: %{with_named_binding: %{post: %{id: %{==: 1}}}}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{where: %{post_id: %{==: %{parent_as: %{post: :id}}}}})




  -----


  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{where: [%{post_id: %{==: %{parent_as: %{post: :id}}}}]})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: true})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: [:id, :body]})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: [map: [:id, :body]]})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: [struct: [:id, :body]]})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: %{map: [:id, :body]}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: %{struct: [:id, :body]}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: [:post_id], select_merge: [:id]})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{id: 1}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{id: %{>: 1}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{where: %{id: %{>: 1}}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{where: [%{id: %{>: 1}}, %{id: %{<: 3}}]}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{or_where: %{id: %{>: 1}}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{or_where: [%{id: %{>: 1}}, %{id: %{<: 3}}]}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: :comments}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: [:comments, :user]}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{qualifier: :right}}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{on: %{id: 2}}}}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{id: 2}}}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{on: %{id: %{==: 3}}}}}})
  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, join: %{association: %{comments: %{on: %{id: %{==: %{parent_as: %{post: :id}}}}}}}})

  EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :comment, title: %{==: %{parent_as: %{comment: %{lower: :title}}}}})
  """

  alias EctoShorts.CommonFilters.Common
  alias EctoShorts.CommonFilters.Schema
  alias EctoShorts.CommonSchemas
  alias EctoShorts.QueryBuilder

  @behaviour EctoShorts.QueryBuilder

  @common_filters Common.filters()

  @default_opts [query_builder_adapter: __MODULE__]

  def convert_params_to_filter(query, params) do
    convert_params_to_filter(query, params, @default_opts)
  end

  @doc """
  ...
  """
  def convert_params_to_filter(query, [], _opts) do
    query
  end

  def convert_params_to_filter(query, params, opts) do
    convert_params_to_filter(
      query,
      CommonSchemas.get_schema_queryable(query),
      params,
      opts
    )
  end

  @doc """
  ...
  """
  def convert_params_to_filter(query, schema_module, params, opts) when is_map(params) do
    convert_params_to_filter(query, schema_module, Map.to_list(params), opts)
  end

  def convert_params_to_filter(query, schema_module, params, opts) do
    params
    |> ensure_last_is_final_filter()
    |> Enum.reduce(query, fn {key, value}, query ->
      QueryBuilder.build_query(query, schema_module, key, value, nil, opts)
    end)
  end

  @impl EctoShorts.QueryBuilder
  @doc """
  ...
  """
  def build_query(query, schema_module, key, value, current_binding)
      when key in @common_filters do
    Common.build_query(query, schema_module, key, value, current_binding)
  end

  def build_query(query, schema_module, key, value, current_binding) do
    Schema.build_query(query, schema_module, key, value, current_binding)
  end

  defp ensure_last_is_final_filter(params) do
    if Keyword.has_key?(params, :last) do
      params
      |> Keyword.delete(:last)
      |> Kernel.++(last: params[:last])
    else
      params
    end
  end
end
