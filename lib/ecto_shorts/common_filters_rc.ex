defmodule EctoShorts.CommonFiltersRc do
  @moduledoc """
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: 1})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: [1, 2, 3]})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{!=: 1}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{!=: [1, 2, 3]}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{!=: %{lower: "example"}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{!=: %{upper: "example"}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{==: 1}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{==: [1, 2, 3]}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{==: %{lower: "example"}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{==: %{upper: "example"}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{>: 1}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{<: 1}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{>=: 1}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{id: %{<=: 1}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{title: %{ilike: "example"}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{title: %{like: "example"}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{id: %{==: 1}}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{subquery: %{comments: %{id: %{==: 1}}}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, id: %{==: %{parent_as: %{post: :id}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, join: %{association: %{comments: %{post_id: %{==: %{parent_as: %{post: :id}}}}}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{subquery: %{from: EctoShorts.Support.Schemas.Comment, on: %{id: 1}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{subquery: %{from: EctoShorts.Support.Schemas.Comment, on: %{id: %{==: 1}}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, join: %{subquery: %{from: EctoShorts.Support.Schemas.Comment, on: %{post_id: %{==: %{parent_as: %{post: :id}}}}}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{preload: :post})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{preload: [post: :user]})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, with_named_binding: %{post: %{id: %{==: 1}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, where: %{with_named_binding: %{post: %{id: %{==: 1}}}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{where: %{post_id: %{==: %{parent_as: %{post: :id}}}}})




  -----


  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{where: [%{post_id: %{==: %{parent_as: %{post: :id}}}}]})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: true})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: [:id, :body]})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: [map: [:id, :body]]})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: [struct: [:id, :body]]})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: %{map: [:id, :body]}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: %{struct: [:id, :body]}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{select: [:post_id], select_merge: [:id]})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{id: 1}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{id: %{>: 1}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{where: %{id: %{>: 1}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{where: [%{id: %{>: 1}}, %{id: %{<: 3}}]}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{or_where: %{id: %{>: 1}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{comments: %{or_where: [%{id: %{>: 1}}, %{id: %{<: 3}}]}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: :comments}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: [:comments, :user]}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{qualifier: :right}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{on: %{id: 2}}}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{id: 2}}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{join: %{association: %{comments: %{on: %{id: %{==: 3}}}}}})
  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :post, join: %{association: %{comments: %{on: %{id: %{==: %{parent_as: %{post: :id}}}}}}}})

  EctoShorts.CommonFiltersRc.convert_params_to_filter(EctoShorts.Support.Schemas.Post, %{as: :comment, title: %{==: %{parent_as: %{comment: %{lower: :title}}}}})
  """
  alias EctoShorts.CommonSchemas

  alias EctoShorts.CommonFiltersRc.{
    Common,
    Schema
  }

  @common_filters Common.filters()

  @doc """
  ...
  """
  def convert_params_to_filter(query, params \\ %{})

  def convert_params_to_filter(query, params) when params === %{} do
    query
  end

  def convert_params_to_filter(query, params) do
    create_schema_filter(query, CommonSchemas.get_schema_queryable(query), params)
  end

  def create_schema_filter(query, schema_module, params) do
    Enum.reduce(params, query, fn {key, value}, query ->
      create_schema_filter(query, schema_module, key, value)
    end)
  end

  def create_schema_filter(query, schema_module, filter, value) when filter in @common_filters do
    Common.create_schema_filter(query, schema_module, filter, value, nil)
  end

  def create_schema_filter(query, schema_module, filter, value) do
    Schema.create_schema_filter(query, schema_module, filter, value, nil)
  end
end
