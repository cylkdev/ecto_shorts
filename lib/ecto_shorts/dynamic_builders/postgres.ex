defmodule EctoShorts.DynamicBuilders.Postgres do
  @moduledoc since: "2.5.0"
  @moduledoc """
  Build dynamic `where` and `or_where` filters for Postgres without
  writing raw `Ecto.Query` expressions.

  This module makes it easier to add dynamic filters to your Ecto
  queries, especially when the filters come from user input or need
  to change at runtime.

  Instead of writing out each condition manually, you can pass in a
  map or keyword list of filters. It handles building the right
  expressions for each field based on its type—so you don’t need to
  worry about whether a field is a string, number, or array.

  ...
  """

  alias EctoShorts.{
    CommonQueryAPI,
    DynamicBuilders.Postgres.Array,
    DynamicBuilders.Postgres.Field,
    SchemaHelpers
  }

  @behaviour EctoShorts.DynamicBuilder

  @type schema_module :: Ecto.Queryable.t()
  @type schema_source :: binary()
  @type dynamic_expr :: %Ecto.Query.DynamicExpr{}
  @type maybe_dynamic_expr :: dynamic_expr() | nil
  @type binding_alias :: atom()

  @type condition :: :and | :or

  @type key :: atom()
  @type value :: any()
  @type operator :: any()
  @type params :: map()

  @impl EctoShorts.DynamicBuilder
  @doc """
  ...
  """
  @spec create_dynamic(
          schema_module(),
          maybe_dynamic_expr(),
          binding_alias() | nil,
          condition(),
          key(),
          value()
        ) :: dynamic_expr()
  def create_dynamic(
        schema_module,
        dyn,
        binding_alias,
        condition,
        key,
        {operator, value}
      ) do
    cond do
      SchemaHelpers.field_type_of_array?(schema_module, key) and is_list(value) ->
        CommonQueryAPI.merge_dynamic(
          dyn,
          condition,
          Array.create_dynamic(binding_alias, key, operator, value)
        )

      SchemaHelpers.field_type_of_array?(schema_module, key) ->
        CommonQueryAPI.merge_dynamic(
          dyn,
          condition,
          Array.create_dynamic(binding_alias, value, operator, key)
        )

      true ->
        CommonQueryAPI.merge_dynamic(
          dyn,
          condition,
          Field.create_dynamic(binding_alias, key, operator, value)
        )
    end
  end

  def create_dynamic(
        dyn,
        binding_alias,
        condition,
        schema_module,
        key,
        value
      ) do
    create_dynamic(
      dyn,
      binding_alias,
      condition,
      schema_module,
      key,
      {:==, value}
    )
  end
end
