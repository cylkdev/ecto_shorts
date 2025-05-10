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

  This api allows you to:

    * Add filters based on a map of parameters (like controller input)

    * Support flexible operators (`:==`, `:!=`, `:<`, `:ilike`, etc.)

    * Match values inside Postgres array fields (e.g., `tags`)

    * Compose multiple filters using `where` and `or_where`

  You get all this without needing to use `Ecto.Query` macros or worry
  about query bindings.

  ## Example

  Here's a query that filters users who are active and over 30:

      filters = %{active: true, age: {:>=, 30}}

      User
      |> EctoShorts.DynamicBuilders.Postgres.dynamic(:user, filters)

  You can also use `or_where/3` to combine filters with `OR` logic:

      EctoShorts.DynamicBuilders.Postgres.or_dynamic(User, :user, [
        %{status: "archived"},
        %{status: "disabled"}
      ])

  ## How It Works

  This module decides what kind of filter to apply based on the field’s type:

    * If the field is an array, it uses Postgres array operators (like `ILIKE ANY`).

    * If the field is a regular value, it uses standard comparisons or pattern matches.

  It delegates the actual filtering to these helpers:

    * `Postgres.Array` – For filters on array fields

    * `Postgres.Field` – For filters on regular (scalar) fields

  In most cases, you won’t need to think about these directly and just
  use the `where/3` and `or_where/3` functions and pass in your data.
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
  ## Examples

      # create a dynamic expression
      iex> EctoShorts.DynamicBuilders.Postgres.build_dynamic(nil, nil, :and, EctoShorts.Schema.Post, :id, 2)
      dynamic([q], q.id == ^2)

      # create a dynamic expression using an operator
      iex> EctoShorts.DynamicBuilders.Postgres.build_dynamic(nil, nil, :and, EctoShorts.Schema.Post, :id, {:>=, 2})
      dynamic([q], q.id >= ^2)

      # add a new expression using an OR condition
      iex> dyn_a = EctoShorts.DynamicBuilders.Postgres.build_dynamic(nil, nil, :and, EctoShorts.Schema.Post, :id, 2)
      ...> EctoShorts.DynamicBuilders.Postgres.build_dynamic(dyn_a, nil, :or, EctoShorts.Schema.Post, :id, {:>=, 2})
      dynamic([q], q.id == ^2 or q.id >= ^2)
  """
  @spec build_dynamic(
          schema_module(),
          maybe_dynamic_expr(),
          binding_alias(),
          condition(),
          key(),
          value()
        ) :: dynamic_expr()
  def build_dynamic(
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
          Array.dynamic(binding_alias, key, operator, value)
        )

      SchemaHelpers.field_type_of_array?(schema_module, key) ->
        CommonQueryAPI.merge_dynamic(
          dyn,
          condition,
          Array.dynamic(binding_alias, value, operator, key)
        )

      true ->
        CommonQueryAPI.merge_dynamic(
          dyn,
          condition,
          Field.dynamic(binding_alias, key, operator, value)
        )
    end
  end

  def build_dynamic(
        dyn,
        binding_alias,
        condition,
        schema_module,
        key,
        value
      ) do
    build_dynamic(
      dyn,
      binding_alias,
      condition,
      schema_module,
      key,
      {:==, value}
    )
  end
end
