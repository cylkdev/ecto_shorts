defmodule EctoShorts.DynamicExpression.Postgres do
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
      |> EctoShorts.DynamicExpression.Postgres.where(:user, filters)

  You can also use `or_where/3` to combine filters with `OR` logic:

      EctoShorts.DynamicExpression.Postgres.or_where(User, :user, [
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
    CommonQuery,
    DynamicExpression.Postgres.Array,
    DynamicExpression.Postgres.Field
  }

  @behaviour EctoShorts.DynamicExpression

  @type source :: binary()
  @type queryable :: Ecto.Queryable.t()
  @type binding_alias :: atom()
  @type params :: map()
  @type key :: atom()
  @type value :: any()
  @type operator :: any()
  @type dyn :: Ecto.Query.DynamicExpr.t()

  @impl EctoShorts.DynamicExpression
  @doc """
  ## Examples

      # create a dynamic expression
      iex> EctoShorts.DynamicExpression.Postgres.build_dynamic_expression(nil, nil, :and, EctoShorts.Schema.Post, :id, 2)
      dynamic([q], q.id == ^2)

      # create a dynamic expression using an operator
      iex> EctoShorts.DynamicExpression.Postgres.build_dynamic_expression(nil, nil, :and, EctoShorts.Schema.Post, :id, {:>=, 2})
      dynamic([q], q.id >= ^2)

      # add a new expression using an OR condition
      iex> dyn_a = EctoShorts.DynamicExpression.Postgres.build_dynamic_expression(nil, nil, :and, EctoShorts.Schema.Post, :id, 2)
      ...> EctoShorts.DynamicExpression.Postgres.build_dynamic_expression(dyn_a, nil, :or, EctoShorts.Schema.Post, :id, {:>=, 2})
      dynamic([q], q.id == ^2 or q.id >= ^2)
  """
  @spec build_dynamic_expression(
          dynamic :: dyn() | nil,
          current_binding :: binding_alias() | nil,
          condition :: :and | :or,
          schema_module :: queryable(),
          key :: key(),
          value :: value()
        ) :: dyn()
  def build_dynamic_expression(
        dyn,
        current_binding,
        condition,
        schema_module,
        key,
        {operator, value}
      ) do
    cond do
      field_type_of_array?(schema_module, key) and is_list(value) ->
        CommonQuery.merge_dynamic(
          dyn,
          condition,
          Array.where(current_binding, key, operator, value)
        )

      field_type_of_array?(schema_module, key) ->
        CommonQuery.merge_dynamic(
          dyn,
          condition,
          Array.where(current_binding, value, operator, key)
        )

      true ->
        CommonQuery.merge_dynamic(
          dyn,
          condition,
          Field.where(current_binding, key, operator, value)
        )
    end
  end

  def build_dynamic_expression(dyn, current_binding, condition, schema_module, key, value) do
    build_dynamic_expression(dyn, current_binding, condition, schema_module, key, {:==, value})
  end

  defp field_type_of_array?(schema_module, key) do
    case field_type(schema_module, key) do
      {:array, _} -> true
      _ -> false
    end
  end

  defp field_type(schema_module, key) do
    schema_module.__schema__(:type, key)
  end
end
