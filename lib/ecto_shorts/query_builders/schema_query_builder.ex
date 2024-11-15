defmodule EctoShorts.QueryBuilders.SchemaQueryBuilder do
  @moduledoc """
  ### Positional Binding

  When creating a query we specify the query bindings on
  the left-hand side of `in` (eg. from `p in Post`). This
  is done inside `from` and `join` clauses. This means
  each time a `from` and `join` clause is added a new
  binding is added.

  This function adds a query expression to the `first`
  or `last` binding in the query which is referred to as
  the "binding position". When the binding position is
  `:first` the query expression is applied to the source
  given in the `from` otherwise when `:last` the query
  expression is applied to the last binding added to
  the query.

  Note: The `first` and `last` binding can refer to the
  source given in the `from` when a query is created
  with `from`.
  """
  alias Ecto.Query
  alias EctoShorts.CommonSchemas

  require Ecto.Query

  @logger_prefix "EctoShorts.QueryBuilders.SchemaQueryBuilder"

  @inner :inner

  # @join_qualifiers [
  #   @inner,
  #   :left,
  #   :right,
  #   :cross,
  #   :cross_lateral,
  #   :full,
  #   :inner_lateral,
  #   :left_lateral
  # ]

  @default_join_params [
    qualifier: @inner
  ]

  @doc """
  ...
  """
  def build_join_query(query, field_name, params, bind_pos) when is_map(params) do
    build_join_query(query, field_name, Map.to_list(params), bind_pos)
  end

  def build_join_query(query, field_name, params, bind_pos) do
    queryable = CommonSchemas.get_schema_queryable(query)

    params = Keyword.merge(@default_join_params, params)

    qual = params[:qualifier] || @inner
    prefix = params[:prefix]

    if field_name in queryable.__schema__(:associations) do
      assoc = queryable.__schema__(:association, field_name)

      assoc_owner_key = assoc.owner_key
      assoc_related_key = assoc.related_key

      case bind_pos do
        :first ->
          Query.join(
            query,
            qual,
            [owner, ..., _],
            assoc in assoc(owner, ^field_name),
            on: field(assoc, ^assoc_related_key) == field(owner, ^assoc_owner_key),
            prefix: ^prefix
          )

        :last ->
          Query.join(
            query,
            qual,
            [_, ..., owner],
            assoc in assoc(owner, ^field_name),
            on: field(assoc, ^assoc_related_key) == field(owner, ^assoc_owner_key),
            prefix: ^prefix
          )

        term ->
          raise_invalid_positional_binding!(term)

      end
    else
      EctoShorts.Utils.Logger.warning(
        @logger_prefix,
        "Field #{inspect(field_name)} is not an association on schema #{inspect(queryable)}."
      )

      query
    end
  end

  @doc """
  ...
  """
  def build_relational_query(query, field_name, params, bind_pos) when is_map(params) do
    build_relational_query(query, field_name, Map.to_list(params), bind_pos)
  end

  def build_relational_query(query, field_name, params, bind_pos) do
    join_params = Keyword.get(params, :join, [])

    query = build_join_query(query, field_name, join_params, bind_pos)

    params
    |> Keyword.delete(:join)
    |> Enum.reduce(query, fn {key, value}, query ->
      build_query(query, key, value, :last)
    end)
  end

  @doc """
  Adds `where`, `or_where`, and `join` expressions.
  """
  def build_query(query, key, params, bind_pos) when key in [:or, :or_where] do
    Enum.reduce(params, query, fn {field_name, value}, query ->
      if is_map(value) or is_list(value) do
        Enum.reduce(value, query, fn {op, value}, query ->
          or_where_query(query, field_name, op, value, bind_pos)
        end)
      else
        or_where_query(query, field_name, :==, value, bind_pos)
      end
    end)
  end

  def build_query(query, field_name, value, bind_pos) do
    queryable = CommonSchemas.get_schema_queryable(query)

    if field_name in queryable.__schema__(:associations) do
      build_relational_query(query, field_name, value, bind_pos)
    else
      if is_map(value) or is_list(value) do
        Enum.reduce(value, query, fn {op, value}, query ->
          where_query(query, field_name, op, value, bind_pos)
        end)
      else
        where_query(query, field_name, :==, value, bind_pos)
      end
    end
  end

  defp or_where_query(query, field_name, :>, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.or_where(query, [first, ..., _], field(first, ^field_name) > ^value)

      :last ->
        Query.or_where(query, [_, ..., last], field(last, ^field_name) > ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp or_where_query(query, field_name, :<, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.or_where(query, [first, ..., _], field(first, ^field_name) < ^value)

      :last ->
        Query.or_where(query, [_, ..., last], field(last, ^field_name) < ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp or_where_query(query, field_name, :>=, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.or_where(query, [first, ..., _], field(first, ^field_name) >= ^value)

      :last ->
        Query.or_where(query, [_, ..., last], field(last, ^field_name) >= ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp or_where_query(query, field_name, :<=, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.or_where(query, [first, ..., _], field(first, ^field_name) <= ^value)

      :last ->
        Query.or_where(query, [_, ..., last], field(last, ^field_name) <= ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp or_where_query(query, field_name, :==, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.or_where(query, [first, ..., _], field(first, ^field_name) == ^value)

      :last ->
        Query.or_where(query, [_, ..., last], field(last, ^field_name) == ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp where_query(query, field_name, :>, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.where(query, [first, ..., _], field(first, ^field_name) > ^value)

      :last ->
        Query.where(query, [_, ..., last], field(last, ^field_name) > ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp where_query(query, field_name, :<, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.where(query, [first, ..., _], field(first, ^field_name) < ^value)

      :last ->
        Query.where(query, [_, ..., last], field(last, ^field_name) < ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp where_query(query, field_name, :>=, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.where(query, [first, ..., _], field(first, ^field_name) >= ^value)

      :last ->
        Query.where(query, [_, ..., last], field(last, ^field_name) >= ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp where_query(query, field_name, :<=, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.where(query, [first, ..., _], field(first, ^field_name) <= ^value)

      :last ->
        Query.where(query, [_, ..., last], field(last, ^field_name) <= ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp where_query(query, field_name, :==, value, bind_pos) do
    case bind_pos do
      :first ->
        Query.where(query, [first, ..., _], field(first, ^field_name) == ^value)

      :last ->
        Query.where(query, [_, ..., last], field(last, ^field_name) == ^value)

      term ->
        raise_invalid_positional_binding!(term)

    end
  end

  defp raise_invalid_positional_binding!(term) do
    raise ArgumentError, """
    Invalid positional binding, expected the atom :first or :last.

    got:
    #{inspect(term)}
    """
  end
end
