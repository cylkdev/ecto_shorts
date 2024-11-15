defmodule EctoShorts.QueryBuilders.CommonQueryBuilder do
  @moduledoc """
  An implementation for `EctoShorts.QueryBuilder`.

  Customizes the data returned by a query, either by
  transforming it or applying specific limits.

  ## Filters

  The filters are:

  | Ecto filters    | EctoShorts filters |
  |-----------------|--------------------|
  | `distinct`      | `after`            |
  | `except`        | `before`           |
  | `except_all`    | `end_date`         |
  | `group_by`      | `first`            |
  | `intersect`     | `ids`              |
  | `intersect_all` | `last`             |
  | `limit`         | `search`           |
  | `offset`        | `start_date`       |
  | `order_by`      |                    |
  | `preload`       |                    |
  | `reverse_order` |                    |
  | `select`        |                    |
  | `select_merge`  |                    |
  | `union`         |                    |
  | `union_all`     |                    |

  See the ecto [documentation](https://hexdocs.pm/ecto/Ecto.Query.html) for more information.
  """
  alias Ecto.Query
  alias EctoShorts.CommonSchemas

  require Ecto.Query

  @type query :: Ecto.Query.t()
  @type filter ::
          :after
          | :before
          | :distinct
          | :end_date
          | :except
          | :except_all
          | :group_by
          | :first
          | :ids
          | :intersect
          | :intersect_all
          | :last
          | :limit
          | :offset
          | :order_by
          | :preload
          | :reverse_order
          | :search
          | :select
          | :select_merge
          | :start_date
          | :union
          | :union_all

  @logger_prefix "EctoShorts.QueryBuilders.CommonQueryBuilder"

  @filters ~w(
    after
    before
    distinct
    end_date
    except
    except_all
    first
    group_by
    ids
    intersect
    intersect_all
    last
    limit
    offset
    order_by
    preload
    reverse_order
    search
    select
    select_merge
    start_date
    union
    union_all
  )a

  @doc """
  Returns the list of supported filters.

  ### Examples

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filters()
      [
        :after,
        :before,
        :distinct,
        :end_date,
        :except,
        :except_all,
        :first,
        :group_by,
        :ids,
        :intersect,
        :intersect_all,
        :last,
        :limit,
        :offset,
        :order_by,
        :preload,
        :reverse_order,
        :search,
        :select,
        :select_merge,
        :start_date,
        :union,
        :union_all
      ]
  """
  @spec filters :: list(filter())
  def filters, do: @filters

  @doc """
  Returns `true` if the key is a supported filter otherwise `false`.

  ### Examples

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:after)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:before)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:distinct)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:end_date)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:except)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:except_all)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:first)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:group_by)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:ids)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:intersect)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:intersect_all)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:last)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:limit)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:offset)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:order_by)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:preload)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:reverse_order)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:search)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:select)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:select_merge)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:start_date)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:union)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:union_all)
      true

      iex> EctoShorts.QueryBuilders.CommonQueryBuilder.filter?(:invalid_key)
      false
  """
  @spec filter?(key :: filter() | term()) :: true | false
  def filter?(:after), do: true
  def filter?(:before), do: true
  def filter?(:distinct), do: true
  def filter?(:end_date), do: true
  def filter?(:except), do: true
  def filter?(:except_all), do: true
  def filter?(:first), do: true
  def filter?(:group_by), do: true
  def filter?(:ids), do: true
  def filter?(:intersect), do: true
  def filter?(:intersect_all), do: true
  def filter?(:last), do: true
  def filter?(:limit), do: true
  def filter?(:offset), do: true
  def filter?(:order_by), do: true
  def filter?(:preload), do: true
  def filter?(:reverse_order), do: true
  def filter?(:search), do: true
  def filter?(:select), do: true
  def filter?(:select_merge), do: true
  def filter?(:start_date), do: true
  def filter?(:union), do: true
  def filter?(:union_all), do: true
  def filter?(_), do: false

  @doc """
  Adds one or more expressions to a query.

  See the ecto [documentation](https://hexdocs.pm/ecto/Ecto.Query.html) for more information on each filter.

  Supports the following filters:

  **`distinct`**

  Selects only distinct values based on fields.

  Examples:

  ```elixir
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :distinct, true)
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, distinct: true>

  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :distinct, :id)
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, distinct: [asc: p0.id]>

  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :distinct, [:id, :title])
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, distinct: [asc: p0.id, asc: p0.title]>

  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :distinct, %{desc: :id})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, distinct: [desc: p0.id]>

  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :distinct, [desc: :id, desc: :title])
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, distinct: [desc: p0.id, desc: p0.title]>
  ```

  **`limit`**

  Loads associated records into a query.

  Examples:

  ```elixir
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :limit, 123_456)
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, limit: ^123_456>
  ```

  **`offset`**

  Loads associated records into a query.

  Examples:

  ```elixir
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :offset, 123_456)
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, offset: ^123_456>
  ```

  **`preload`**

  Loads associated records into a query.

  Examples:

  ```elixir
  # preload one association
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :preload, :comments)
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, preload: [:comments]>

  # preload many associations
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :preload, [:comments])
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, preload: [:comments]>
  ```

  **`select`**

  Specifies the fields and transformations on data.

  Examples:

  ```elixir
  # select fields and return a struct
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :select, [:title])
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: [:title]>

  # select fields from an association and return structs
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :select, [:id, :title, comments: [:id, :body, :post_id]])
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: [:id, :title, comments: [:id, :body, :post_id]]>

  # select fields and return a struct
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :select, %{struct: [:title]})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: struct(p0, [:title])>

  # select fields and return a map
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :select, %{map: [:title]})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: map(p0, [:title])>

  # select fields and return a struct
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :select, {:struct, [:title]})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: struct(p0, [:title])>

  # select fields and return a map
  iex> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(EctoShorts.Schemas.Post, :select, {:map, [:title]})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: map(p0, [:title])>
  ```

  **`select_merge`**

  Specifies the fields and transformations on data.

  Examples:

  ```elixir
  # select fields and return a map
  iex> EctoShorts.Schemas.Post
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select, %{map: []})
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select_merge, %{map: [:likes]})
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select_merge, %{map: [:title]})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: map(p0, [:likes, :title])>

  # add multiple select merge expressions
  iex> EctoShorts.Schemas.Post
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select, %{map: []})
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select_merge, [map: [:likes], map: [:title]])
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: map(p0, [:likes, :title])>

  # select fields and return a struct
  iex> EctoShorts.Schemas.Post
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select, %{struct: []})
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select_merge, %{struct: [:likes]})
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select_merge, %{struct: [:title]})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: struct(p0, [:likes, :title])>

  # select fields and return a struct
  iex> EctoShorts.Schemas.Post
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select, %{struct: []})
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select_merge, {:struct, [:likes]})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: struct(p0, [:likes])>

  # select fields and return a map
  iex> EctoShorts.Schemas.Post
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select, %{map: []})
  ...> |> EctoShorts.QueryBuilders.CommonQueryBuilder.build_query(:select_merge, {:map, [:likes]})
  #Ecto.Query<from p0 in EctoShorts.Schemas.Post, select: map(p0, [:likes])>
  ```
  """
  @spec build_query(query :: query(), filter :: filter(), expr :: term()) :: query()
  def build_query(query, :ids, ids) do
    Query.where(query, [x], x.id in ^ids)
  end

  def build_query(query, :after, id) do
    Query.where(query, [x], x.id > ^id)
  end

  def build_query(query, :before, id) do
    Query.where(query, [x], x.id < ^id)
  end

  def build_query(query, :start_date, date_time) do
    Query.where(query, [x], x.inserted_at >= ^date_time)
  end

  def build_query(query, :end_date, date_time) do
    Query.where(query, [x], x.inserted_at <= ^date_time)
  end

  def build_query(query, :first, value) do
    Query.limit(query, ^value)
  end

  def build_query(query, :last, value) do
    query
    |> Query.exclude(:order_by)
    |> Query.from(order_by: [desc: :inserted_at], limit: ^value)
    |> Query.subquery()
    |> Query.order_by(:id)
  end

  def build_query(query, :search, value) do
    queryable = CommonSchemas.get_schema_queryable(query)

    if function_exported?(queryable, :by_search, 2) do
      queryable.by_search(query, value)
    else
      EctoShorts.Utils.Logger.warning(
        @logger_prefix,
        """
        Filter `:search` is set and the module #{inspect(queryable)}
        does not define the function `by_search/2`.

        To fix this define the `by_search/2` callback function in
        the schema module:

        ```
        defmodule #{inspect(queryable)} do
          use Ecto.Schema
          # ...

          def by_search(query, params) do
            # Your code goes here
          end
        end
        ```
        """
      )

      query
    end
  end

  def build_query(query, :distinct, expr) do
    distinct_query(query, expr)
  end

  def build_query(query, :except, other_query) do
    Query.except(query, ^other_query)
  end

  def build_query(query, :except_all, other_query) do
    Query.except_all(query, ^other_query)
  end

  def build_query(query, :group_by, fields) do
    Query.group_by(query, ^fields)
  end

  def build_query(query, :intersect, other_query) do
    Query.union(query, ^other_query)
  end

  def build_query(query, :intersect_all, other_query) do
    Query.union_all(query, ^other_query)
  end

  def build_query(query, :limit, expr) do
    Query.limit(query, ^expr)
  end

  def build_query(query, :offset, expr) do
    Query.offset(query, ^expr)
  end

  def build_query(query, :order_by, expr) do
    order_by_query(query, expr)
  end

  def build_query(query, :preload, expr) do
    Query.preload(query, [x], ^expr)
  end

  def build_query(query, :reverse_order, reverse_order?) do
    if reverse_order? do
      Query.reverse_order(query)
    else
      query
    end
  end

  def build_query(query, :select, expr) do
    select_query(query, expr)
  end

  def build_query(query, :select_merge, expr) do
    select_merge_query(query, expr)
  end

  def build_query(query, :union, other_query) do
    Query.union(query, ^other_query)
  end

  def build_query(query, :union_all, other_query) do
    Query.union_all(query, ^other_query)
  end

  # distinct

  defp distinct_query(query, list) when is_list(list) do
    if Keyword.keyword?(list) do
      Query.distinct(query, [], ^dynamic_order_fields(list))
    else
      Query.distinct(query, [], ^list)
    end
  end

  defp distinct_query(query, map) when is_map(map) do
    Query.distinct(query, [], ^dynamic_order_fields(map))
  end

  defp distinct_query(query, expr) do
    Query.distinct(query, [], ^expr)
  end

  defp dynamic_order_fields(enum), do: Enum.map(enum, &dynamic_order_field/1)

  defp dynamic_order_field({order, field_name}) when order in [:asc, :desc] do
    {order, Query.dynamic([x], field(x, ^field_name))}
  end

  # order_by

  defp order_by_query(query, fields) do
    if Keyword.keyword?(fields) do
      Enum.reduce(fields, query, &reduce_order_by_query/2)
    else
      Query.order_by(query, ^fields)
    end
  end

  defp reduce_order_by_query({order, field_name}, query) do
    Query.order_by(query, [], [{^order, ^field_name}])
  end

  # select

  defp select_query(query, {:map, fields}) do
    Query.select(query, [x], map(x, ^fields))
  end

  defp select_query(query, {:struct, fields}) do
    Query.select(query, [x], struct(x, ^fields))
  end

  defp select_query(query, fields) when is_list(fields) do
    if Keyword.keyword?(fields) do
      Enum.reduce(fields, query, &select_query(&2, &1))
    else
      Query.select(query, [x], ^fields)
    end
  end

  defp select_query(query, map) do
    Enum.reduce(map, query, &select_query(&2, &1))
  end

  # select_merge

  defp select_merge_query(query, {:map, fields}) do
    Query.select_merge(query, [x], map(x, ^fields))
  end

  defp select_merge_query(query, {:struct, fields}) do
    Query.select_merge(query, [x], struct(x, ^fields))
  end

  defp select_merge_query(query, fields) when is_list(fields) do
    if Keyword.keyword?(fields) do
      Enum.reduce(fields, query, &select_merge_query(&2, &1))
    else
      Query.select_merge(query, [x], ^fields)
    end
  end

  defp select_merge_query(query, enum) do
    Enum.reduce(enum, query, &select_merge_query(&2, &1))
  end
end
