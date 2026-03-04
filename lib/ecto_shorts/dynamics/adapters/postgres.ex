defmodule EctoShorts.Dynamics.Adapters.Postgres do
  @moduledoc since: "3.0.0"
  @moduledoc """
  PostgreSQL-specific dynamic expression adapter.

  Use this adapter when working with PostgreSQL databases. It provides
  comprehensive support for PostgreSQL operators including array operations,
  scalar comparisons, pattern matching, and custom operators like `:ids`,
  `:before`, `:after`, and `:exists`.

  ## Architecture overview

  The adapter is split into three components:

  * **EctoShorts.Dynamics.Adapters.Postgres.CommonExpr** - Handles special operators
    (`:ids`, `:before`, `:after`,  `:start_date`, `:end_date`, `:exists`).

  * **EctoShorts.Dynamics.Adapters.Postgres.ArrayExpr** - Handles array field
    operations (membership, overlap, contains, pattern matching).

  * **EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr** - Handles scalar field
    operations (equality, comparison, pattern matching, ranges).

  ## How operators are resolved

  When the adapter is called, it decides what to do by checking a few things
  in the following order.

  1. It looks at the key and asks whether it is one of the operator
  keys listed in `@operators`. If it is, the adapter treats it as an
  operator and routes it to `CommonExpr`.

  2. If the key is not an operator, the adapter looks up the field in the
  schema using `CommonSchema.get_schema_reflection/3`. It uses that
  information to tell whether the field is an array field.

  3. If the field is an array, the adapter routes the work to `ArrayExpr`.

  4. If the field is not an array, the adapter routes the work to `ScalarExpr`.

  ### Routing flow diagram

      Filter key -> Is it a special operator?
      Yes -> CommonExpr
      No  -> Is it an array field?
              Yes -> ArrayExpr
              No  -> ScalarExpr

  ## Common operators

  The adapter supports these special operators:

  * `:ids` - filter by a list of IDs (generates `id IN (...)`)
  * `:before` - filter by datetime before a value (generates `field < value`)
  * `:after` - filter by datetime after a value (generates `field > value`)
  * `:start_date` - filter by date range start
  * `:end_date` - filter by date range end
  * `:exists` - filter by subquery existence

  ### Operator examples

  **Filter by IDs:**

      EctoShorts.CommonFilters.convert_params_to_filter(Post, %{ids: [1, 2, 3]})
      # WHERE id IN (1, 2, 3)

  **Filter by date range:**

      EctoShorts.CommonFilters.convert_params_to_filter(Post, %{before: ~U[2024-01-01 00:00:00Z]})
      # WHERE inserted_at < '2024-01-01 00:00:00Z'

  **Filter by existence:**

      EctoShorts.CommonFilters.convert_params_to_filter(Post, %{exists: %{source: Comment, where: %{approved: true}}})
      # WHERE EXISTS (SELECT 1 FROM comments WHERE approved = TRUE)

  ## Array vs scalar expressions

  The adapter automatically detects array fields and routes them to the
  appropriate component.

  ### Array field operations

  Array fields support these operations:

  * **Membership** - check if value is in array (`:in` operator)
  * **Overlap** - check if arrays overlap (`:overlap` operator)
  * **Contains** - check if array contains values (`:contains` operator)
  * **Pattern matching** - LIKE/ILIKE on array elements

  Example:

      # Schema with array field
      schema "posts" do
        field :tags, {:array, :string}
      end

      # Filter by array membership
      EctoShorts.CommonFilters.convert_params_to_filter(Post, %{tags: %{in: "elixir"}})
      # WHERE 'elixir' = ANY(tags)

  ### Scalar field operations

  Scalar fields support these operations:

  * **Equality** - `==`, `!=`
  * **Comparison** - `>`, `<`, `>=`, `<=`
  * **Pattern matching** - `:like`, `:ilike`
  * **List membership** - `:in`, `:not_in`
  * **Range** - `:between`

  Example:

      # Filter by comparison
      EctoShorts.CommonFilters.convert_params_to_filter(Post, %{views: %{>: 100, <: 1000}})
      # WHERE views > 100 AND views < 1000

  ## Component details

  ### CommonExpr

  Handles special operators that do not map directly to schema fields:

      # :ids operator
      %{ids: [1, 2, 3]}
      # Generates: WHERE id IN (1, 2, 3)

      # :before operator
      %{before: ~U[2024-01-01 00:00:00Z]}
      # Generates: WHERE inserted_at < '2024-01-01 00:00:00Z'

  ### ArrayExpr

  Handles PostgreSQL array operations:

      # Array membership
      %{tags: %{in: "elixir"}}
      # Generates: WHERE 'elixir' = ANY(tags)

      # Array overlap
      %{tags: %{overlap: ["elixir", "phoenix"]}}
      # Generates: WHERE tags && ARRAY['elixir', 'phoenix']

      # Array contains
      %{tags: %{contains: ["elixir", "phoenix"]}}
      # Generates: WHERE tags @> ARRAY['elixir', 'phoenix']

  ### ScalarExpr

  Handles standard field operations:

      # Equality
      %{title: "Hello"}
      # Generates: WHERE title = 'Hello'

      # Comparison
      %{views: %{>: 100}}
      # Generates: WHERE views > 100

      # Pattern matching
      %{title: %{like: "Hello"}}
      # Generates: WHERE title LIKE '%Hello%'

  ## Extending the adapter

  To add custom operators, create a new adapter that delegates to the
  PostgreSQL adapter for standard operations:

      defmodule MyApp.CustomAdapter do
        @behaviour EctoShorts.Dynamics.Adapter

        alias EctoShorts.Dynamics.Adapters.Postgres

        @operators [:custom_op | Postgres.operators()]

        @impl true
        def operators, do: @operators

        @impl true
        def build_dynamic(source, binding_selector, :custom_op, expr) do
          # Handle custom operator
          import Ecto.Query, only: [dynamic: 2]
          dynamic([{^binding_selector, r}], fragment("custom_function(?)", ^expr))
        end

        def build_dynamic(source, binding_selector, key, expr) do
          # Delegate to PostgreSQL adapter
          Postgres.build_dynamic(source, binding_selector, key, expr)
        end
      end

  ## Custom operator examples

  ### Example 1: Full-text search

      defmodule MyApp.SearchAdapter do
        @behaviour EctoShorts.Dynamics.Adapter

        import Ecto.Query, only: [dynamic: 2]

        @operators [:search | EctoShorts.Dynamics.Adapters.Postgres.operators()]

        @impl true
        def operators, do: @operators

        @impl true
        def build_dynamic(_source, binding_selector, :search, query) do
          dynamic([{^binding_selector, r}],
            fragment(
              "to_tsvector('english', ? || ' ' || ?) @@ plainto_tsquery(?)",
              r.title,
              r.body,
              ^query
            )
          )
        end

        def build_dynamic(source, binding_selector, key, expr) do
          EctoShorts.Dynamics.Adapters.Postgres.build_dynamic(
            source,
            binding_selector,
            key,
            expr
          )
        end
      end

  ### Example 2: JSON operations

      defmodule MyApp.JsonAdapter do
        @behaviour EctoShorts.Dynamics.Adapter

        import Ecto.Query, only: [dynamic: 2]

        @operators [:json_contains | EctoShorts.Dynamics.Adapters.Postgres.operators()]

        @impl true
        def operators, do: @operators

        @impl true
        def build_dynamic(_source, binding_selector, :json_contains, {field, value}) do
          dynamic([{^binding_selector, r}],
            fragment("? @> ?::jsonb", field(r, ^field), ^Jason.encode!(value))
          )
        end

        def build_dynamic(source, binding_selector, key, expr) do
          EctoShorts.Dynamics.Adapters.Postgres.build_dynamic(
            source,
            binding_selector,
            key,
            expr
          )
        end
      end

  ## Troubleshooting

  **Problem:** Array operations not working.

  **Solution:** Verify the field is defined as `{:array, type}` in your schema.
  The adapter uses schema introspection to detect array fields.

  **Problem:** Special operator not recognized.

  **Solution:** Check that the operator is recogized by this adapter using the
  `operator?/1` function. Only operators in this list are routed to `CommonExpr`.

  **Problem:** Generated SQL is incorrect.

  **Solution:** Use `Ecto.Adapters.SQL.to_sql/3` to inspect the generated SQL.
  Verify that the filter parameters match the expected structure.

  See also `EctoShorts.Dynamics.Adapter`, `EctoShorts.Dynamics`,
  `EctoShorts.CommonFilters`, and `EctoShorts.CommonSchema`.
  """

  alias Ecto.Query
  alias EctoShorts.CommonSchema
  alias EctoShorts.Dynamics.Adapters.Postgres.{ArrayExpr, CommonExpr, ScalarExpr}

  require Ecto.Query

  @behaviour EctoShorts.Dynamics.Adapter

  @operators [:ids, :before, :after, :start_date, :end_date, :exists]
  @helper_operators [:datetime, :date]
  @alias_operators %{eq: :==, ne: :!=, gt: :>, gte: :>=, lt: :<, lte: :<=}

  @impl true
  def operators, do: @operators

  @impl true
  def operator?(key), do: key in @operators

  @impl true
  def build_dynamic(source, binding_selector, key, expr) do
    cond do
      key in @operators ->
        CommonExpr.apply_dynamic_expr(binding_selector, key, normalize_operator_expr(expr))

      match?({:array, _}, CommonSchema.get_schema_reflection(source, :type, key)) ->
        build_field_dynamic(ArrayExpr, binding_selector, key, expr)

      true ->
        build_field_dynamic(ScalarExpr, binding_selector, key, expr)
    end
  end

  defp build_field_dynamic(mod, binding_selector, key, expr) do
    expr
    |> normalize_field_expr()
    |> Enum.reduce(nil, fn item, acc ->
      case mod.apply_dynamic_expr(binding_selector, key, item) do
        nil -> acc
        dyn -> if acc, do: Query.dynamic([], ^acc and ^dyn), else: dyn
      end
    end)
  end

  defp normalize_field_expr(map) when is_map(map) and not is_struct(map) do
    Enum.flat_map(map, &normalize_entry/1)
  end

  defp normalize_field_expr(list) when is_list(list) do
    if Keyword.keyword?(list) do
      Enum.flat_map(list, &normalize_entry/1)
    else
      [{:==, list}]
    end
  end

  defp normalize_field_expr({op, value}) when is_atom(op) do
    normalize_entry({op, value})
  end

  defp normalize_field_expr(value), do: [{:==, value}]

  defp normalize_entry({op, value}) when op in @helper_operators do
    [{op, to_keyword_payload(value)}]
  end

  defp normalize_entry({op, value}) when is_map_key(@alias_operators, op) do
    normalize_entry({Map.fetch!(@alias_operators, op), value})
  end

  defp normalize_entry({op, value}) do
    case normalize_inner(value) do
      nil ->
        [{op, value}]

      inner_list ->
        Enum.map(inner_list, fn inner -> {op, inner} end)
    end
  end

  defp normalize_inner(map) when is_map(map) and not is_struct(map) do
    Enum.flat_map(map, &normalize_entry/1)
  end

  defp normalize_inner(list) when is_list(list) do
    if Keyword.keyword?(list), do: Enum.flat_map(list, &normalize_entry/1), else: nil
  end

  defp normalize_inner(_), do: nil

  defp to_keyword_payload(map) when is_map(map) and not is_struct(map) do
    Enum.map(map, fn {k, v} -> {k, v} end)
  end

  defp to_keyword_payload(list) when is_list(list), do: list
  defp to_keyword_payload(value), do: value

  defp normalize_operator_expr(map) when is_map(map) and not is_struct(map) do
    case Map.to_list(map) do
      [{k, v}] -> {k, normalize_operator_expr(v)}
      _other -> map
    end
  end

  defp normalize_operator_expr(list) when is_list(list) do
    if Keyword.keyword?(list) do
      case list do
        [{k, v}] -> {k, normalize_operator_expr(v)}
        _other -> list
      end
    else
      list
    end
  end

  defp normalize_operator_expr(value), do: value
end
