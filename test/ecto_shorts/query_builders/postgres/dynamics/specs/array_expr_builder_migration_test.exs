defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ArrayExprBuilderMigrationTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defmodule LegacyArrayExpr do
    @moduledoc false

    import Ecto.Query

    require EctoShorts.QueryBuilders.Postgres.Dynamics.ArrayExprBuilder

    EctoShorts.QueryBuilders.Postgres.Dynamics.ArrayExprBuilder.define_nil_comparisons(__MODULE__)
    EctoShorts.QueryBuilders.Postgres.Dynamics.ArrayExprBuilder.define_case_transforms(__MODULE__)
    EctoShorts.QueryBuilders.Postgres.Dynamics.ArrayExprBuilder.define_like_ilikes(__MODULE__)
    EctoShorts.QueryBuilders.Postgres.Dynamics.ArrayExprBuilder.define_base_ops(__MODULE__)
  end

  defmodule SpecArrayExpr do
    @moduledoc false

    import Ecto.Query

    require EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ArrayExprBuilder

    EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ArrayExprBuilder.define_exprs(__MODULE__)
  end

  test "migrated array builder matches legacy for nil comparisons" do
    # Nil comparisons are special because SQL `= NULL` is not true.
    key = :archived_at

    expected_is_nil = dynamic([q], is_nil(field(q, ^key)))
    expected_not_nil = dynamic([q], not is_nil(field(q, ^key)))

    assert_dynamic(
      expected_is_nil,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:eq, nil})
    )

    assert_dynamic(
      expected_is_nil,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:eq, nil})
    )

    assert_dynamic(
      expected_not_nil,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:!=, nil})
    )

    assert_dynamic(
      expected_not_nil,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:!=, nil})
    )
  end

  test "migrated array builder matches legacy for lower/upper transforms" do
    # These transforms search inside the array by unnesting.
    key = :tags

    expected_lower =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE lower(t) = ?
          )
          """,
          field(q, ^key),
          ^"elixir"
        )
      )

    assert_dynamic(
      expected_lower,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:lower, "elixir"})
    )

    assert_dynamic(
      expected_lower,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:lower, "elixir"})
    )

    expected_not_upper =
      dynamic(
        [q],
        fragment(
          """
          NOT EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE upper(t) = ?
          )
          """,
          field(q, ^key),
          ^"ELIXIR"
        )
      )

    assert_dynamic(
      expected_not_upper,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:not, {:upper, "ELIXIR"}})
    )

    assert_dynamic(
      expected_not_upper,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:not, {:upper, "ELIXIR"}})
    )
  end

  test "migrated array builder matches legacy for like/ilike operators" do
    # LIKE/ILIKE operators search inside the array by unnesting, and they accept
    # either a string or a list of strings.
    key = :tags

    patterns = ["%elixir%"]

    expected_like =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE t LIKE ANY (?)
          )
          """,
          field(q, ^key),
          ^patterns
        )
      )

    assert_dynamic(
      expected_like,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:like, "elixir"})
    )

    assert_dynamic(
      expected_like,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:like, "elixir"})
    )

    patterns2 = ["%elixir%", "%ecto%"]

    expected_not_ilike =
      dynamic(
        [q],
        fragment(
          """
          NOT EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE t ILIKE ANY (?)
          )
          """,
          field(q, ^key),
          ^patterns2
        )
      )

    assert_dynamic(
      expected_not_ilike,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:not, {:ilike, ["elixir", "ecto"]}})
    )

    assert_dynamic(
      expected_not_ilike,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:not, {:ilike, ["elixir", "ecto"]}})
    )
  end

  test "migrated array builder matches legacy for base operators" do
    key = :scores

    # Scalar membership in array.
    expected_in = dynamic([q], ^3 in field(q, ^key))

    assert_dynamic(
      expected_in,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:in, 3})
    )

    assert_dynamic(
      expected_in,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:in, 3})
    )

    # Array overlap / contains.
    expected_overlap = dynamic([q], fragment("? && ?", field(q, ^key), ^[1, 2]))
    expected_contains = dynamic([q], fragment("? @> ?", field(q, ^key), ^[1, 2]))

    assert_dynamic(
      expected_overlap,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:in, [1, 2]})
    )

    assert_dynamic(
      expected_overlap,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:in, [1, 2]})
    )

    assert_dynamic(
      expected_contains,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:in, {:all, [1, 2]}})
    )

    assert_dynamic(
      expected_contains,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:in, {:all, [1, 2]}})
    )

    # Any comparisons.
    expected_any_gt = dynamic([q], fragment("? < ANY(?)", ^10, field(q, ^key)))

    assert_dynamic(
      expected_any_gt,
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:>, 10})
    )

    assert_dynamic(
      expected_any_gt,
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:>, 10})
    )

    # Operator aliases map to the same operator set.
    assert_dynamic(
      LegacyArrayExpr.dynamic_field_expr({:as, nil}, key, {:gt, 10}),
      SpecArrayExpr.dynamic_field_expr({:as, nil}, key, {:gt, 10})
    )
  end
end
