defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ScalarExprBuilderMigrationTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defmodule LegacyScalarExpr do
    @moduledoc false

    import Ecto.Query

    require EctoShorts.QueryBuilders.Postgres.Dynamics.ScalarExprBuilder

    EctoShorts.QueryBuilders.Postgres.Dynamics.ScalarExprBuilder.define_scalar_exprs(__MODULE__)
  end

  defmodule SpecScalarExpr do
    @moduledoc false

    import Ecto.Query

    require EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ScalarExprBuilder

    EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ScalarExprBuilder.define_exprs(__MODULE__)
  end

  test "migrated scalar builder matches legacy for base field ops" do
    # These operators are the simplest scalar cases.
    # They build Ecto expressions like `field(q, ^key) == ^value`.
    key = :age

    expected_eq = dynamic([q], field(q, ^key) == ^1)
    expected_gt = dynamic([q], field(q, ^key) > ^1)
    expected_in = dynamic([q], field(q, ^key) in ^[1, 2, 3])

    assert_dynamic(
      expected_eq,
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:==, 1})
    )

    assert_dynamic(
      expected_eq,
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:==, 1})
    )

    assert_dynamic(
      expected_gt,
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:>, 1})
    )

    assert_dynamic(
      expected_gt,
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:>, 1})
    )

    assert_dynamic(
      expected_in,
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:in, [1, 2, 3]})
    )

    assert_dynamic(
      expected_in,
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:in, [1, 2, 3]})
    )
  end

  test "migrated scalar builder matches legacy for alias operators" do
    # Legacy supports a friendly operator form:
    #
    #   {:gt, 1}  -> {:>, 1}
    #   {:gte, 1} -> {:>=, 1}
    #   {:lt, 1}  -> {:<, 1}
    #   {:lte, 1} -> {:<=, 1}
    #   {:eq, 1}  -> {:==, 1}
    #
    # The spec-driven builder must keep the same mapping.
    key = :age

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:gt, 1}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:gt, 1})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:eq, 1}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:eq, 1})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:gt, 1}}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:gt, 1}})
    )
  end

  test "migrated scalar builder matches legacy for nil equality" do
    # Nil comparisons are special because SQL `= NULL` is not true.
    # Both builders use `is_nil/1` and `not is_nil/1`.
    key = :deleted_at

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:eq, nil}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:eq, nil})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:!=, nil}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:!=, nil})
    )
  end

  test "migrated scalar builder matches legacy for list equality semantics" do
    # List inputs for `:==`/`:!=` are treated as IN / NOT IN.
    key = :status

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:==, ["a", "b"]}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:==, ["a", "b"]})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:!=, ["a", "b"]}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:!=, ["a", "b"]})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:==, ["a", "b"]}}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:==, ["a", "b"]}})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:!=, ["a", "b"]}}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:!=, ["a", "b"]}})
    )
  end

  test "migrated scalar builder matches legacy for :all list normalization" do
    # Legacy supports `{:in, {:all, values}}` and `{:not, {:in, {:all, values}}}`.
    # The spec-driven builder keeps the same normalization behavior.
    key = :tags

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:in, {:all, ["a", "b"]}}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:in, {:all, ["a", "b"]}})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:in, {:all, ["a", "b"]}}}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:in, {:all, ["a", "b"]}}})
    )
  end

  test "migrated scalar builder matches legacy for lower/upper transforms" do
    # These operators lower/upper the field before comparing.
    key = :email

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:lower, "a@example.com"}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:lower, "a@example.com"})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:upper, "A@EXAMPLE.COM"}}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:upper, "A@EXAMPLE.COM"}})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:!=, {:lower, "a@example.com"}}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:!=, {:lower, "a@example.com"}})
    )
  end

  test "migrated scalar builder matches legacy for like/ilike operators" do
    # LIKE and ILIKE are supported for a single value and a list of values.
    key = :title

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:like, "hello"}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:like, "hello"})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:ilike, "hello"}}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:ilike, "hello"}})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:like, ["hello", "world"]}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:like, ["hello", "world"]})
    )

    assert_dynamic(
      LegacyScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:like, ["hello", "world"]}}),
      SpecScalarExpr.dynamic_field_expr({:as, nil}, key, {:not, {:like, ["hello", "world"]}})
    )
  end
end
