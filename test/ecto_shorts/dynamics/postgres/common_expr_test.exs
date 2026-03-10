defmodule EctoShorts.Dynamics.Postgres.CommonExprTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.Dynamics.Postgres.CommonExpr
  alias EctoShorts.Schema.Post

  import Ecto.Query

  test "dynamic_expr/4 builds a root named-binding expression" do
    expected = dynamic([q], field(q, :id) in ^[1, 2])
    actual = CommonExpr.dynamic_expr({:as, nil}, :ids, [1, 2], nil, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias expression" do
    date = ~U[2026-03-09 02:04:01.573399Z]
    expected = from(p in Post, as: :post, where: p.inserted_at >= ^date)

    actual =
      from(p in Post, as: :post, where: ^CommonExpr.dynamic_expr({:as, :post}, :start_date, date, nil, []))

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding expression" do
    expected = dynamic([_, q], field(q, :id) < ^10)
    actual = CommonExpr.dynamic_expr({:at, 2}, :before, 10, nil, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 handles binding-selector calls directly" do
    expected = CommonExpr.dynamic_expr({:at, 2}, :before, 10, nil, [])
    actual = CommonExpr.dynamic_expr({:at, 2}, :before, 10, nil, [])

    assert_dynamic(expected, actual)
  end

  test "unknown keys return nil" do
    assert is_nil(CommonExpr.dynamic_expr({:as, nil}, :missing, 1, nil, []))
  end
end
