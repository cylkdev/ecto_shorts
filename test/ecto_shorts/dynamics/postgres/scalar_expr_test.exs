defmodule EctoShorts.Dynamics.Postgres.ScalarExprTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.Dynamics.Postgres.ScalarExpr
  alias EctoShorts.Schema.Post

  import Ecto.Query

  test "dynamic_expr/4 builds a root named-binding equality expression from a plain scalar term" do
    expected = dynamic([q], field(q, :id) == ^1)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, 1, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding nil expression from a plain scalar term" do
    expected = dynamic([q], is_nil(field(q, :id)))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, nil, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding equality expression" do
    expected = dynamic([q], field(q, :id) == ^1)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, {:eq, 1}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding equality expression from :==" do
    expected = dynamic([q], field(q, :id) == ^1)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, {:==, 1}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding nil expression from :eq" do
    expected = dynamic([q], is_nil(field(q, :id)))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, {:eq, nil}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding nil expression from :==" do
    expected = dynamic([q], is_nil(field(q, :id)))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, {:==, nil}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias expression" do
    id = 1
    expected = from(p in Post, as: :post, where: p.id == ^id)

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :id, {:eq, id}, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias expression from a plain scalar term" do
    id = 1
    expected = from(p in Post, as: :post, where: p.id == ^id)

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :id, id, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias nil expression" do
    expected = from(p in Post, as: :post, where: is_nil(p.id))

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :id, {:eq, nil}, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding expression" do
    id = 1
    expected = dynamic([_, q], q.id == ^id)
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, {:eq, id}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding expression from a plain scalar term" do
    id = 1
    expected = dynamic([_, q], q.id == ^id)
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, id, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding nil expression" do
    expected = dynamic([_, q], is_nil(q.id))
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, {:eq, nil}, [])

    assert_dynamic(expected, actual)
  end
end
