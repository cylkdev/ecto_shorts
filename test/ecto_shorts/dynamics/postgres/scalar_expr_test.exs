defmodule EctoShorts.Dynamics.Postgres.ScalarExprTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.Dynamics.Postgres.ScalarExpr
  alias EctoShorts.Schema.Comment
  alias EctoShorts.Schema.Post

  import Ecto.Query

  test "dynamic_expr/4 builds a root named-binding equality expression from a plain scalar term" do
    expected = dynamic([q], field(q, :id) == ^1)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, nil, 1, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding nil expression from a plain scalar term" do
    expected = dynamic([q], is_nil(field(q, :id)))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, nil, nil, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 keeps the schema field key dynamic" do
    expected = dynamic([q], field(q, :title) == ^"hello")
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, nil, "hello", [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding equality expression" do
    expected = dynamic([q], field(q, :id) == ^1)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, nil, {:eq, 1}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding equality expression from :==" do
    expected = dynamic([q], field(q, :id) == ^1)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, nil, {:==, 1}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding nil expression from :eq" do
    expected = dynamic([q], is_nil(field(q, :id)))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, nil, {:eq, nil}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding nil expression from :==" do
    expected = dynamic([q], is_nil(field(q, :id)))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, nil, {:==, nil}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding not-nil expression from :!=" do
    expected = dynamic([q], not is_nil(field(q, :published_at)))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :published_at, nil, {:!=, nil}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding not-nil expression from :ne" do
    expected = dynamic([q], not is_nil(field(q, :published_at)))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :published_at, nil, {:ne, nil}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding inequality expression from :!=" do
    expected = dynamic([q], field(q, :views) != ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:!=, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding inequality expression from :ne" do
    expected = dynamic([q], field(q, :views) != ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:ne, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding greater-than expression" do
    expected = dynamic([q], field(q, :views) > ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:>, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding greater-than expression from an explicit wrapped arithmetic value" do
    expected = dynamic([q], field(q, :views) > field(q, :views) + ^10)

    actual =
      ScalarExpr.dynamic_expr(
        {:as, nil},
        :views,
        nil,
        {:>, {:value, {:+, {{:field, :views}, {:value, 10}}}}},
        []
      )

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding greater-than-or-equal expression" do
    expected = dynamic([q], field(q, :views) >= ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:>=, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding less-than expression" do
    expected = dynamic([q], field(q, :views) < ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:<, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding less-than-or-equal expression" do
    expected = dynamic([q], field(q, :views) <= ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:<=, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding greater-than expression from :gt" do
    expected = dynamic([q], field(q, :views) > ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:gt, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding greater-than-or-equal expression from :gte" do
    expected = dynamic([q], field(q, :views) >= ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:gte, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding less-than expression from :lt" do
    expected = dynamic([q], field(q, :views) < ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:lt, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding less-than-or-equal expression from :lte" do
    expected = dynamic([q], field(q, :views) <= ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:lte, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding membership expression from :in" do
    expected = dynamic([q], field(q, :id) in ^[1, 2, 3])
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, nil, {:in, [1, 2, 3]}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 treats a list value with :== as membership" do
    expected = dynamic([q], field(q, :published) in ^[true, false])
    actual = ScalarExpr.dynamic_expr({:as, nil}, :published, nil, {:==, [true, false]}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 treats a list value with :!= as negated membership" do
    expected = dynamic([q], field(q, :published) not in ^[true, false])
    actual = ScalarExpr.dynamic_expr({:as, nil}, :published, nil, {:!=, [true, false]}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 preserves struct values like DateTime in comparisons" do
    dt = ~U[2026-01-01 00:00:00Z]
    expected = dynamic([q], field(q, :published_at) >= ^dt)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :published_at, nil, {:>=, dt}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding aggregate comparison expression" do
    expected = dynamic([q], avg(field(q, :views)) > ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, nil, {:avg, {:>, 10}}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding negated aggregate comparison expression" do
    expected = dynamic([q], not (avg(field(q, :views)) > ^10))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, :not, {:avg, {:>, 10}}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding quantified equality expression" do
    subquery_expr =
      from(c in Comment,
        where: c.published == ^true,
        select: c.id
      )

    expected = dynamic([q], field(q, :id) == all(subquery_expr))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, nil, {:==, {:all, subquery_expr}}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding negated quantified equality expression" do
    subquery_expr =
      from(c in Comment,
        where: c.published == ^true,
        select: c.id
      )

    expected = dynamic([q], not (field(q, :id) == all(subquery_expr)))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :id, :not, {:==, {:all, subquery_expr}}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding like expression" do
    expected = dynamic([q], like(field(q, :title), ^"%hello%"))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, nil, {:like, "hello"}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding ilike expression" do
    expected = dynamic([q], ilike(field(q, :title), ^"%hello%"))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, nil, {:ilike, "hello"}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding like-any expression" do
    patterns = ["%hello%", "%world%"]

    expected =
      dynamic([q], fragment("? LIKE ANY(?)", field(q, :title), ^patterns))

    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, nil, {:like, ["hello", "world"]}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding ilike-any expression" do
    patterns = ["%hello%", "%world%"]

    expected =
      dynamic([q], fragment("? ILIKE ANY(?)", field(q, :title), ^patterns))

    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, nil, {:ilike, ["hello", "world"]}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding lower transform equality expression" do
    expected = dynamic([q], fragment("lower(?)", field(q, :title)) == ^"hello")
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, nil, {:==, {:lower, "hello"}}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding upper transform equality expression" do
    expected = dynamic([q], fragment("upper(?)", field(q, :title)) == ^"HELLO")
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, nil, {:==, {:upper, "HELLO"}}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding lower transform inequality expression" do
    expected = dynamic([q], fragment("lower(?)", field(q, :title)) != ^"hello")
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, nil, {:!=, {:lower, "hello"}}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding upper transform inequality expression" do
    expected = dynamic([q], fragment("upper(?)", field(q, :title)) != ^"HELLO")
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, nil, {:!=, {:upper, "HELLO"}}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding negated membership expression" do
    expected = dynamic([q], field(q, :published) not in ^[true, false])
    actual = ScalarExpr.dynamic_expr({:as, nil}, :published, :not, {:in, [true, false]}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding negated greater-than expression" do
    expected = dynamic([q], not (field(q, :views) > ^10))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, :not, {:>, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding negated equality expression" do
    expected = dynamic([q], field(q, :views) != ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, :not, {:==, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding double-negated inequality expression" do
    expected = dynamic([q], field(q, :views) == ^10)
    actual = ScalarExpr.dynamic_expr({:as, nil}, :views, :not, {:!=, 10}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding negated like expression" do
    expected = dynamic([q], not like(field(q, :title), ^"%hello%"))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, :not, {:like, "hello"}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding negated like-any expression" do
    patterns = ["%hello%", "%world%"]

    expected = dynamic([q], not fragment("? LIKE ANY(?)", field(q, :title), ^patterns))
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, :not, {:like, ["hello", "world"]}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a root named-binding negated lower transform expression" do
    expected = dynamic([q], fragment("lower(?)", field(q, :title)) != ^"hello")
    actual = ScalarExpr.dynamic_expr({:as, nil}, :title, :not, {:==, {:lower, "hello"}}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias expression" do
    id = 1
    expected = from(p in Post, as: :post, where: p.id == ^id)

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :id, nil, {:eq, id}, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias expression from a plain scalar term" do
    id = 1
    expected = from(p in Post, as: :post, where: p.id == ^id)

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :id, nil, id, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias expression from :==" do
    id = 1
    expected = from(p in Post, as: :post, where: p.id == ^id)

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :id, nil, {:==, id}, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias nil expression from a plain scalar term" do
    expected = from(p in Post, as: :post, where: is_nil(p.id))

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :id, nil, nil, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias nil expression" do
    expected = from(p in Post, as: :post, where: is_nil(p.id))

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :id, nil, {:eq, nil}, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias nil expression from :==" do
    expected = from(p in Post, as: :post, where: is_nil(p.id))

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :id, nil, {:==, nil}, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a named-binding alias greater-than expression" do
    expected = from(p in Post, as: :post, where: p.views > ^10)

    actual =
      from(p in Post,
        as: :post,
        where: ^ScalarExpr.dynamic_expr({:as, :post}, :views, nil, {:>, 10}, [])
      )

    assert_sql(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding expression" do
    id = 1
    expected = dynamic([_, q], q.id == ^id)
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, nil, {:eq, id}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding expression from a plain scalar term" do
    id = 1
    expected = dynamic([_, q], q.id == ^id)
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, nil, id, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding expression from :==" do
    id = 1
    expected = dynamic([_, q], q.id == ^id)
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, nil, {:==, id}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding nil expression from a plain scalar term" do
    expected = dynamic([_, q], is_nil(q.id))
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, nil, nil, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding nil expression" do
    expected = dynamic([_, q], is_nil(q.id))
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, nil, {:eq, nil}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding nil expression from :==" do
    expected = dynamic([_, q], is_nil(q.id))
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, nil, {:==, nil}, [])

    assert_dynamic(expected, actual)
  end

  test "dynamic_expr/4 builds a positional-binding membership expression" do
    expected = dynamic([_, q], q.id in ^[1, 2, 3])
    actual = ScalarExpr.dynamic_expr({:at, 2}, :id, nil, {:in, [1, 2, 3]}, [])

    assert_dynamic(expected, actual)
  end
end
