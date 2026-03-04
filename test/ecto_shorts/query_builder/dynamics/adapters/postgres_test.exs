defmodule EctoShorts.Dynamics.Adapters.PostgresTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  import Ecto.Query

  alias EctoShorts.Dynamics.Adapters.Postgres
  alias EctoShorts.Schema.Post

  describe "operators/0" do
    test "returns the list of special operators" do
      assert Postgres.operators() == [:ids, :before, :after, :start_date, :end_date, :exists]
    end
  end

  describe "operator?/1" do
    test "returns true for special operators" do
      assert Postgres.operator?(:ids)
      assert Postgres.operator?(:before)
      assert Postgres.operator?(:after)
      assert Postgres.operator?(:start_date)
      assert Postgres.operator?(:end_date)
      assert Postgres.operator?(:exists)
    end

    test "returns false for non-operator keys" do
      refute Postgres.operator?(:title)
      refute Postgres.operator?(:published)
      refute Postgres.operator?(:views)
      refute Postgres.operator?(:tags)
      refute Postgres.operator?(:unknown_key)
    end
  end

  describe "build_dynamic/4 common operators" do
    test ":ids operator" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :ids, [1, 2])
      expected = dynamic([q], field(q, :id) in ^[1, 2])

      assert_dynamic(expected, actual)
    end

    test ":after operator" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :after, 10)
      expected = dynamic([q], field(q, :id) > ^10)

      assert_dynamic(expected, actual)
    end

    test ":before operator" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :before, 5)
      expected = dynamic([q], field(q, :id) < ^5)

      assert_dynamic(expected, actual)
    end

    test ":start_date operator" do
      date = ~U[2024-01-01 00:00:00Z]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :start_date, date)
      expected = dynamic([q], field(q, :inserted_at) >= ^date)

      assert_dynamic(expected, actual)
    end

    test ":end_date operator" do
      date = ~U[2024-12-31 00:00:00Z]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :end_date, date)
      expected = dynamic([q], field(q, :inserted_at) <= ^date)

      assert_dynamic(expected, actual)
    end

    test ":exists with subquery" do
      subquery_expr = from(c in "comments", select: c.post_id)
      actual = Postgres.build_dynamic(Post, {:as, nil}, :exists, subquery_expr)
      expected = dynamic([q], exists(subquery_expr))

      assert_dynamic(expected, actual)
    end

    test ":exists with negated subquery" do
      subquery_expr = from(c in "comments", select: c.post_id)
      actual = Postgres.build_dynamic(Post, {:as, nil}, :exists, {:not, subquery_expr})
      expected = dynamic([q], not exists(subquery_expr))

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 scalar equality" do
    test "equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:==, "a"})
      expected = dynamic([q], field(q, ^:title) == ^"a")

      assert_dynamic(expected, actual)
    end

    test "not equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:!=, "a"})
      expected = dynamic([q], field(q, ^:title) != ^"a")

      assert_dynamic(expected, actual)
    end

    test "nil equality" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:==, nil})
      expected = dynamic([q], is_nil(field(q, ^:title)))

      assert_dynamic(expected, actual)
    end

    test "nil equality with :eq alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:eq, nil})
      expected = dynamic([q], is_nil(field(q, ^:title)))

      assert_dynamic(expected, actual)
    end

    test "nil inequality" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:!=, nil})
      expected = dynamic([q], not is_nil(field(q, ^:title)))

      assert_dynamic(expected, actual)
    end

    test ":eq alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:eq, "a"})
      expected = dynamic([q], field(q, ^:title) == ^"a")

      assert_dynamic(expected, actual)
    end

    test ":eq nil" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:eq, nil})
      expected = dynamic([q], is_nil(field(q, ^:title)))

      assert_dynamic(expected, actual)
    end

    test ":ne alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:ne, "a"})
      expected = dynamic([q], field(q, ^:title) != ^"a")

      assert_dynamic(expected, actual)
    end

    test ":ne nil" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:ne, nil})
      expected = dynamic([q], not is_nil(field(q, ^:title)))

      assert_dynamic(expected, actual)
    end

    test "== with list coerces to :in" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:==, ["a", "b"]})
      expected = dynamic([q], field(q, ^:title) in ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test "!= with list coerces to :not_in" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:!=, ["a", "b"]})
      expected = dynamic([q], field(q, ^:title) not in ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test "not == with value" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:==, "a"}})
      expected = dynamic([q], field(q, ^:title) != ^"a")

      assert_dynamic(expected, actual)
    end

    test "not != with value" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:!=, "a"}})
      expected = dynamic([q], field(q, ^:title) == ^"a")

      assert_dynamic(expected, actual)
    end

    test "not == with list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:==, ["a", "b"]}})
      expected = dynamic([q], field(q, ^:title) not in ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test "not != with list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:!=, ["a", "b"]}})
      expected = dynamic([q], field(q, ^:title) in ^["a", "b"])

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 scalar comparison" do
    test "greater than" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:>, 1})
      expected = dynamic([q], field(q, ^:views) > ^1)

      assert_dynamic(expected, actual)
    end

    test "greater than or equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:>=, 1})
      expected = dynamic([q], field(q, ^:views) >= ^1)

      assert_dynamic(expected, actual)
    end

    test "less than" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:<, 1})
      expected = dynamic([q], field(q, ^:views) < ^1)

      assert_dynamic(expected, actual)
    end

    test "less than or equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:<=, 1})
      expected = dynamic([q], field(q, ^:views) <= ^1)

      assert_dynamic(expected, actual)
    end

    test ":gt alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:gt, 1})
      expected = dynamic([q], field(q, ^:views) > ^1)

      assert_dynamic(expected, actual)
    end

    test ":gte alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:gte, 1})
      expected = dynamic([q], field(q, ^:views) >= ^1)

      assert_dynamic(expected, actual)
    end

    test ":lt alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:lt, 1})
      expected = dynamic([q], field(q, ^:views) < ^1)

      assert_dynamic(expected, actual)
    end

    test ":lte alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:lte, 1})
      expected = dynamic([q], field(q, ^:views) <= ^1)

      assert_dynamic(expected, actual)
    end

    test "not greater than" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:not, {:>, 1}})
      expected = dynamic([q], not (field(q, ^:views) > ^1))

      assert_dynamic(expected, actual)
    end

    test "not greater than or equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:not, {:>=, 1}})
      expected = dynamic([q], not (field(q, ^:views) >= ^1))

      assert_dynamic(expected, actual)
    end

    test "not less than" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:not, {:<, 1}})
      expected = dynamic([q], not (field(q, ^:views) < ^1))

      assert_dynamic(expected, actual)
    end

    test "not less than or equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:not, {:<=, 1}})
      expected = dynamic([q], not (field(q, ^:views) <= ^1))

      assert_dynamic(expected, actual)
    end

    test "not :gt alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:not, {:gt, 1}})
      expected = dynamic([q], not (field(q, ^:views) > ^1))

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 scalar :in" do
    test ":in with list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:in, ["a"]})
      expected = dynamic([q], field(q, ^:title) in ^["a"])

      assert_dynamic(expected, actual)
    end

    test ":in with {:all, values}" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:in, {:all, ["a", "b"]}})
      expected = dynamic([q], field(q, ^:title) in ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test "not :in" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:in, ["a", "b"]}})
      expected = dynamic([q], field(q, ^:title) not in ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test "not :in with {:all, values}" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:in, {:all, ["a", "b"]}}})
      expected = dynamic([q], field(q, ^:title) not in ^["a", "b"])

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 scalar lower/upper" do
    test "lower" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:lower, "foo"})
      expected = dynamic([q], fragment("lower(?)", field(q, ^:title)) == ^"foo")

      assert_dynamic(expected, actual)
    end

    test "upper" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:upper, "foo"})
      expected = dynamic([q], fragment("upper(?)", field(q, ^:title)) == ^"foo")

      assert_dynamic(expected, actual)
    end

    test "not lower" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:lower, "foo"}})
      expected = dynamic([q], not (fragment("lower(?)", field(q, ^:title)) == ^"foo"))

      assert_dynamic(expected, actual)
    end

    test "not upper" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:upper, "foo"}})
      expected = dynamic([q], not (fragment("upper(?)", field(q, ^:title)) == ^"foo"))

      assert_dynamic(expected, actual)
    end

    test "== lower" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:==, {:lower, "foo"}})
      expected = dynamic([q], fragment("lower(?)", field(q, ^:title)) == ^"foo")

      assert_dynamic(expected, actual)
    end

    test "== upper" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:==, {:upper, "foo"}})
      expected = dynamic([q], fragment("upper(?)", field(q, ^:title)) == ^"foo")

      assert_dynamic(expected, actual)
    end

    test "!= lower" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:!=, {:lower, "foo"}})
      expected = dynamic([q], fragment("lower(?)", field(q, ^:title)) != ^"foo")

      assert_dynamic(expected, actual)
    end

    test "!= upper" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:!=, {:upper, "foo"}})
      expected = dynamic([q], fragment("upper(?)", field(q, ^:title)) != ^"foo")

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 scalar like/ilike" do
    test "like with list" do
      patterns = ["%foo%", "%bar%"]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:like, ["foo", "bar"]})
      expected = dynamic([q], fragment("? LIKE ANY(?)", field(q, ^:title), ^patterns))

      assert_dynamic(expected, actual)
    end

    test "like with value" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:like, "foo"})
      expected = dynamic([q], like(field(q, ^:title), ^"%foo%"))

      assert_dynamic(expected, actual)
    end

    test "not like with list" do
      patterns = ["%foo%"]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:like, ["foo"]}})
      expected = dynamic([q], not fragment("? LIKE ANY(?)", field(q, ^:title), ^patterns))

      assert_dynamic(expected, actual)
    end

    test "not like with value" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:like, "foo"}})
      expected = dynamic([q], not like(field(q, ^:title), ^"%foo%"))

      assert_dynamic(expected, actual)
    end

    test "ilike with list" do
      patterns = ["%foo%"]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:ilike, ["foo"]})
      expected = dynamic([q], fragment("? ILIKE ANY(?)", field(q, ^:title), ^patterns))

      assert_dynamic(expected, actual)
    end

    test "ilike with value" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:ilike, "foo"})
      expected = dynamic([q], ilike(field(q, ^:title), ^"%foo%"))

      assert_dynamic(expected, actual)
    end

    test "not ilike with list" do
      patterns = ["%foo%"]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:ilike, ["foo"]}})
      expected = dynamic([q], not fragment("? ILIKE ANY(?)", field(q, ^:title), ^patterns))

      assert_dynamic(expected, actual)
    end

    test "not ilike with value" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :title, {:not, {:ilike, "foo"}})
      expected = dynamic([q], not ilike(field(q, ^:title), ^"%foo%"))

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 scalar aggregates" do
    test "avg" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:avg, {:>, 10}})
      expected = dynamic([q], avg(field(q, ^:views)) > ^10)

      assert_dynamic(expected, actual)
    end

    test "count" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :id, {:count, {:>, 1}})
      expected = dynamic([q], count(field(q, ^:id)) > ^1)

      assert_dynamic(expected, actual)
    end

    test "sum" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:sum, {:>=, 10}})
      expected = dynamic([q], sum(field(q, ^:views)) >= ^10)

      assert_dynamic(expected, actual)
    end

    test "min" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:min, {:<, 100}})
      expected = dynamic([q], min(field(q, ^:views)) < ^100)

      assert_dynamic(expected, actual)
    end

    test "max" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:max, {:<=, 100}})
      expected = dynamic([q], max(field(q, ^:views)) <= ^100)

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 scalar all/any helpers" do
    test "all with subquery" do
      subquery_expr = from(c in "comments", select: c.post_id)
      actual = Postgres.build_dynamic(Post, {:as, nil}, :id, {:all, {:>, subquery_expr}})
      expected = dynamic([q], field(q, ^:id) > all(subquery_expr))

      assert_dynamic(expected, actual)
    end

    test "negated all with subquery" do
      subquery_expr = from(c in "comments", select: c.post_id)
      actual = Postgres.build_dynamic(Post, {:as, nil}, :id, {:not, {:all, {:>, subquery_expr}}})
      expected = dynamic([q], not (field(q, ^:id) > all(subquery_expr)))

      assert_dynamic(expected, actual)
    end

    test "any with subquery" do
      subquery_expr = from(c in "comments", select: c.post_id)
      actual = Postgres.build_dynamic(Post, {:as, nil}, :id, {:any, {:>, subquery_expr}})
      expected = dynamic([q], field(q, ^:id) > any(subquery_expr))

      assert_dynamic(expected, actual)
    end

    test "negated any with subquery" do
      subquery_expr = from(c in "comments", select: c.post_id)
      actual = Postgres.build_dynamic(Post, {:as, nil}, :id, {:not, {:any, {:>, subquery_expr}}})
      expected = dynamic([q], not (field(q, ^:id) > any(subquery_expr)))

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 scalar arithmetic" do
    test "+" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:>, {:+, [:views, 10]}})
      expected = dynamic([q], field(q, ^:views) > field(q, ^:views) + ^10)

      assert_dynamic(expected, actual)
    end

    test "-" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:>=, {:-, [:views, 2]}})
      expected = dynamic([q], field(q, ^:views) >= field(q, ^:views) - ^2)

      assert_dynamic(expected, actual)
    end

    test "*" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:<, {:*, [:views, 3]}})
      expected = dynamic([q], field(q, ^:views) < field(q, ^:views) * ^3)

      assert_dynamic(expected, actual)
    end

    test "/" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :views, {:<=, {:/, [:views, 4]}})
      expected = dynamic([q], field(q, ^:views) <= field(q, ^:views) / ^4)

      assert_dynamic(expected, actual)
    end

    test "nested arithmetic" do
      actual =
        Postgres.build_dynamic(
          Post,
          {:as, nil},
          :views,
          {:not, {:>, {:*, [{:+, [:views, 10]}, 2]}}}
        )

      expected =
        dynamic(
          [q],
          not (field(q, ^:views) > (field(q, ^:views) + ^10) * ^2)
        )

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 array nil equality" do
    test "nil equality" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:==, nil})
      expected = dynamic([q], is_nil(field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test "nil inequality" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:!=, nil})
      expected = dynamic([q], not is_nil(field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test ":eq nil" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:eq, nil})
      expected = dynamic([q], is_nil(field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test ":ne nil" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:ne, nil})
      expected = dynamic([q], not is_nil(field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 array lower/upper" do
    test "lower" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:lower, "foo"})

      expected =
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
            field(q, ^:tags),
            ^"foo"
          )
        )

      assert_dynamic(expected, actual)
    end

    test "upper" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:upper, "foo"})

      expected =
        dynamic(
          [q],
          fragment(
            """
            EXISTS (
              SELECT 1
              FROM unnest(?) AS t
              WHERE upper(t) = ?
            )
            """,
            field(q, ^:tags),
            ^"foo"
          )
        )

      assert_dynamic(expected, actual)
    end

    test "not lower" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:lower, "foo"}})

      expected =
        dynamic(
          [q],
          fragment(
            """
            NOT EXISTS (
              SELECT 1
              FROM unnest(?) AS t
              WHERE lower(t) = ?
            )
            """,
            field(q, ^:tags),
            ^"foo"
          )
        )

      assert_dynamic(expected, actual)
    end

    test "not upper" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:upper, "foo"}})

      expected =
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
            field(q, ^:tags),
            ^"foo"
          )
        )

      assert_dynamic(expected, actual)
    end

    test "== lower" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:==, {:lower, "foo"}})

      expected =
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
            field(q, ^:tags),
            ^"foo"
          )
        )

      assert_dynamic(expected, actual)
    end

    test "== upper" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:==, {:upper, "foo"}})

      expected =
        dynamic(
          [q],
          fragment(
            """
            EXISTS (
              SELECT 1
              FROM unnest(?) AS t
              WHERE upper(t) = ?
            )
            """,
            field(q, ^:tags),
            ^"foo"
          )
        )

      assert_dynamic(expected, actual)
    end

    test "!= lower" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:!=, {:lower, "foo"}})

      expected =
        dynamic(
          [q],
          fragment(
            """
            NOT EXISTS (
              SELECT 1
              FROM unnest(?) AS t
              WHERE lower(t) = ?
            )
            """,
            field(q, ^:tags),
            ^"foo"
          )
        )

      assert_dynamic(expected, actual)
    end

    test "!= upper" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:!=, {:upper, "foo"}})

      expected =
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
            field(q, ^:tags),
            ^"foo"
          )
        )

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 array like/ilike" do
    test "ilike" do
      patterns = ["%elixir%"]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:ilike, "elixir"})

      expected =
        dynamic(
          [q],
          fragment(
            """
            EXISTS (
              SELECT 1
              FROM unnest(?) AS t
              WHERE t ILIKE ANY (?)
            )
            """,
            field(q, ^:tags),
            ^patterns
          )
        )

      assert_dynamic(expected, actual)
    end

    test "like" do
      patterns = ["%elixir%"]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:like, "elixir"})

      expected =
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
            field(q, ^:tags),
            ^patterns
          )
        )

      assert_dynamic(expected, actual)
    end

    test "not ilike" do
      patterns = ["%ecto%"]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:ilike, "ecto"}})

      expected =
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
            field(q, ^:tags),
            ^patterns
          )
        )

      assert_dynamic(expected, actual)
    end

    test "not like" do
      patterns = ["%ecto%"]
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:like, "ecto"}})

      expected =
        dynamic(
          [q],
          fragment(
            """
            NOT EXISTS (
              SELECT 1
              FROM unnest(?) AS t
              WHERE t LIKE ANY (?)
            )
            """,
            field(q, ^:tags),
            ^patterns
          )
        )

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 array equality" do
    test "== list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:==, ["a", "b"]})
      expected = dynamic([q], field(q, ^:tags) == ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test "!= list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:!=, ["a", "b"]})
      expected = dynamic([q], field(q, ^:tags) != ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test "not == list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:==, ["a", "b"]}})
      expected = dynamic([q], field(q, ^:tags) != ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test "not != list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:!=, ["a", "b"]}})
      expected = dynamic([q], field(q, ^:tags) == ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test "== value (membership)" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:==, "a"})
      expected = dynamic([q], ^"a" in field(q, ^:tags))

      assert_dynamic(expected, actual)
    end

    test "!= value (exclusion)" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:!=, "a"})
      expected = dynamic([q], ^"a" not in field(q, ^:tags))

      assert_dynamic(expected, actual)
    end

    test ":eq list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:eq, ["a", "b"]})
      expected = dynamic([q], field(q, ^:tags) == ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test ":ne list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:ne, ["a", "b"]})
      expected = dynamic([q], field(q, ^:tags) != ^["a", "b"])

      assert_dynamic(expected, actual)
    end

    test ":eq value (membership)" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:eq, "a"})
      expected = dynamic([q], ^"a" in field(q, ^:tags))

      assert_dynamic(expected, actual)
    end

    test ":ne value (exclusion)" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:ne, "a"})
      expected = dynamic([q], ^"a" not in field(q, ^:tags))

      assert_dynamic(expected, actual)
    end

    test "not :eq value" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:eq, "a"}})
      expected = dynamic([q], ^"a" not in field(q, ^:tags))

      assert_dynamic(expected, actual)
    end

    test ":in value (membership)" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:in, "a"})
      expected = dynamic([q], ^"a" in field(q, ^:tags))

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 array :in/:all" do
    test ":in list (overlap)" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:in, ["a", "b"]})
      expected = dynamic([q], fragment("? && ?", field(q, ^:tags), ^["a", "b"]))

      assert_dynamic(expected, actual)
    end

    test "not :in list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:in, ["a", "b"]}})

      expected =
        dynamic(
          [q],
          not fragment("? && ?", field(q, ^:tags), ^["a", "b"])
        )

      assert_dynamic(expected, actual)
    end

    test ":all :in list (contains)" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:all, {:in, ["a", "b"]}})
      expected = dynamic([q], fragment("? @> ?", field(q, ^:tags), ^["a", "b"]))

      assert_dynamic(expected, actual)
    end

    test "not :all :in list" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:all, {:in, ["a", "b"]}}})
      expected = dynamic([q], not fragment("? @> ?", field(q, ^:tags), ^["a", "b"]))

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 array comparison" do
    test "greater than" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:>, "a"})
      expected = dynamic([q], fragment("? < ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test "greater than or equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:>=, "a"})
      expected = dynamic([q], fragment("? <= ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test "less than" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:<, "a"})
      expected = dynamic([q], fragment("? > ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test "less than or equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:<=, "a"})
      expected = dynamic([q], fragment("? >= ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test ":gt alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:gt, "a"})
      expected = dynamic([q], fragment("? < ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test ":gte alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:gte, "a"})
      expected = dynamic([q], fragment("? <= ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test ":lt alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:lt, "a"})
      expected = dynamic([q], fragment("? > ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test ":lte alias" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:lte, "a"})
      expected = dynamic([q], fragment("? >= ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test "not greater than" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:>, "a"}})
      expected = dynamic([q], not fragment("? < ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test "not greater than or equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:>=, "a"}})
      expected = dynamic([q], not fragment("? <= ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test "not less than" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:<, "a"}})
      expected = dynamic([q], not fragment("? > ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end

    test "not less than or equal" do
      actual = Postgres.build_dynamic(Post, {:as, nil}, :tags, {:not, {:<=, "a"}})
      expected = dynamic([q], not fragment("? >= ANY(?)", ^"a", field(q, ^:tags)))

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 schema-less fallback" do
    test "uses scalar expressions without schema metadata" do
      actual = Postgres.build_dynamic({"posts", nil}, {:as, nil}, :title, {:==, "hi"})
      expected = dynamic([q], field(q, ^:title) == ^"hi")

      assert_dynamic(expected, actual)
    end
  end
end
