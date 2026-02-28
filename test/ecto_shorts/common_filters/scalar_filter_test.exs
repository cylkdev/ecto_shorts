defmodule EctoShorts.CommonFilters.ScalarFilterTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 scalar field filters" do
    test "preserves struct values (DateTime) for scalar comparisons" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.published_at == ^dt

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{published_at: dt},
          []
        )

      assert_sql(expected, q2)
    end

    test "preserves struct values (DateTime) for operator map comparisons" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.published_at >= ^dt

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{published_at: %{>=: dt}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports === nil comparisons (generates IS NULL)" do
      expected = from p in Post, where: is_nil(p.published_at)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published_at: nil}, [])

      assert_sql(expected, q2)
    end

    test "supports !== nil comparisons (generates IS NOT NULL)" do
      expected = from p in Post, where: not is_nil(p.published_at)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published_at: %{!=: nil}}, [])

      assert_sql(expected, q2)
    end

    test "invalid nil operator logs warning and leaves query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{published_at: %{>: nil}}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected the operator to be one of [:eq, :==, :!=] for nil comparison, got: :>"
      assert_received {:q2, q2}
      assert_sql(q, q2)
    end

    test "supports explicit operator" do
      expected = from p in Post, where: p.published != ^true
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published: %{!=: true}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated explicit operator" do
      expected = from p in Post, where: p.published != ^true
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{not: %{==: true}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated explicit operator (not !=)" do
      expected = from p in Post, where: p.published == ^true
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{not: %{!=: true}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports explicit IN operator for scalar fields" do
      expected = from p in Post, where: p.published in ^[true, false]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published: %{in: [true, false]}}, [])

      assert_sql(expected, q2)
    end

    test "supports explicit NOT IN operator for scalar fields" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{published: %{not: %{in: [true, false]}}}, [])

      assert_sql(expected, q2)
    end

    test "supports LIKE operator for scalar fields" do
      expected = from p in Post, where: like(p.title, ^"%hello%")
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{like: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "supports ILIKE operator for scalar fields" do
      expected = from p in Post, where: ilike(p.title, ^"%hello%")
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{ilike: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "supports LIKE operator for scalar fields with list RHS (LIKE ANY)" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: fragment("? LIKE ANY(?)", p.title, ^patterns)
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{like: ["hello", "world"]}}, [])

      assert_sql(expected, q2)
    end

    test "supports ILIKE operator for scalar fields with list RHS (ILIKE ANY)" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: fragment("? ILIKE ANY(?)", p.title, ^patterns)
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{ilike: ["hello", "world"]}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated LIKE operator for scalar fields" do
      expected = from p in Post, where: not like(p.title, ^"%hello%")
      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{not: %{like: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated ILIKE operator for scalar fields" do
      expected = from p in Post, where: not ilike(p.title, ^"%hello%")
      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{not: %{ilike: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated LIKE operator for scalar fields with list RHS (NOT LIKE ANY)" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: not fragment("? LIKE ANY(?)", p.title, ^patterns)
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{title: %{not: %{like: ["hello", "world"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated ILIKE operator for scalar fields with list RHS (NOT ILIKE ANY)" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: not fragment("? ILIKE ANY(?)", p.title, ^patterns)
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{title: %{not: %{ilike: ["hello", "world"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "coerces !== with list RHS to NOT IN for scalar fields" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{!=: [true, false]}},
          []
        )

      assert_sql(expected, q2)
    end

    test "coerces not === with list RHS to NOT IN for scalar fields" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{not: %{==: [true, false]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "coerces not !== with list RHS to IN for scalar fields" do
      expected = from p in Post, where: p.published in ^[true, false]
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{not: %{!=: [true, false]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports > comparison for scalar fields" do
      expected = from p in Post, where: p.views > ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{>: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports >= comparison for scalar fields" do
      expected = from p in Post, where: p.views >= ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{>=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports < comparison for scalar fields" do
      expected = from p in Post, where: p.views < ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{<: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports <= comparison for scalar fields" do
      expected = from p in Post, where: p.views <= ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{<=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated > comparison for scalar fields" do
      expected = from p in Post, where: not (p.views > ^10)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{not: %{>: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "supports LOWER operator for scalar fields" do
      expected = from p in Post, where: fragment("lower(?)", p.title) == ^"hello"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{lower: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for scalar fields" do
      expected = from p in Post, where: fragment("upper(?)", p.title) == ^"HELLO"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{upper: "HELLO"}}, [])

      assert_sql(expected, q2)
    end

    test "supports LOWER operator for scalar fields via explicit ==" do
      expected = from p in Post, where: fragment("lower(?)", p.title) == ^"hello"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{==: %{lower: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for scalar fields via explicit ==" do
      expected = from p in Post, where: fragment("upper(?)", p.title) == ^"HELLO"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{==: %{upper: "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports LOWER operator for scalar fields via explicit !=" do
      expected = from p in Post, where: fragment("lower(?)", p.title) != ^"hello"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{!=: %{lower: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for scalar fields via explicit !=" do
      expected = from p in Post, where: fragment("upper(?)", p.title) != ^"HELLO"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{!=: %{upper: "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated LOWER operator for scalar fields" do
      expected = from p in Post, where: not (fragment("lower(?)", p.title) == ^"hello")
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{title: %{not: %{lower: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated UPPER operator for scalar fields" do
      expected = from p in Post, where: not (fragment("upper(?)", p.title) == ^"HELLO")
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{title: %{not: %{upper: "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports multiple conditions under a single filter" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          where: p.published != ^false
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{published: [==: true, !=: false]}, [])

      assert_sql(expected, q2)
    end

    test "supports boolean :and operator for multiple comparisons on same field" do
      expected =
        from(p in Post,
          where: p.views > ^10 and p.views < ^20
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{and: [>: 10, <: 20]}}, [])

      assert_sql(expected, q2)
    end

    test "supports boolean :or operator for multiple comparisons on same field" do
      expected =
        from(p in Post,
          where: p.published == ^true or p.published == ^false
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{published: %{or: [==: true, ==: false]}},
          []
        )

      assert_sql(expected, q2)
    end

    test "boolean operator with empty params is a no-op" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{and: []}}, [])
      assert q2 === q
    end

    test "supports :gt alias operator for scalar fields" do
      expected = from p in Post, where: p.views > ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{gt: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports :gte alias operator for scalar fields" do
      expected = from p in Post, where: p.views >= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{gte: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports :lt alias operator for scalar fields" do
      expected = from p in Post, where: p.views < ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{lt: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports :lte alias operator for scalar fields" do
      expected = from p in Post, where: p.views <= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{lte: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports explicit == operator for scalar fields" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{==: 1}}, [])

      assert_sql(expected, q2)
    end

    test "supports :eq alias operator for scalar fields" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{eq: 1}}, [])

      assert_sql(expected, q2)
    end

    test "supports explicit == with list RHS coercing to IN for scalar fields" do
      expected = from p in Post, where: p.published in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{==: [true, false]}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated >= comparison for scalar fields" do
      expected = from p in Post, where: not (p.views >= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{>=: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated < comparison for scalar fields" do
      expected = from p in Post, where: not (p.views < ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{<: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated <= comparison for scalar fields" do
      expected = from p in Post, where: not (p.views <= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{<=: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated == comparison for scalar fields" do
      expected = from p in Post, where: p.views != ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{==: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "supports explicit == nil (IS NULL)" do
      expected = from p in Post, where: is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{==: nil}}, [])

      assert_sql(expected, q2)
    end

    test "supports :eq nil alias (IS NULL)" do
      expected = from p in Post, where: is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{eq: nil}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated NOT IN coercing to IN for scalar fields (double negation)" do
      expected = from p in Post, where: p.published in ^[true, false]

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{published: %{not: %{!=: [true, false]}}},
          []
        )

      assert_sql(expected, q2)
    end
  end
end
