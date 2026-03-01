defmodule EctoShorts.CommonFilters.ScalarFilterTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 comparison operators" do
    test "comparison - %{id: %{==: 1}}" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{==: 1}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{id: %{eq: 1}}" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{eq: 1}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{published_at: %{==: nil}} (IS NULL)" do
      expected = from p in Post, where: is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{==: nil}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{published_at: %{eq: nil}} (IS NULL)" do
      expected = from p in Post, where: is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{eq: nil}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{published_at: %{!=: nil}} (IS NOT NULL)" do
      expected = from p in Post, where: not is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{!=: nil}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{published_at: %{ne: nil}} (IS NOT NULL)" do
      expected = from p in Post, where: not is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{ne: nil}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{>: 10}}" do
      expected = from p in Post, where: p.views > ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{>=: 10}}" do
      expected = from p in Post, where: p.views >= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{<: 10}}" do
      expected = from p in Post, where: p.views < ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{<=: 10}}" do
      expected = from p in Post, where: p.views <= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{!=: 10}}" do
      expected = from p in Post, where: p.views != ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{!=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{ne: 10}}" do
      expected = from p in Post, where: p.views != ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{ne: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{gt: 10}}" do
      expected = from p in Post, where: p.views > ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{gt: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{gte: 10}}" do
      expected = from p in Post, where: p.views >= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{gte: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{lt: 10}}" do
      expected = from p in Post, where: p.views < ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{lt: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{views: %{lte: 10}}" do
      expected = from p in Post, where: p.views <= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{lte: 10}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{published: %{in: [true, false]}}" do
      expected = from p in Post, where: p.published in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{in: [true, false]}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{id: %{in: [1, 2, 3]}}" do
      expected = from p in Post, where: p.id in ^[1, 2, 3]
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{in: [1, 2, 3]}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{published: %{==: [true, false]}} (coerces to IN)" do
      expected = from p in Post, where: p.published in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{==: [true, false]}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - %{published: %{!=: [true, false]}} (coerces to NOT IN)" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{!=: [true, false]}}, [])

      assert_sql(expected, q2)
    end

    test "comparison - preserves struct values (DateTime)" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.published_at >= ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{>=: dt}}, [])

      assert_sql(expected, q2)
    end

    test "invalid nil operator logs warning and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{published_at: %{>: nil}}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "No dynamic expression generated for field :published_at with expression: {:>, nil}"
      assert_received {:q2, q2}
      assert q2 === q
    end
  end

  describe "convert_params_to_filter/3 negation" do
    test "negation - %{published: %{not: %{in: [true, false]}}}" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{not: %{in: [true, false]}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{published: %{not: %{==: [true, false]}}} (coerces to NOT IN)" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{not: %{==: [true, false]}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{published: %{not: %{!=: [true, false]}}} (double negation coerces to IN)" do
      expected = from p in Post, where: p.published in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{not: %{!=: [true, false]}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{>: 10}}}" do
      expected = from p in Post, where: not (p.views > ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{>: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{>=: 10}}}" do
      expected = from p in Post, where: not (p.views >= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{>=: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{<: 10}}}" do
      expected = from p in Post, where: not (p.views < ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{<: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{<=: 10}}}" do
      expected = from p in Post, where: not (p.views <= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{<=: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{==: 10}}}" do
      expected = from p in Post, where: p.views != ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{==: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{!=: 10}}}" do
      expected = from p in Post, where: p.views == ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{!=: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{ne: 10}}}" do
      expected = from p in Post, where: p.views == ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{ne: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{gt: 10}}}" do
      expected = from p in Post, where: not (p.views > ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{gt: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{gte: 10}}}" do
      expected = from p in Post, where: not (p.views >= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{gte: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{lt: 10}}}" do
      expected = from p in Post, where: not (p.views < ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{lt: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "negation - %{views: %{not: %{lte: 10}}}" do
      expected = from p in Post, where: not (p.views <= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{lte: 10}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 string matching" do
    test "string matching - %{title: %{like: \"hello\"}}" do
      expected = from p in Post, where: like(p.title, ^"%hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{like: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "string matching - %{title: %{ilike: \"hello\"}}" do
      expected = from p in Post, where: ilike(p.title, ^"%hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{ilike: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "string matching - %{title: %{like: [\"hello\", \"world\"]}}" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: fragment("? LIKE ANY(?)", p.title, ^patterns)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{like: ["hello", "world"]}}, [])

      assert_sql(expected, q2)
    end

    test "string matching - %{title: %{ilike: [\"hello\", \"world\"]}}" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: fragment("? ILIKE ANY(?)", p.title, ^patterns)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{ilike: ["hello", "world"]}}, [])

      assert_sql(expected, q2)
    end

    test "string matching - %{title: %{not: %{like: \"hello\"}}}" do
      expected = from p in Post, where: not like(p.title, ^"%hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{not: %{like: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "string matching - %{title: %{not: %{ilike: \"hello\"}}}" do
      expected = from p in Post, where: not ilike(p.title, ^"%hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{not: %{ilike: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "string matching - %{title: %{not: %{like: [\"hello\", \"world\"]}}}" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: not fragment("? LIKE ANY(?)", p.title, ^patterns)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{title: %{not: %{like: ["hello", "world"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "string matching - %{title: %{not: %{ilike: [\"hello\", \"world\"]}}}" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: not fragment("? ILIKE ANY(?)", p.title, ^patterns)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{title: %{not: %{ilike: ["hello", "world"]}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 string transformations" do
    test "string transformation - %{title: %{==: %{lower: \"hello\"}}}" do
      expected = from p in Post, where: fragment("lower(?)", p.title) == ^"hello"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{==: %{lower: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "string transformation - %{title: %{==: %{upper: \"HELLO\"}}}" do
      expected = from p in Post, where: fragment("upper(?)", p.title) == ^"HELLO"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{==: %{upper: "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "string transformation - %{title: %{!=: %{lower: \"hello\"}}}" do
      expected = from p in Post, where: fragment("lower(?)", p.title) != ^"hello"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{!=: %{lower: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "string transformation - %{title: %{!=: %{upper: \"HELLO\"}}}" do
      expected = from p in Post, where: fragment("upper(?)", p.title) != ^"HELLO"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{!=: %{upper: "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "string transformation - %{title: %{not: %{==: %{lower: \"hello\"}}}}" do
      expected = from p in Post, where: fragment("lower(?)", p.title) != ^"hello"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{not: %{==: %{lower: "hello"}}}}, [])

      assert_sql(expected, q2)
    end

    test "string transformation - %{title: %{not: %{==: %{upper: \"HELLO\"}}}}" do
      expected = from p in Post, where: fragment("upper(?)", p.title) != ^"HELLO"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{not: %{==: %{upper: "HELLO"}}}}, [])

      assert_sql(expected, q2)
    end
  end
end
