defmodule EctoShorts.CommonFilters.ScalarExprExtendedTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 arithmetic negation" do
    test "excludes records using negated arithmetic comparison" do
      expected = from(p in Post, where: not (p.views == p.views + ^10))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{==: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 value wrapper negation" do
    test "excludes records using negated value-wrapped comparison" do
      expected = from(p in Post, where: p.views != ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{==: %{value: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "excludes records using negated value-wrapped greater-than" do
      expected = from(p in Post, where: not (p.views > ^5))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{>: %{value: 5}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 arithmetic subtraction comparisons" do
    test "matches records using arithmetic subtraction comparison" do
      expected = from(p in Post, where: p.views != p.views - ^5)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{!=: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using arithmetic subtraction greater-than comparison" do
      expected = from(p in Post, where: p.views > p.views - ^5)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "excludes records using negated arithmetic subtraction comparison" do
      expected = from(p in Post, where: not (p.views > p.views - ^5))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{>: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 arithmetic multiplication comparisons" do
    test "matches records using arithmetic multiplication comparison" do
      expected = from(p in Post, where: p.views == p.views * ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{==: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "excludes records using negated arithmetic multiplication comparison" do
      expected = from(p in Post, where: not (p.views >= p.views * ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{>=: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 arithmetic division comparisons" do
    test "matches records using arithmetic division comparison" do
      expected = from(p in Post, where: p.views > p.views / ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "excludes records using negated arithmetic division comparison" do
      expected = from(p in Post, where: not (p.views > p.views / ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{>: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 generic scalar fallback" do
    test "matches records using the generic scalar != fallback" do
      expected = from(p in Post, where: p.views != ^5)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{!=: %{value: 5}}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the generic scalar >= fallback" do
      expected = from(p in Post, where: p.views >= ^5)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>=: %{value: 5}}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the generic scalar < fallback" do
      expected = from(p in Post, where: p.views < ^5)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<: %{value: 5}}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the generic scalar <= fallback" do
      expected = from(p in Post, where: p.views <= ^5)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<=: %{value: 5}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records using the negated generic scalar != fallback" do
      expected = from(p in Post, where: p.views == ^5)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{!=: %{value: 5}}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records using the negated generic scalar >= fallback" do
      expected = from(p in Post, where: not (p.views >= ^5))
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{>=: %{value: 5}}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records using the negated generic scalar < fallback" do
      expected = from(p in Post, where: not (p.views < ^5))
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{<: %{value: 5}}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records using the negated generic scalar <= fallback" do
      expected = from(p in Post, where: not (p.views <= ^5))
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{<=: %{value: 5}}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 generic datetime comparisons" do
    test "matches records using generic datetime ago comparison via catch-all" do
      expected =
        from(p in Post, where: fragment("date(?)", p.published_at) == fragment("date(?)", ago(^1, "month")))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{published_at: %{==: %{date: %{ago: [count: 1, interval: "month"]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "excludes records using negated generic datetime comparison via catch-all" do
      expected =
        from(p in Post, where: fragment("date(?)", p.published_at) != fragment("date(?)", ago(^1, "month")))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{published_at: %{not: %{==: %{date: %{ago: [count: 1, interval: "month"]}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end
end
