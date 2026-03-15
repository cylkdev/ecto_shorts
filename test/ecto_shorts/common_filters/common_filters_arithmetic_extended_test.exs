defmodule EctoShorts.CommonFilters.ArithmeticExtendedTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  # Each test exercises a specific apply_arith_comparison/6 clause.
  # The input shape is %{field: %{op: %{value: %{arith_op: [%{field: f2}, %{value: v}]}}}}

  describe "convert_params_to_filter/3 arithmetic + variants" do
    test "views == views + 10 (plain)" do
      expected = from(p in Post, where: p.views == p.views + ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{==: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views != views + 10 (plain)" do
      expected = from(p in Post, where: p.views != p.views + ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{!=: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views > views + 10 (plain)" do
      expected = from(p in Post, where: p.views > p.views + ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views >= views + 10 (plain)" do
      expected = from(p in Post, where: p.views >= p.views + ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>=: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views < views + 10 (plain)" do
      expected = from(p in Post, where: p.views < p.views + ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{<: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views <= views + 10 (plain)" do
      expected = from(p in Post, where: p.views <= p.views + ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{<=: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views != views + 10) (negated)" do
      expected = from(p in Post, where: not (p.views != p.views + ^10))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{!=: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views >= views + 10) (negated)" do
      expected = from(p in Post, where: not (p.views >= p.views + ^10))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{>=: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views < views + 10) (negated)" do
      expected = from(p in Post, where: not (p.views < p.views + ^10))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{<: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views <= views + 10) (negated)" do
      expected = from(p in Post, where: not (p.views <= p.views + ^10))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{<=: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 arithmetic - variants" do
    test "views == views - 5 (plain)" do
      expected = from(p in Post, where: p.views == p.views - ^5)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{==: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views != views - 5 (plain)" do
      expected = from(p in Post, where: p.views != p.views - ^5)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{!=: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views >= views - 5 (plain)" do
      expected = from(p in Post, where: p.views >= p.views - ^5)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>=: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views < views - 5 (plain)" do
      expected = from(p in Post, where: p.views < p.views - ^5)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{<: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views <= views - 5 (plain)" do
      expected = from(p in Post, where: p.views <= p.views - ^5)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{<=: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views == views - 5) (negated)" do
      expected = from(p in Post, where: not (p.views == p.views - ^5))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{==: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views != views - 5) (negated)" do
      expected = from(p in Post, where: not (p.views != p.views - ^5))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{!=: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views >= views - 5) (negated)" do
      expected = from(p in Post, where: not (p.views >= p.views - ^5))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{>=: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views < views - 5) (negated)" do
      expected = from(p in Post, where: not (p.views < p.views - ^5))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{<: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views <= views - 5) (negated)" do
      expected = from(p in Post, where: not (p.views <= p.views - ^5))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{<=: %{value: %{-: [%{field: "views"}, %{value: 5}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 arithmetic * variants" do
    test "views == views * 2 (plain)" do
      expected = from(p in Post, where: p.views == p.views * ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{==: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views != views * 2 (plain)" do
      expected = from(p in Post, where: p.views != p.views * ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{!=: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views > views * 2 (plain)" do
      expected = from(p in Post, where: p.views > p.views * ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views < views * 2 (plain)" do
      expected = from(p in Post, where: p.views < p.views * ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{<: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views <= views * 2 (plain)" do
      expected = from(p in Post, where: p.views <= p.views * ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{<=: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views == views * 2) (negated)" do
      expected = from(p in Post, where: not (p.views == p.views * ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{==: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views != views * 2) (negated)" do
      expected = from(p in Post, where: not (p.views != p.views * ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{!=: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views > views * 2) (negated)" do
      expected = from(p in Post, where: not (p.views > p.views * ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{>: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views < views * 2) (negated)" do
      expected = from(p in Post, where: not (p.views < p.views * ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{<: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views <= views * 2) (negated)" do
      expected = from(p in Post, where: not (p.views <= p.views * ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{<=: %{value: %{*: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 arithmetic / variants" do
    test "views == views / 2 (plain)" do
      expected = from(p in Post, where: p.views == p.views / ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{==: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views != views / 2 (plain)" do
      expected = from(p in Post, where: p.views != p.views / ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{!=: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views >= views / 2 (plain)" do
      expected = from(p in Post, where: p.views >= p.views / ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>=: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views < views / 2 (plain)" do
      expected = from(p in Post, where: p.views < p.views / ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{<: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "views <= views / 2 (plain)" do
      expected = from(p in Post, where: p.views <= p.views / ^2)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{<=: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views == views / 2) (negated)" do
      expected = from(p in Post, where: not (p.views == p.views / ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{==: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views != views / 2) (negated)" do
      expected = from(p in Post, where: not (p.views != p.views / ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{!=: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views >= views / 2) (negated)" do
      expected = from(p in Post, where: not (p.views >= p.views / ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{>=: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views < views / 2) (negated)" do
      expected = from(p in Post, where: not (p.views < p.views / ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{<: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "not (views <= views / 2) (negated)" do
      expected = from(p in Post, where: not (p.views <= p.views / ^2))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{<=: %{value: %{/: [%{field: "views"}, %{value: 2}]}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end
end
