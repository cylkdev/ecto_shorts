defmodule EctoShorts.CommonFilters.DateWrappersTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 date wrappers" do
    test "Rule Statement 7: inserted_at equals ago 1 day using date wrapper" do
      expected =
        from(p in Post,
          where: fragment("date(?)", p.inserted_at) == fragment("date(?)", ago(^1, "day"))
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{==: %{date: %{ago: %{count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, actual)
    end

    test "Rule Statement 8: inserted_at not equals from_now 1 day using date wrapper" do
      expected =
        from(p in Post,
          where: fragment("date(?)", p.inserted_at) != fragment("date(?)", from_now(^1, "day"))
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{!=: %{date: %{from_now: %{count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, actual)
    end

    test "Rule Statement 10: inserted_at greater than from_now 1 day negated using date wrapper" do
      expected =
        from(p in Post,
          where: not (fragment("date(?)", p.inserted_at) > fragment("date(?)", from_now(^1, "day")))
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{not: %{>: %{date: %{from_now: %{count: 1, interval: "day"}}}}}},
          []
        )

      assert_sql(expected, actual)
    end

    test "Rule Statement 11: inserted_at >= datetime_add 7 days using date wrapper" do
      expected =
        from(p in Post,
          where:
            fragment("date(?)", p.inserted_at) >=
              fragment("date(?)", datetime_add(p.inserted_at, ^7, "day"))
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>=: %{date: %{add: %{field: "inserted_at", count: 7, interval: "day"}}}}},
          []
        )

      assert_sql(expected, actual)
    end

    test "Rule Statement 12: inserted_at less than ago 1 month using date wrapper" do
      expected =
        from(p in Post,
          where: fragment("date(?)", p.inserted_at) < fragment("date(?)", ago(^1, "month"))
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{<: %{date: %{ago: %{count: 1, interval: "month"}}}}},
          []
        )

      assert_sql(expected, actual)
    end
  end
end
