defmodule EctoShorts.CommonFilters.DatetimeWrappersTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 datetime wrappers" do
    test "matches records using datetime_add before comparison" do
      expected = from(p in Post, where: p.inserted_at >= datetime_add(p.inserted_at, ^1, "day"))

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>=: %{datetime: %{add: %{field: "inserted_at", count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, actual)
    end

    test "matches records using ago before comparison" do
      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: "day"}}}}},
          []
        )

      expected = from(p in Post, where: p.inserted_at > ago(^1, "day"))

      assert_sql(expected, actual)
    end

    test "matches records using from_now before comparison" do
      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>: %{datetime: %{from_now: %{count: 1, interval: "day"}}}}},
          []
        )

      expected = from(p in Post, where: p.inserted_at > from_now(^1, "day"))

      assert_sql(expected, actual)
    end

    test "excludes records using negated datetime_add comparison" do
      expected = from(p in Post, where: not (p.inserted_at >= datetime_add(p.inserted_at, ^1, "day")))

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            inserted_at: %{
              not: %{>=: %{datetime: %{add: %{field: "inserted_at", count: 1, interval: "day"}}}}
            }
          },
          []
        )

      assert_sql(expected, actual)
    end
  end
end
