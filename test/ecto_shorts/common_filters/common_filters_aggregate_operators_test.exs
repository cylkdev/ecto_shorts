defmodule EctoShorts.CommonFilters.AggregateOperatorsTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 aggregate operators" do
    test "Rule Statement 1: avg views greater than" do
      expected = from(p in Post, where: avg(p.views) > ^10)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{avg: %{>: 10}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 2: avg views greater than negated" do
      expected = from(p in Post, where: not (avg(p.views) > ^10))
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{avg: %{>: 10}}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 3: count views greater than zero" do
      expected = from(p in Post, where: count(p.views) > ^0)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{count: %{>: 0}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 4: max views greater than or equal" do
      expected = from(p in Post, where: max(p.views) >= ^100)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{max: %{>=: 100}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 5: min views less than" do
      expected = from(p in Post, where: min(p.views) < ^5)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{min: %{<: 5}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 6: sum views equals" do
      expected = from(p in Post, where: sum(p.views) == ^1000)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{sum: %{==: 1000}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 7: avg views not equals" do
      expected = from(p in Post, where: avg(p.views) != ^50)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{avg: %{!=: 50}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 8: count views equals zero" do
      expected = from(p in Post, where: count(p.views) == ^0)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{count: %{==: 0}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 9: count views greater than zero negated" do
      expected = from(p in Post, where: not (count(p.views) > ^0))
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{count: %{>: 0}}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 10: max views greater than or equal negated" do
      expected = from(p in Post, where: not (max(p.views) >= ^100))
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{max: %{>=: 100}}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 11: avg views less than or equal" do
      expected = from(p in Post, where: avg(p.views) <= ^10)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{avg: %{<=: 10}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 12: sum views greater than" do
      expected = from(p in Post, where: sum(p.views) > ^500)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{sum: %{>: 500}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 13: min views equals zero" do
      expected = from(p in Post, where: min(p.views) == ^0)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{min: %{==: 0}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 14: sum views not equals zero" do
      expected = from(p in Post, where: sum(p.views) != ^0)
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{sum: %{!=: 0}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 15: min views less than negated" do
      expected = from(p in Post, where: not (min(p.views) < ^5))
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{min: %{<: 5}}}}, [])

      assert_sql(expected, actual)
    end

    test "Rule Statement 16: sum views greater than negated" do
      expected = from(p in Post, where: not (sum(p.views) > ^500))
      actual = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{sum: %{>: 500}}}}, [])

      assert_sql(expected, actual)
    end
  end
end
