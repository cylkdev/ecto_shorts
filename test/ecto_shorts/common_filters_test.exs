defmodule EctoShorts.CommonFiltersTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "CommonExpr filters" do
    test "scalar field :id" do
      id = 1
      expected = from(p in Post, where: p.id == ^id)
      actual = CommonFilters.convert_params_to_filter(Post, %{id: id}, [])

      assert_sql(expected, actual)
    end

    test "scalar field :id with operator :==" do
      id = 1
      expected = from(p in Post, where: p.id == ^id)
      actual = CommonFilters.convert_params_to_filter(Post, %{id: %{==: id}}, [])

      assert_sql(expected, actual)
    end

    test ":ids" do
      ids = [1, 2, 3]
      expected = from(p in Post, where: p.id in ^ids)
      actual = CommonFilters.convert_params_to_filter(Post, %{ids: ids}, [])

      assert_sql(expected, actual)
    end

    test "start_date builds an inserted_at lower bound" do
      date = ~U[2026-03-09 02:04:01.573399Z]
      expected = from(p in Post, where: p.inserted_at >= ^date)
      actual = CommonFilters.convert_params_to_filter(Post, %{start_date: date}, [])

      assert_sql(expected, actual)
    end

    test "end_date builds an inserted_at upper bound" do
      date = ~U[2026-03-09 02:04:01.573399Z]
      expected = from(p in Post, where: p.inserted_at <= ^date)
      actual = CommonFilters.convert_params_to_filter(Post, %{end_date: date}, [])

      assert_sql(expected, actual)
    end
  end
end
