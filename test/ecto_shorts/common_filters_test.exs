defmodule EctoShorts.CommonFiltersTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "CommonExpr filters" do
    test "start_date builds an inserted_at lower bound" do
      date = ~U[2026-03-09 02:04:01.573399Z]
      expected = from(p in Post, where: p.inserted_at >= ^date)
      actual = CommonFilters.convert_params_to_filter(Post, %{start_date: date}, [])

      assert_sql(expected, actual)
    end
  end
end
