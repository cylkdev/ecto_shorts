defmodule EctoShorts.CommonFilters.LimitTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 limit shapes" do
    test "matches Ecto.Query for a root integer limit" do
      expected = limit(Post, ^10)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{limit: 10},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when limit overrides a previous limit" do
      expected = limit(Post, ^10)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{limit: 10},
          []
        )

      assert_query(expected, actual)
    end
  end
end
