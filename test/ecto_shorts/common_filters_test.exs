defmodule EctoShorts.CommonFiltersTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Comment
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "Array Fields" do
    test "1" do
      expected = from(p in Post, where: p.id == ^1)
      actual = CommonFilters.convert_params_to_filter(Post, %{id: 1}, [])

      assert_sql(expected, actual)
    end

    # test "1" do
    #   expected = from(p in Post, where: p.id == ^1)
    #   actual = CommonFilters.convert_params_to_filter(Post, %{id: 1}, [])

    #   assert_sql(expected, actual)
    # end
  end
end
