defmodule EctoShorts.QueryBuilderTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  import Ecto.Query

  alias EctoShorts.QueryBuilder
  alias EctoShorts.Schema.Post

  test "build_query/6 applies pagination filters via the Filters stage" do
    binding = {:as, nil}
    limit_value = 5

    query = from(p in Post)

    actual = QueryBuilder.build_query(Post, query, binding, :limit, limit_value, [])
    expected = limit(query, ^limit_value)

    assert_query(expected, actual)
  end
end
