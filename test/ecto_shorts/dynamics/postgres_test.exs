defmodule EctoShorts.Adapters.PostgresTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Adapters.Postgres
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  test "build_dynamic/4 supports scalar operator :eq for aliased binding" do
    id = 1
    expected = from(p in Post, as: :post, where: p.id == ^id)

    actual =
      from(p in Post,
        as: :post,
        where: ^Postgres.build_dynamic(Post, {:as, :post}, {:id, %{eq: id}}, [])
      )

    EctoShorts.Testing.assert_sql(EctoShorts.Repo, expected, actual)
  end

  test "build_dynamic/4 supports scalar operator :eq for positional binding" do
    id = 1
    expected = dynamic([_, q], q.id == ^id)
    actual = Postgres.build_dynamic(Post, {:at, 2}, {:id, %{eq: id}}, [])

    assert_dynamic(expected, actual)
  end
end
