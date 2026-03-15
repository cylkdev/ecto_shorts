defmodule EctoShorts.Dynamics.PostgresTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Dynamics.Postgres
  alias EctoShorts.Schema.Comment
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

  test "build_dynamic/4 resolves nested scalar wrapper terms before routing" do
    expected = dynamic([q], fragment("lower(?)", field(q, :title)) != ^"hello")

    actual =
      Postgres.build_dynamic(
        Post,
        {:as, nil},
        {:title, %{not: %{==: %{lower: "hello"}}}},
        []
      )

    assert_dynamic(expected, actual)
  end

  test "build_dynamic/4 resolves quantified payload with default equality shorthand" do
    subquery_expr =
      from(c in Comment,
        where: c.published == ^true,
        select: c.id
      )

    expected = dynamic([q], field(q, :id) == all(subquery_expr))

    actual =
      Postgres.build_dynamic(
        Post,
        {:as, nil},
        {:id, %{all: %{from: Comment, where: %{published: true}}}},
        []
      )

    assert_dynamic(expected, actual)
  end

  test "build_dynamic/4 negates quantified equality as a wrapped comparison" do
    subquery_expr =
      from(c in Comment,
        where: c.published == ^true,
        select: c.id
      )

    expected = dynamic([q], not (field(q, :id) == all(subquery_expr)))

    actual =
      Postgres.build_dynamic(
        Post,
        {:as, nil},
        {:id, %{not: %{all: %{from: Comment, where: %{published: true}}}}},
        []
      )

    assert_dynamic(expected, actual)
  end

  test "build_dynamic/4 resolves quantified greater-than all comparison payloads" do
    subquery_expr =
      from(c in Comment,
        where: c.published == ^true,
        select: c.id
      )

    expected = dynamic([q], field(q, :id) > all(subquery_expr))

    actual =
      Postgres.build_dynamic(
        Post,
        {:as, nil},
        {:id, %{>: %{all: %{from: Comment, where: %{published: true}}}}},
        []
      )

    assert_dynamic(expected, actual)
  end

  test "build_dynamic/4 resolves quantified less-than-or-equal any comparison payloads" do
    subquery_expr =
      from(c in Comment,
        where: c.published == ^true,
        select: c.id
      )

    expected = dynamic([q], field(q, :id) <= any(subquery_expr))

    actual =
      Postgres.build_dynamic(
        Post,
        {:as, nil},
        {:id, %{<=: %{any: %{from: Comment, where: %{published: true}}}}},
        []
      )

    assert_dynamic(expected, actual)
  end

  test "build_dynamic/4 routes array-local all comparison payloads without using quantified subquery handling" do
    expected = dynamic([q], fragment("? < ALL(?)", ^"a", field(q, :tags)))

    actual =
      Postgres.build_dynamic(
        Post,
        {:as, nil},
        {:tags, %{all: %{>: "a"}}},
        []
      )

    assert_dynamic(expected, actual)
  end

  test "build_dynamic/4 routes array-local all in payloads without using quantified subquery handling" do
    expected = dynamic([q], fragment("? <@ ?", field(q, :tags), ^["elixir", "erlang"]))

    actual =
      Postgres.build_dynamic(
        Post,
        {:as, nil},
        {:tags, %{all: %{in: ["elixir", "erlang"]}}},
        []
      )

    assert_dynamic(expected, actual)
  end
end
