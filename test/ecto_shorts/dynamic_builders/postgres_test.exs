defmodule EctoShorts.DynamicBuilders.PostgresTest do
  use ExUnit.Case, async: true

  alias EctoShorts.DynamicBuilders.Postgres
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

  describe "build_dynamic/4 :exists operator" do
    test "builds an exists dynamic for a subquery term" do
      import Ecto.Query

      subq = from(p in Post, where: p.published == ^true) |> subquery()

      actual =
        Postgres.build_dynamic(
          Post,
          {:as, nil},
          {:exists, subq},
          []
        )

      refute is_nil(actual)
    end
  end

  describe "build_dynamic/4 merge_dynamic(a, op, nil) path" do
    test "returns non-nil result when second param entry produces nil dynamic" do
      actual =
        Postgres.build_dynamic(
          Post,
          {:as, nil},
          {:id, [eq: 1, unsupported_op_xyz: "value"]},
          []
        )

      refute is_nil(actual)
    end
  end

  describe "build_dynamic/4 top-level quantifier operator (:all/:any)" do
    test "builds a single-field :all dynamic expression" do
      id = 1
      expected = dynamic([q], q.id == ^id)

      actual =
        Postgres.build_dynamic(
          Post,
          {:as, nil},
          {:all, [id: id]},
          []
        )

      assert_dynamic(expected, actual)
    end

    test "builds a single-field :any dynamic expression" do
      id = 1
      expected = dynamic([q], q.id == ^id)

      actual =
        Postgres.build_dynamic(
          Post,
          {:as, nil},
          {:any, [id: id]},
          []
        )

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 apply_expr nested keyword and map paths" do
    test "handles a map value inside a filter entry (apply_expr map branch)" do
      expected = dynamic([q], q.id != ^2 and q.id == ^1)

      actual =
        Postgres.build_dynamic(
          Post,
          {:as, nil},
          {:id, %{eq: 1, !=: 2}},
          []
        )

      assert_dynamic(expected, actual)
    end

    test "handles a keyword value inside a filter entry (apply_expr keyword branch)" do
      id1 = 1
      id2 = 2
      expected = dynamic([q], q.id == ^id1 and q.id != ^id2)

      actual =
        Postgres.build_dynamic(
          Post,
          {:as, nil},
          {:id, [eq: id1, !=: id2]},
          []
        )

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 :or merge operator in expr_entry" do
    test "merges two entries with :or when the or operator key is used" do
      id1 = 1
      id2 = 2

      expected =
        dynamic(
          [q],
          q.id == ^id1 or q.id == ^id2
        )

      actual =
        Postgres.build_dynamic(
          Post,
          {:as, nil},
          {:id, [or: {:==, id1}, or: {:==, id2}]},
          []
        )

      assert_dynamic(expected, actual)
    end
  end

  describe "build_dynamic/4 quantified_query_payload? false branch" do
    test "handles list quantifier payload that is not a keyword list with :from key" do
      vals = [1, 2, 3]

      actual =
        Postgres.build_dynamic(
          Post,
          {:as, nil},
          {:id, %{==: %{all: vals}}},
          []
        )

      refute is_nil(actual)
    end
  end
end
