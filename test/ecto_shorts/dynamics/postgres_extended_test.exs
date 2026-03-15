defmodule EctoShorts.Dynamics.PostgresExtendedTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Dynamics.Postgres
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

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
