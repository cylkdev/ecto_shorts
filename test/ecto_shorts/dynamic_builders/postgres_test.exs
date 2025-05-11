defmodule EctoShorts.DynamicBuilders.PostgresTest do
  use ExUnit.Case, async: true
  doctest EctoShorts.DynamicBuilders.Postgres

  alias EctoShorts.DynamicBuilders.Postgres
  alias EctoShorts.Schema.Post

  import Ecto.Query, only: [dynamic: 2]
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  describe "create_dynamic/6" do
    test "builds a dynamic expression with condition :and" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic([q], q.description == "example" and q.id == ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :id, 1)

      assert_dynamic dynamic([q], q.description == "example" or q.id == ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :id, 1)
    end

    test "builds a dynamic expression using the :== operator" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic([q], q.description == "example" and q.id == ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :id, {:==, 1})

      assert_dynamic dynamic([q], q.description == "example" or q.id == ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :id, {:==, 1})

      assert_dynamic dynamic([q], q.description == "example" and q.id in ^[1, 2, 3]),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :id, {:==, [1, 2, 3]})

      assert_dynamic dynamic([q], q.description == "example" or q.id in ^[1, 2, 3]),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :id, {:==, [1, 2, 3]})

      assert_dynamic dynamic(
                       [q],
                       q.description == "example" and fragment("LOWER(?)", q.id) == ^"example"
                     ),
                     Postgres.create_dynamic(
                       Post,
                       dyn,
                       nil,
                       :and,
                       :id,
                       {:==, {:lower, "example"}}
                     )

      assert_dynamic dynamic(
                       [q],
                       q.description == "example" or fragment("LOWER(?)", q.id) == ^"example"
                     ),
                     Postgres.create_dynamic(
                       Post,
                       dyn,
                       nil,
                       :or,
                       :id,
                       {:==, {:lower, "example"}}
                     )

      assert_dynamic dynamic(
                       [q],
                       q.description == "example" and fragment("UPPER(?)", q.id) == ^"example"
                     ),
                     Postgres.create_dynamic(
                       Post,
                       dyn,
                       nil,
                       :and,
                       :id,
                       {:==, {:upper, "example"}}
                     )

      assert_dynamic dynamic(
                       [q],
                       q.description == "example" or fragment("UPPER(?)", q.id) == ^"example"
                     ),
                     Postgres.create_dynamic(
                       Post,
                       dyn,
                       nil,
                       :or,
                       :id,
                       {:==, {:upper, "example"}}
                     )
    end

    test "builds a dynamic expression using the :!= operator" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic([q], q.description == "example" and q.id != ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :id, {:!=, 1})

      assert_dynamic dynamic([q], q.description == "example" or q.id != ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :id, {:!=, 1})

      assert_dynamic dynamic([q], q.description == "example" and q.id not in ^[1, 2, 3]),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :id, {:!=, [1, 2, 3]})

      assert_dynamic dynamic([q], q.description == "example" or q.id not in ^[1, 2, 3]),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :id, {:!=, [1, 2, 3]})
    end

    test "builds a dynamic expression using the :> operator" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic([q], q.description == "example" and q.id > ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :id, {:>, 1})

      assert_dynamic dynamic([q], q.description == "example" or q.id > ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :id, {:>, 1})
    end

    test "builds a dynamic expression using the :< operator" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic([q], q.description == "example" and q.id < ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :id, {:<, 1})

      assert_dynamic dynamic([q], q.description == "example" or q.id < ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :id, {:<, 1})
    end

    test "builds a dynamic expression using the :>= operator" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic([q], q.description == "example" and q.id >= ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :id, {:>=, 1})

      assert_dynamic dynamic([q], q.description == "example" or q.id >= ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :id, {:>=, 1})
    end

    test "builds a dynamic expression using the :<= operator" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic([q], q.description == "example" and q.id <= ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :id, {:<=, 1})

      assert_dynamic dynamic([q], q.description == "example" or q.id <= ^1),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :id, {:<=, 1})
    end

    test "builds a dynamic expression using the :ilike operator" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic([q], q.description == "example" and ilike(q.title, ^"%example%")),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :title, {:ilike, "example"})

      assert_dynamic dynamic([q], q.description == "example" or ilike(q.title, ^"%example%")),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :title, {:ilike, "example"})
    end

    test "builds a dynamic expression using the :like operator" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic([q], q.description == "example" and like(q.title, ^"%example%")),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :title, {:like, "example"})

      assert_dynamic dynamic([q], q.description == "example" or like(q.title, ^"%example%")),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :title, {:like, "example"})
    end

    test "builds a dynamic expression using the :=~ operator" do
      dyn = dynamic([q], q.description == "example")

      assert_dynamic dynamic(
                       [q],
                       q.description == "example" and fragment("? ~* ?", q.title, ^"example")
                     ),
                     Postgres.create_dynamic(Post, dyn, nil, :and, :title, {:=~, "example"})

      assert_dynamic dynamic(
                       [q],
                       q.description == "example" or fragment("? ~* ?", q.title, ^"example")
                     ),
                     Postgres.create_dynamic(Post, dyn, nil, :or, :title, {:=~, "example"})
    end
  end
end
