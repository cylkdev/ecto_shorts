defmodule EctoShorts.DynamicBuilders.Postgres.FieldTest do
  use ExUnit.Case, async: true
  doctest EctoShorts.DynamicBuilders.Postgres.Field

  alias EctoShorts.DynamicBuilders.Postgres.Field

  import Ecto.Query, only: [dynamic: 2]
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  describe "create_dynamic/4" do
    test "builds a dynamic expression using the :== operator" do
      assert_dynamic dynamic([q], is_nil(q.id)),
                     Field.create_dynamic(nil, :id, :==, nil)

      assert_dynamic dynamic([q], q.id == ^1),
                     Field.create_dynamic(nil, :id, :==, 1)

      assert_dynamic dynamic([q], q.id in ^[1, 2, 3]),
                     Field.create_dynamic(nil, :id, :==, [1, 2, 3])

      assert_dynamic dynamic([q], fragment("LOWER(?)", q.title) == ^"example"),
                     Field.create_dynamic(nil, :title, :==, {:lower, "example"})

      assert_dynamic dynamic([q], fragment("UPPER(?)", q.title) == ^"example"),
                     Field.create_dynamic(nil, :title, :==, {:upper, "example"})
    end

    test "builds a dynamic expression using the :!= operator" do
      assert_dynamic dynamic([q], not is_nil(q.id)),
                     Field.create_dynamic(nil, :id, :!=, nil)

      assert_dynamic dynamic([q], q.id != ^1),
                     Field.create_dynamic(nil, :id, :!=, 1)

      assert_dynamic dynamic([q], q.id not in ^[1, 2, 3]),
                     Field.create_dynamic(nil, :id, :!=, [1, 2, 3])

      assert_dynamic dynamic([q], fragment("LOWER(?)", q.title) != ^"example"),
                     Field.create_dynamic(nil, :title, :!=, {:lower, "example"})

      assert_dynamic dynamic([q], fragment("UPPER(?)", q.title) != ^"example"),
                     Field.create_dynamic(nil, :title, :!=, {:upper, "example"})
    end

    test "builds a dynamic expression using the :> operator" do
      assert_dynamic dynamic([q], q.id > ^1),
                     Field.create_dynamic(nil, :id, :>, 1)
    end

    test "builds a dynamic expression using the :< operator" do
      assert_dynamic dynamic([q], q.id < ^1),
                     Field.create_dynamic(nil, :id, :<, 1)
    end

    test "builds a dynamic expression using the :>= operator" do
      assert_dynamic dynamic([q], q.id >= ^1),
                     Field.create_dynamic(nil, :id, :>=, 1)
    end

    test "builds a dynamic expression using the :<= operator" do
      assert_dynamic dynamic([q], q.id <= ^1),
                     Field.create_dynamic(nil, :id, :<=, 1)
    end

    test "builds a dynamic expression using the :ilike operator" do
      assert_dynamic dynamic([q], ilike(q.title, ^"%example%")),
                     Field.create_dynamic(nil, :title, :ilike, "example")
    end

    test "builds a dynamic expression using the :like operator" do
      assert_dynamic dynamic([q], like(q.title, ^"%example%")),
                     Field.create_dynamic(nil, :title, :like, "example")
    end

    test "builds a dynamic expression using the :=~ operator" do
      assert_dynamic dynamic([q], fragment("? ~* ?", q.title, ^"example")),
                     Field.create_dynamic(nil, :title, :=~, "example")
    end
  end
end
