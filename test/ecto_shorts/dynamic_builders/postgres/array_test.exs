defmodule EctoShorts.DynamicBuilders.Postgres.ArrayTest do
  use ExUnit.Case, async: true
  doctest EctoShorts.DynamicBuilders.Postgres.Array

  alias EctoShorts.DynamicBuilders.Postgres.Array

  import Ecto.Query, only: [dynamic: 2]
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  describe "create_dynamic/4" do
    test "builds a dynamic expression using the :== operator" do
      assert_dynamic dynamic([q], ^1 in q.tags),
                     Array.create_dynamic(nil, 1, :==, :tags)

      assert_dynamic dynamic([q], q.tags == ^["example"]),
                     Array.create_dynamic(nil, :tags, :==, ["example"])

      assert_dynamic dynamic([q], is_nil(q.tags)),
                     Array.create_dynamic(nil, :tags, :==, nil)

      assert_dynamic dynamic(
                       [q],
                       fragment(
                         "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE UPPER(value) = UPPER(?))",
                         q.tags,
                         ^"example"
                       )
                     ),
                     Array.create_dynamic(nil, :tags, :==, {:upper, "example"})
    end

    test "builds a dynamic expression using the :!= operator" do
      assert_dynamic dynamic([q], ^1 not in q.tags),
                     Array.create_dynamic(nil, 1, :!=, :tags)

      assert_dynamic dynamic([q], q.tags != ^["example"]),
                     Array.create_dynamic(nil, :tags, :!=, ["example"])

      assert_dynamic dynamic([q], not is_nil(q.tags)),
                     Array.create_dynamic(nil, :tags, :!=, nil)

      assert_dynamic dynamic(
                       [q],
                       fragment(
                         "EXISTS (SELECT 1 FROM unnest(?) AS value WHERE UPPER(value) != UPPER(?))",
                         q.tags,
                         ^"example"
                       )
                     ),
                     Array.create_dynamic(nil, :tags, :!=, {:upper, "example"})
    end

    test "builds a dynamic expression using the :> operator" do
      assert_dynamic dynamic([q], q.tags > ^["example"]),
                     Array.create_dynamic(nil, :tags, :>, ["example"])

      assert_dynamic dynamic([q], fragment("? > ANY(?)", ^["example"], q.tags)),
                     Array.create_dynamic(nil, ["example"], :>, :tags)
    end

    test "builds a dynamic expression using the :< operator" do
      assert_dynamic dynamic([q], q.tags < ^["example"]),
                     Array.create_dynamic(nil, :tags, :<, ["example"])

      assert_dynamic dynamic([q], fragment("? < ANY(?)", ^["example"], q.tags)),
                     Array.create_dynamic(nil, ["example"], :<, :tags)
    end

    test "builds a dynamic expression using the :>= operator" do
      assert_dynamic dynamic([q], q.tags >= ^["example"]),
                     Array.create_dynamic(nil, :tags, :>=, ["example"])

      assert_dynamic dynamic([q], fragment("? >= ANY(?)", ^["example"], q.tags)),
                     Array.create_dynamic(nil, ["example"], :>=, :tags)
    end

    test "builds a dynamic expression using the :<= operator" do
      assert_dynamic dynamic([q], q.tags <= ^["example"]),
                     Array.create_dynamic(nil, :tags, :<=, ["example"])

      assert_dynamic dynamic([q], fragment("? <= ANY(?)", ^["example"], q.tags)),
                     Array.create_dynamic(nil, ["example"], :<=, :tags)
    end

    test "builds a dynamic expression using the :ilike operator" do
      assert_dynamic dynamic([q], fragment("? ILIKE ANY(?)", ^"%example%", q.tags)),
                     Array.create_dynamic(nil, "example", :ilike, :tags)
    end

    test "builds a dynamic expression using the :like operator" do
      assert_dynamic dynamic([q], fragment("? LIKE ANY(?)", ^"%example%", q.tags)),
                     Array.create_dynamic(nil, "example", :like, :tags)
    end

    test "builds a dynamic expression using the :=~ operator" do
      assert_dynamic dynamic([q], fragment("? ~* ANY(?)", ^"example", q.tags)),
                     Array.create_dynamic(nil, "example", :=~, :tags)
    end
  end
end
