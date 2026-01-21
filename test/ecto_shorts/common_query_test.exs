defmodule EctoShorts.CommonQueryTest do
  use EctoShorts.DataCase

  import Ecto.Query

  alias EctoShorts.CommonQuery
  alias EctoShorts.Schema.Comment
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.PostHasSchemaPrefix
  alias EctoShorts.Schema.User

  describe "get_query_source/1" do
    test "returns {table, nil} for bare table source" do
      q = from(u in "users")
      assert CommonQuery.get_query_source(q) == {"users", nil}
    end

    test "returns {schema_source, schema} for schema queryable" do
      assert CommonQuery.get_query_source(User) == {"users", User}
    end

    test "returns custom source tuple when query is built from {source, schema} tuple" do
      q = from(u in {"custom_users", User})
      assert CommonQuery.get_query_source(q) == {"custom_users", User}
    end

    test "returns source tuple from composed query" do
      q = User |> where([u], u.age > 0) |> select([u], u.id)
      assert CommonQuery.get_query_source(q) == {"users", User}
    end

    test "returns source tuple for subquery (inner source)" do
      inner = from(u in User, where: u.age > 0)
      q = from(u in subquery(inner))

      assert CommonQuery.get_query_source(q) == {"users", User}
    end
  end

  describe "get_query_prefix/1" do
    test "returns nil when query has no prefix" do
      assert CommonQuery.get_query_prefix(User) == nil
    end

    test "returns schema prefix when schema defines @schema_prefix" do
      assert CommonQuery.get_query_prefix(PostHasSchemaPrefix) == "custom_schema_prefix"
    end

    test "returns query prefix when explicitly set" do
      q = from(p in Post, prefix: "explicit_prefix")
      assert CommonQuery.get_query_prefix(q) == "explicit_prefix"
    end

    test "returns query prefix when explicitly set on subquery" do
      inner = from(p in Post, prefix: "inner_prefix")
      q = from(p in subquery(inner))

      assert CommonQuery.get_query_prefix(q) == "inner_prefix"
    end
  end

  describe "query_binding_count/1" do
    test "counts from binding as 1" do
      assert CommonQuery.query_binding_count(User) == 1
    end

    test "counts joins" do
      q =
        from(u in User,
          join: p in assoc(u, :posts),
          join: c in assoc(p, :comments)
        )

      assert CommonQuery.query_binding_count(q) == 3
    end
  end

  describe "get_query_binding_source/2" do
    test "resolves :as for from binding" do
      q = from(u in User, as: :user)
      assert CommonQuery.get_query_binding_source(q, :user) == {"users", User}
    end

    test "resolves join binding by position" do
      q = from(u in User, as: :user, join: p in Post, as: :post, on: true)

      assert CommonQuery.get_query_binding_source(q, 1) == {"users", User}
      assert CommonQuery.get_query_binding_source(q, 2) == {nil, Post}
    end

    test "resolves join binding by alias" do
      q = from(u in User, as: :user, join: p in Post, as: :post, on: true)

      assert CommonQuery.get_query_binding_source(q, :post) == {nil, Post}
    end

    test "returns nil for unknown alias" do
      q = from(u in User, as: :user)
      assert CommonQuery.get_query_binding_source(q, :does_not_exist) == nil
    end

    test "returns nil when binding position is out of range" do
      q = from(u in User, as: :user)
      assert CommonQuery.get_query_binding_source(q, 2) == nil
    end

    test "supports negative index binding for last join" do
      q =
        from(u in User,
          join: p in Post,
          on: true,
          join: c in Comment,
          on: true
        )

      assert CommonQuery.get_query_binding_source(q, -1) == {nil, Comment}
    end

    test "resolves assoc join source as {nil, RelatedSchema} (pos 0)" do
      q =
        from(u in User,
          as: :user,
          join: p in assoc(u, :posts),
          as: :post
        )

      assert CommonQuery.get_query_binding_source(q, :post) == {nil, Post}
      assert CommonQuery.get_query_binding_source(q, 2) == {nil, Post}
    end

    test "resolves assoc join source through another assoc join (pos > 0)" do
      q =
        from(u in User,
          as: :user,
          join: p in assoc(u, :posts),
          as: :post,
          join: c in assoc(p, :comments),
          as: :comment
        )

      assert CommonQuery.get_query_binding_source(q, :comment) == {nil, Comment}
      assert CommonQuery.get_query_binding_source(q, 3) == {nil, Comment}
    end

    test "resolves assoc join binding on a subquery's inner query" do
      inner =
        from(u in User,
          as: :user,
          join: p in assoc(u, :posts),
          as: :post
        )

      q = from(u in subquery(inner))

      assert CommonQuery.get_query_binding_source(q, :post) == {nil, Post}
    end

    test "returns {table, nil} for bare table from with alias" do
      q = from(u in "users", as: :user)
      assert CommonQuery.get_query_binding_source(q, :user) == {"users", nil}
    end
  end
end
