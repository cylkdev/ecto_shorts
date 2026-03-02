defmodule EctoShorts.CommonQueryTest do
  use EctoShorts.DataCase, async: true

  import Ecto.Query

  alias EctoShorts.CommonQuery
  alias EctoShorts.Schema.Comment
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.PostHasSchemaPrefix
  alias EctoShorts.Schema.User

  describe "get_query_source/1" do
    test "returns the table name without a schema for a bare table query" do
      q = from(u in "users")
      assert CommonQuery.get_query_source(q) === {"users", nil}
    end

    test "returns the table name and schema module for a schema query" do
      assert CommonQuery.get_query_source(User) === {"users", User}
    end

    test "returns the custom table name when the query uses a tuple source" do
      q = from(u in {"custom_users", User})
      assert CommonQuery.get_query_source(q) === {"custom_users", User}
    end

    test "returns the source even after adding where and select clauses" do
      q = User |> where([u], u.age > 0) |> select([u], u.id)
      assert CommonQuery.get_query_source(q) === {"users", User}
    end

    test "returns the inner source when the query wraps a subquery" do
      inner = from(u in User, where: u.age > 0)
      q = from(u in subquery(inner))

      assert CommonQuery.get_query_source(q) === {"users", User}
    end
  end

  describe "get_query_prefix/1" do
    test "returns nil when no prefix is set" do
      assert CommonQuery.get_query_prefix(User) === nil
    end

    test "returns the schema prefix when the schema defines one" do
      assert CommonQuery.get_query_prefix(PostHasSchemaPrefix) === "custom_schema_prefix"
    end

    test "returns the prefix set directly on the query" do
      q = from(p in Post, prefix: "explicit_prefix")
      assert CommonQuery.get_query_prefix(q) === "explicit_prefix"
    end

    test "returns the prefix from the inner subquery" do
      inner = from(p in Post, prefix: "inner_prefix")
      q = from(p in subquery(inner))

      assert CommonQuery.get_query_prefix(q) === "inner_prefix"
    end
  end

  describe "query_binding_count/1" do
    test "counts one binding for a plain schema query" do
      assert CommonQuery.query_binding_count(User) === 1
    end

    test "counts each join as an additional binding" do
      q =
        from(u in User,
          join: p in assoc(u, :posts),
          join: c in assoc(p, :comments)
        )

      assert CommonQuery.query_binding_count(q) === 3
    end
  end

  describe "get_query_binding_source/2" do
    test "finds the source for a named from binding" do
      q = from(u in User, as: :user)
      assert CommonQuery.get_query_binding_source(q, :user) === {"users", User}
    end

    test "finds the source for a join by its position number" do
      q = from(u in User, as: :user, join: p in Post, as: :post, on: true)

      assert CommonQuery.get_query_binding_source(q, 1) === {"users", User}
      assert CommonQuery.get_query_binding_source(q, 2) === {nil, Post}
    end

    test "finds the source for a join by its alias name" do
      q = from(u in User, as: :user, join: p in Post, as: :post, on: true)

      assert CommonQuery.get_query_binding_source(q, :post) === {nil, Post}
    end

    test "returns nil when the alias does not exist" do
      q = from(u in User, as: :user)
      assert CommonQuery.get_query_binding_source(q, :does_not_exist) === nil
    end

    test "returns nil when the position is out of range" do
      q = from(u in User, as: :user)
      assert CommonQuery.get_query_binding_source(q, 2) === nil
    end

    test "finds the last join using a negative position" do
      q =
        from(u in User,
          join: p in Post,
          on: true,
          join: c in Comment,
          on: true
        )

      assert CommonQuery.get_query_binding_source(q, -1) === {nil, Comment}
    end

    test "finds the related schema for an association join" do
      q =
        from(u in User,
          as: :user,
          join: p in assoc(u, :posts),
          as: :post
        )

      assert CommonQuery.get_query_binding_source(q, :post) === {nil, Post}
      assert CommonQuery.get_query_binding_source(q, 2) === {nil, Post}
    end

    test "follows a chain of association joins to find the final schema" do
      q =
        from(u in User,
          as: :user,
          join: p in assoc(u, :posts),
          as: :post,
          join: c in assoc(p, :comments),
          as: :comment
        )

      assert CommonQuery.get_query_binding_source(q, :comment) === {nil, Comment}
      assert CommonQuery.get_query_binding_source(q, 3) === {nil, Comment}
    end

    test "finds an association join inside a subquery" do
      inner =
        from(u in User,
          as: :user,
          join: p in assoc(u, :posts),
          as: :post
        )

      q = from(u in subquery(inner))

      assert CommonQuery.get_query_binding_source(q, :post) === {nil, Post}
    end

    test "returns the table name without a schema for a named bare table query" do
      q = from(u in "users", as: :user)
      assert CommonQuery.get_query_binding_source(q, :user) === {"users", nil}
    end
  end
end
