defmodule EctoShorts.CommonFiltersTest do
  use ExUnit.Case
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.User

  import Ecto.Query
  import ExUnit.CaptureLog

  require Logger

  describe "convert_params_to_filter/3" do
    test "supports map of params" do
      expected = from p in Post, where: p.id == ^1
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{id: 1}, [])

      assert_sql(expected, q2)
    end

    test "supports keyword list of params" do
      expected = from p in Post, where: p.id == ^1
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, [id: 1], [])

      assert_sql(expected, q2)
    end

    test "supports list of maps of params" do
      expected = from p in Post, where: p.id == ^1
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, [%{id: 1}], [])

      assert_sql(expected, q2)
    end

    test "supports list of keywords of params" do
      expected = from p in Post, where: p.id == ^1
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, [[id: 1]], [])

      assert_sql(expected, q2)
    end

    test "defaults to :where filter when filter not provided" do
      expected = from p in Post, where: p.published == ^true
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published: true}, [])

      assert_sql(expected, q2)
    end

    test "supports Ecto.Query.t() source" do
      expected = from p in Post, where: p.published == ^true
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{published: true}, [])

      assert_sql(expected, q2)
    end

    test "supports binary() table name source" do
      expected = from p in "posts", select: [:id]
      q2 = CommonFilters.convert_params_to_filter("posts", %{select: [:id]}, [])

      assert_sql(expected, q2)
    end

    test "supports Ecto.Schema.t() schema module source" do
      expected = from p in Post, select: p
      q2 = CommonFilters.convert_params_to_filter(Post, %{select: true}, [])

      assert_sql(expected, q2)
    end

    test "supports {binary(), Ecto.Schema.t()} source" do
      expected = from p in {"custom_posts", Post}, select: p
      q2 = CommonFilters.convert_params_to_filter({"custom_posts", Post}, %{select: true}, [])

      assert_sql(expected, q2)
    end

    test "supports {nil, Ecto.Schema.t()} source" do
      expected = from p in Post, select: p
      q2 = CommonFilters.convert_params_to_filter({nil, Post}, %{select: true}, [])

      assert_sql(expected, q2)
    end

    test "supports {binary(), nil} source" do
      expected = from p in "posts", select: [:id]
      q2 = CommonFilters.convert_params_to_filter({"posts", nil}, %{select: [:id]}, [])

      assert_sql(expected, q2)
    end

    test "invalid binding params logs error and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{as: 123}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected value for binding selector to be a map or keyword list, got: 123"
      assert_received {:q2, q2}
      assert q2 == q
    end

    test "supports binding selector with explicit operator tuple" do
      q = from p in Post, as: :post

      expected =
        from(p in Post,
          as: :post,
          where: p.published == ^true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{as: %{post: %{published: %{==: true}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :where supports list of params for a field" do
      q = from p in Post, as: :post

      expected = from p in Post, as: :post, where: p.published in ^[true, false]

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{as: %{post: %{published: [true, false]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :select supports non-tuple select value" do
      q = from p in Post, as: :post

      expected = from p in Post, as: :post, select: p.id

      q2 = CommonFilters.convert_params_to_filter(q, %{as: %{post: %{select: :id}}}, [])

      assert_sql(expected, q2)
    end

    test "binding selector with non-map params logs error and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{as: %{post: 123}}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected params to be a map or keyword list, got: 123"
      assert_received {:q2, q2}
      assert q2 == q
    end

    test "raises an error if top-level params is not a map or keyword list" do
      q = from(p in Post)

      assert_raise ArgumentError,
                   "Expected params to be a map or list, got: %{123 => \"oops\"}",
                   fn ->
                     CommonFilters.convert_params_to_filter(q, %{123 => "oops"}, [])
                   end
    end

    test "supports :select true (selects the binding)" do
      expected = from p in Post, select: p
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{select: true}, [])

      assert_sql(expected, q2)
    end

    test "supports :select for a single field" do
      expected = from p in Post, select: p.id
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: :id}, [])

      assert_sql(expected, q2)
    end

    test "supports :select with a non-keyword list (selects the literal list)" do
      expected = from p in Post, select: ^[:id, :title]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: [:id, :title]}, [])

      assert_sql(expected, q2)
    end

    test "supports :select {:map, map} for custom field aliases" do
      expected = from p in Post, select: %{custom_id: p.id}
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: %{map: %{custom_id: :id}}}, [])

      assert_sql(expected, q2)
    end

    test "supports :select {:map, fields} (Ecto map/2)" do
      expected = from p in Post, select: map(p, [:id, :title])
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: %{map: [:id, :title]}}, [])

      assert_sql(expected, q2)
    end

    test "supports :select {:struct, fields} (Ecto struct/2)" do
      expected = from p in Post, select: struct(p, [:id])
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: %{struct: [:id]}}, [])

      assert_sql(expected, q2)
    end

    test "supports :select_merge {:map, map} for custom field aliases" do
      expected = from p in Post, select_merge: %{custom_id: p.id}
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{select_merge: %{map: %{custom_id: :id}}}, [])

      assert_sql(expected, q2)
    end

    test "supports :select_merge {:map, fields} (Ecto map/2)" do
      expected = from p in Post, select_merge: map(p, [:id, :title])
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select_merge: %{map: [:id, :title]}}, [])

      assert_sql(expected, q2)
    end

    test "supports :select with :select_merge in one query" do
      expected = from p in Post, select: map(p, [:id]), select_merge: %{post_title: p.title}

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [select: %{map: [:id]}, select_merge: %{map: %{post_title: :title}}],
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :select_merge supports non-tuple select_merge value" do
      q = from p in Post, as: :post

      expected = from p in Post, as: :post, select_merge: %{custom_id: p.id}

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{as: %{post: %{select_merge: %{map: %{custom_id: :id}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports plain :preload list" do
      expected = from p in Post, preload: [:author]
      q2 = CommonFilters.convert_params_to_filter(Post, %{preload: [:author]}, [])

      assert_query(expected, q2)
    end

    test "supports plain nested :preload keyword list" do
      expected = from p in Post, preload: [author: [:posts]]
      q2 = CommonFilters.convert_params_to_filter(Post, %{preload: [author: [:posts]]}, [])

      assert_query(expected, q2)
    end

    test "supports binding-aware :preload with named binding selector" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          preload: [author: a]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{preload: [author: [binding: [as: :author]]]},
          []
        )

      assert_query(expected, q2)
    end

    test "supports binding-aware :preload with positional binding selector" do
      q =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          preload: [author: a]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{preload: [author: [binding: [at: 2]]]},
          []
        )

      assert_query(expected, q2)
    end

    test "supports mixed selector+nested preload payload in one assoc entry" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          preload: [author: {a, [posts: [:comments]]}]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{preload: [author: [posts: [:comments], binding: [as: :author]]]},
          []
        )

      assert_query(expected, q2)
    end

    test "supports mixed plain and binding-aware :preload entries" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          preload: [:comments, author: a]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{preload: [:comments, author: [binding: [as: :author]]]},
          []
        )

      assert_query(expected, q2)
    end

    test "malformed preload binding selector logs warning and skips entry" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{preload: [author: [binding: :author]]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :binding to be [as: atom()] or [at: integer()], got: :author"
      assert_received {:q2, q2}
      assert_query(q, q2)
    end

    test "missing preload binding alias raises Ecto.QueryError" do
      q = from(p in Post)

      assert_raise Ecto.QueryError, ~r/unknown bind name `:missing`/, fn ->
        CommonFilters.convert_params_to_filter(
          q,
          %{preload: [author: [binding: [as: :missing]]]},
          []
        )
      end
    end

    test "supports LOWER operator for scalar fields" do
      expected = from p in Post, where: fragment("lower(?)", p.title) == ^"hello"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{lower: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for scalar fields" do
      expected = from p in Post, where: fragment("upper(?)", p.title) == ^"HELLO"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{upper: "HELLO"}}, [])

      assert_sql(expected, q2)
    end

    test "supports LOWER operator for scalar fields via explicit ==" do
      expected = from p in Post, where: fragment("lower(?)", p.title) == ^"hello"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{==: {:lower, "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for scalar fields via explicit ==" do
      expected = from p in Post, where: fragment("upper(?)", p.title) == ^"HELLO"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{==: {:upper, "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports LOWER operator for scalar fields via explicit !=" do
      expected = from p in Post, where: fragment("lower(?)", p.title) != ^"hello"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{!=: {:lower, "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for scalar fields via explicit !=" do
      expected = from p in Post, where: fragment("upper(?)", p.title) != ^"HELLO"
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{!=: {:upper, "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated LOWER operator for scalar fields" do
      expected = from p in Post, where: not (fragment("lower(?)", p.title) == ^"hello")
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{title: %{not: %{lower: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated UPPER operator for scalar fields" do
      expected = from p in Post, where: not (fragment("upper(?)", p.title) == ^"HELLO")
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{title: %{not: %{upper: "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports LIKE operator for array fields with list RHS (EXISTS ... LIKE ANY)" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t LIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{tags: %{like: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "supports ILIKE operator for array fields with list RHS (EXISTS ... ILIKE ANY)" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t ILIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{tags: %{ilike: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "supports LOWER operator for array fields" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE lower(t) = ?
              )
              """,
              p.tags,
              ^"elixir"
            )
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{lower: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for array fields" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE upper(t) = ?
              )
              """,
              p.tags,
              ^"ELIXIR"
            )
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{upper: "ELIXIR"}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated LOWER operator for array fields" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE lower(t) = ?
              )
              """,
              p.tags,
              ^"elixir"
            )
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{not: %{lower: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated UPPER operator for array fields" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE upper(t) = ?
              )
              """,
              p.tags,
              ^"ELIXIR"
            )
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{not: %{upper: "ELIXIR"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports :limit" do
      expected = from p in Post, limit: ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{limit: 10}, [])

      assert_sql(expected, q2)
    end

    test "supports :offset" do
      expected = from p in Post, offset: ^5
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{offset: 5}, [])

      assert_sql(expected, q2)
    end

    test "supports :last" do
      expected =
        Post
        |> exclude(:order_by)
        |> from(order_by: [desc: :id], limit: ^2)
        |> subquery()
        |> order_by(:id)

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{last: 2}, [])

      assert_sql(expected, q2)
    end

    test "supports :limit and :offset combined with where" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          limit: ^10,
          offset: ^5
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{
            published: true,
            limit: 10,
            offset: 5
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "preserves struct values (DateTime) for scalar comparisons" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.published_at == ^dt

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{published_at: dt},
          []
        )

      assert_sql(expected, q2)
    end

    test "preserves struct values (DateTime) for operator map comparisons" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.published_at >= ^dt

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{published_at: %{>=: dt}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports == nil comparisons (generates IS NULL)" do
      expected = from p in Post, where: is_nil(p.published_at)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published_at: nil}, [])

      assert_sql(expected, q2)
    end

    test "supports != nil comparisons (generates IS NOT NULL)" do
      expected = from p in Post, where: not is_nil(p.published_at)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published_at: %{!=: nil}}, [])

      assert_sql(expected, q2)
    end

    test "supports == nil comparisons for array fields (generates IS NULL)" do
      expected = from p in Post, where: is_nil(p.tags)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: nil}, [])

      assert_sql(expected, q2)
    end

    test "supports != nil comparisons for array fields (generates IS NOT NULL)" do
      expected = from p in Post, where: not is_nil(p.tags)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{!=: nil}}, [])

      assert_sql(expected, q2)
    end

    test "invalid nil operator raises helpful error" do
      assert_raise ArgumentError,
                   "Expected the operator to be one of [:eq, :==, :!=] for nil comparison, got: :>",
                   fn ->
                     CommonFilters.convert_params_to_filter(Post, %{published_at: %{>: nil}}, [])
                   end
    end

    test "supports multiple filters (where and or_where)" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          or_where: p.published == ^false
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{
            where: %{published: true},
            or_where: %{published: false}
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports positional binding selector via :at" do
      expected = from p in Post, where: p.published == ^true
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{at: %{1 => %{published: true}}}, [])

      assert_sql(expected, q2)
    end

    test "invalid :at binding selector key logs error and is skipped" do
      expected = from p in Post, where: p.published == ^true
      q = Post

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{
                at: %{
                  "1" => %{published: false},
                  1 => %{published: true}
                }
              },
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~
               "Expected binding selector to be one of {:as, atom()} or {:at, integer()}, got: {:at, \"1\"}"

      assert_received {:q2, q2}
      assert_sql(expected, q2)
    end

    test "supports named binding selector via :as" do
      expected = from p in Post, as: :post, where: p.published == ^true
      q = from p in Post, as: :post
      q2 = CommonFilters.convert_params_to_filter(q, %{as: %{post: %{published: true}}}, [])

      assert_sql(expected, q2)
    end

    test "supports filtering on both root field and association field via :as" do
      expected =
        from(p in Post,
          as: :post,
          join: a in assoc(p, :author),
          as: :author,
          where: p.published == ^true,
          where: a.first_name == ^"John"
        )

      q =
        from(p in Post,
          as: :post,
          join: a in assoc(p, :author),
          as: :author
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{
            as: [
              post: %{published: true},
              author: %{first_name: "John"}
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports joining an association via association key params" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          where: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: [as: :author, first_name: "John"]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join association entry" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{join: [author: [as: :author]]}, [])

      assert_sql(expected, q2)
    end

    test "supports canonical :join schema source entry" do
      expected =
        from(p in Post,
          join: u in User,
          as: :user_join,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [schema: [source: User, as: :user_join, on: true]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join table source entry" do
      expected =
        from(p in Post,
          join: u in "users",
          as: :users_table,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [table: [source: "users", as: :users_table, on: true]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join query source entry" do
      user_query = from(u in User, where: u.age >= ^18)

      expected =
        from(p in Post,
          join: u in ^user_query,
          as: :adult_users,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [query: [source: user_query, as: :adult_users, on: true]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join subquery source entry" do
      user_query = from(u in User, where: u.age >= ^18)

      expected =
        from(p in Post,
          join: u in subquery(user_query),
          as: :adult_users_subquery,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [subquery: [source: user_query, as: :adult_users_subquery, on: true]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join subquery source params composed into a query" do
      expected_subquery = from(u in User, where: u.age >= ^18)

      expected =
        from(p in Post,
          join: u in subquery(expected_subquery),
          as: :adult_users_subquery,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              subquery: [
                source: [
                  from: User,
                  query: [age: [>=: 18]]
                ],
                as: :adult_users_subquery,
                on: true
              ]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join subquery source params without :from using existing source" do
      expected_subquery = from(p in Post, where: p.published == ^true)

      expected =
        from(p in Post,
          join: s in subquery(expected_subquery),
          as: :published_posts_subquery,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              subquery: [
                source: [query: [published: true]],
                as: :published_posts_subquery,
                on: true
              ]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join list of entries and preserves order" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          join: u in "users",
          as: :users_table,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              author: [as: :author],
              table: [source: "users", as: :users_table, on: true]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join under named binding selector" do
      q = from(p in Post, as: :post)

      expected =
        from(p in Post,
          as: :post,
          join: a in assoc(p, :author),
          as: :author
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{as: %{post: %{join: [author: [as: :author]]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports join source key dispatch through canonical :join source entry" do
      previous = Application.get_env(:ecto_shorts, :join_source_module)

      on_exit(fn ->
        Application.put_env(:ecto_shorts, :join_source_module, previous)
      end)

      Application.put_env(:ecto_shorts, :join_source_module, EctoShorts.TestJoinSources)

      expected_source_query =
        from(u in fragment("SELECT * FROM users WHERE age >= ?", ^21), select: u)

      expected =
        from(p in Post,
          join: a in ^expected_source_query,
          as: :active_users,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: %{name: :active_users, values: [min_age: 21]},
                as: :active_users,
                on: true
              ]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports join source key dispatch through runtime :join_source_module option" do
      expected_source_query =
        from(u in fragment("SELECT * FROM users WHERE age >= ?", ^21), select: u)

      expected =
        from(p in Post,
          join: a in ^expected_source_query,
          as: :active_users,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: %{name: :active_users, values: [min_age: 21]},
                as: :active_users,
                on: true
              ]
            ]
          },
          join_source_module: EctoShorts.TestJoinSources
        )

      assert_sql(expected, q2)
    end

    test "supports join hint key resolution through runtime :join_source_module option" do
      expected_source_query =
        from(u in fragment("SELECT * FROM users WHERE age >= ?", ^21), select: u)

      expected =
        from(p in Post,
          join: a in ^expected_source_query,
          as: :active_users,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: %{name: :active_users, values: [min_age: 21]},
                hints: :users_age_index,
                as: :active_users,
                on: true
              ]
            ]
          },
          join_source_module: EctoShorts.TestJoinSources
        )

      assert_sql(expected, q2)
      assert [%Ecto.Query.JoinExpr{hints: ["USE INDEX(users_age_index)"]}] = q2.joins
    end

    test "unknown join source key logs warning and skips entry" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{
                join: [
                  fragment: [
                    source: %{name: :unknown_key, values: [min_age: 18]},
                    as: :x,
                    on: true
                  ]
                ]
              },
              join_source_module: EctoShorts.TestJoinSources
            )

          send(self(), {:q2, q2})
        end)

      assert log =~
               "Join source callback returned error for key :unknown_key: :unsupported_fragment_key"

      assert_received {:q2, q2}
      assert_sql(q, q2)
    end

    test "malformed join source payload logs warning and skips entry" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{
                join: [
                  fragment: [
                    source: %{name: :active_users, values: [a: [x: 1], b: [y: 2]]},
                    as: :x,
                    on: true
                  ]
                ]
              },
              join_source_module: EctoShorts.TestJoinSources
            )

          send(self(), {:q2, q2})
        end)

      assert log =~
               "Join source callback returned error for key :active_users: {:missing_or_invalid, :min_age}"

      assert_received {:q2, q2}
      assert_sql(q, q2)
    end

    test "mixed valid and invalid canonical :join entries applies valid entries" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              Post,
              %{join: [[author: [as: :author]], 123]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :join params to be a map or keyword list, got: 123"
      assert_received {:q2, q2}
      assert_sql(expected, q2)
    end

    test "supports explicit operator" do
      expected = from p in Post, where: p.published != ^true
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published: %{!=: true}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated explicit operator" do
      expected = from p in Post, where: p.published != ^true
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{not: %{==: true}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated explicit operator (not !=)" do
      expected = from p in Post, where: p.published == ^true
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{not: %{!=: true}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports explicit IN operator for scalar fields" do
      expected = from p in Post, where: p.published in ^[true, false]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published: %{in: [true, false]}}, [])

      assert_sql(expected, q2)
    end

    test "supports explicit NOT IN operator for scalar fields" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{published: %{not: %{in: [true, false]}}}, [])

      assert_sql(expected, q2)
    end

    test "supports LIKE operator for scalar fields" do
      expected = from p in Post, where: like(p.title, ^"%hello%")
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{like: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "supports ILIKE operator for scalar fields" do
      expected = from p in Post, where: ilike(p.title, ^"%hello%")
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{ilike: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "supports LIKE operator for scalar fields with list RHS (LIKE ANY)" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: fragment("? LIKE ANY(?)", p.title, ^patterns)
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{like: ["hello", "world"]}}, [])

      assert_sql(expected, q2)
    end

    test "supports ILIKE operator for scalar fields with list RHS (ILIKE ANY)" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: fragment("? ILIKE ANY(?)", p.title, ^patterns)
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{ilike: ["hello", "world"]}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated LIKE operator for scalar fields" do
      expected = from p in Post, where: not like(p.title, ^"%hello%")
      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{not: %{like: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated ILIKE operator for scalar fields" do
      expected = from p in Post, where: not ilike(p.title, ^"%hello%")
      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{title: %{not: %{ilike: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated LIKE operator for scalar fields with list RHS (NOT LIKE ANY)" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: not fragment("? LIKE ANY(?)", p.title, ^patterns)
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{title: %{not: %{like: ["hello", "world"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated ILIKE operator for scalar fields with list RHS (NOT ILIKE ANY)" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: not fragment("? ILIKE ANY(?)", p.title, ^patterns)
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{title: %{not: %{ilike: ["hello", "world"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "coerces != with list RHS to NOT IN for scalar fields" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{!=: [true, false]}},
          []
        )

      assert_sql(expected, q2)
    end

    test "coerces not == with list RHS to NOT IN for scalar fields" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{not: %{==: [true, false]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "coerces not != with list RHS to IN for scalar fields" do
      expected = from p in Post, where: p.published in ^[true, false]
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{published: %{not: %{!=: [true, false]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports > comparison for scalar fields" do
      expected = from p in Post, where: p.views > ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{>: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports >= comparison for scalar fields" do
      expected = from p in Post, where: p.views >= ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{>=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports < comparison for scalar fields" do
      expected = from p in Post, where: p.views < ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{<: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports <= comparison for scalar fields" do
      expected = from p in Post, where: p.views <= ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{<=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated > comparison for scalar fields" do
      expected = from p in Post, where: not (p.views > ^10)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{not: %{>: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "array field supports overlaps-any via :in with list RHS" do
      expected =
        from(p in Post,
          where: fragment("? && ?", p.tags, ^["elixir"])
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{in: ["elixir"]}}, [])

      assert_sql(expected, q2)
    end

    test "supports LIKE operator for array fields (casts to text)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t LIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{like: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports ILIKE operator for array fields (casts to text)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t ILIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{ilike: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated LIKE operator for array fields (casts to text)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t LIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{like: "elixir"}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated ILIKE operator for array fields (casts to text)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t ILIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{ilike: "elixir"}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated LIKE operator for array fields with list RHS (NOT EXISTS ... LIKE ANY)" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t LIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{like: ["elixir", "erlang"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated ILIKE operator for array fields with list RHS (NOT EXISTS ... ILIKE ANY)" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t ILIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{ilike: ["elixir", "erlang"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports NOT overlap via not in with list RHS" do
      expected =
        from(p in Post,
          where: not fragment("? && ?", p.tags, ^["elixir"])
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{in: ["elixir"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports contains-all via in: %{all: list}" do
      expected =
        from(p in Post,
          where: fragment("? @> ?", p.tags, ^["elixir", "erlang"])
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{in: %{all: ["elixir", "erlang"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports negated contains-all via not in: %{all: list}" do
      expected =
        from(p in Post,
          where: not fragment("? @> ?", p.tags, ^["elixir", "erlang"])
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{in: %{all: ["elixir", "erlang"]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports membership via :in with scalar RHS" do
      expected =
        from(p in Post,
          where: ^"elixir" in p.tags
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{in: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field defaults scalar RHS to membership (== value becomes in value)" do
      expected =
        from(p in Post,
          where: ^"elixir" in p.tags
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: "elixir"}, [])

      assert_sql(expected, q2)
    end

    test "array field compares equality when RHS is a list and operator defaults to ==" do
      expected =
        from(p in Post,
          where: p.tags == ^["elixir", "erlang"]
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: ["elixir", "erlang"]}, [])

      assert_sql(expected, q2)
    end

    test "array field supports negated equality when RHS is a list" do
      expected =
        from(p in Post,
          where: p.tags != ^["elixir"]
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{not: %{==: ["elixir"]}}}, [])

      assert_sql(expected, q2)
    end

    test "array field supports > comparison against scalar (any element matches)" do
      expected =
        from(p in Post,
          where: fragment("? < ANY(?)", ^"elixir", p.tags)
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{>: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array field supports >= comparison against scalar (any element matches)" do
      expected =
        from(p in Post,
          where: fragment("? <= ANY(?)", ^"elixir", p.tags)
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{>=: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports < comparison against scalar (any element matches)" do
      expected =
        from(p in Post,
          where: fragment("? > ANY(?)", ^"elixir", p.tags)
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{<: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports <= comparison against scalar (any element matches)" do
      expected =
        from(p in Post,
          where: fragment("? >= ANY(?)", ^"elixir", p.tags)
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{<=: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array field supports negated > comparison against scalar" do
      expected =
        from(p in Post,
          where: not fragment("? < ANY(?)", ^"elixir", p.tags)
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{not: %{>: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports keyword-list params" do
      expected = from p in Post, where: p.published == ^true
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, [published: true], [])

      assert_sql(expected, q2)
    end

    test "supports keyword-list params as a list" do
      expected = from p in Post, where: p.published == ^true
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, [published: true], [])

      assert_sql(expected, q2)
    end

    test "supports binding target params as a keyword list" do
      expected = from p in Post, where: p.published == ^true
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{at: %{1 => [published: true]}}, [])

      assert_sql(expected, q2)
    end

    test "treats non-keyword lists as values (defaults operator to ==)" do
      expected = from p in Post, where: p.published in ^[true, false]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published: [true, false]}, [])

      assert_sql(expected, q2)
    end

    test "supports multiple conditions under a single filter" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          where: p.published != ^false
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{published: [==: true, !=: false]}, [])

      assert_sql(expected, q2)
    end

    test "supports boolean :and operator for multiple comparisons on same field" do
      expected =
        from(p in Post,
          where: p.views > ^10 and p.views < ^20
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{and: [>: 10, <: 20]}}, [])

      assert_sql(expected, q2)
    end

    test "supports boolean :or operator for multiple comparisons on same field" do
      expected =
        from(p in Post,
          where: p.published == ^true or p.published == ^false
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{published: %{or: [==: true, ==: false]}},
          []
        )

      assert_sql(expected, q2)
    end

    test "boolean operator group is correctly grouped with other where conditions" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          where: p.views > ^10 or p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            where: [
              published: true,
              views: %{or: [>: 10, <: 5]}
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports boolean operator group under :or_where" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          or_where: p.views > ^10 or p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [
            or_where: %{views: %{or: [>: 10, <: 5]}},
            published: true
          ],
          []
        )

      assert_sql(expected, q2)
    end

    test "boolean operator with empty params is a no-op" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{and: []}}, [])
      assert q2 == q
    end

    test "supports composite :or operator with multiple field maps" do
      expected =
        from(p in Post,
          where:
            (p.published == ^true and p.views == ^20) or
              (p.published == ^false and p.views == ^10)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            or: [
              [published: true, views: 20],
              [published: false, views: 10]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports composite :or where inner maps also use field-level :or" do
      expected =
        from(p in Post,
          where:
            p.published == ^true or p.published == ^false or
              (p.published == ^true or p.published == ^false)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            or: [
              [published: [or: [==: true, ==: false]]],
              [published: [or: [==: true, ==: false]]]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports composite :and operator with multiple field maps" do
      expected =
        from(p in Post,
          where:
            p.published == ^true and p.views == ^20 and (p.title == ^"hello" and p.views == ^15)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            and: [
              [published: true, views: 20],
              [title: "hello", views: 15]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator with single map in list" do
      expected =
        from(p in Post,
          where: p.published == ^true and p.views == ^20
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{or: [[published: true, views: 20]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator is correctly grouped with other where conditions" do
      expected =
        from(p in Post,
          where: p.title == ^"test",
          where:
            (p.published == ^true and p.views == ^20) or
              (p.published == ^false and p.views == ^10)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [
            title: "test",
            or: [[published: true, views: 20], [published: false, views: 10]]
          ],
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator supports keyword list syntax" do
      expected =
        from(p in Post,
          where:
            (p.published == ^true and p.views == ^20) or
              (p.published == ^false and p.views == ^10)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            or: [
              [published: true, views: 20],
              [published: false, views: 10]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator with operators in field values" do
      expected =
        from(p in Post,
          where:
            (p.views > ^10 and p.published == ^true) or
              (p.views < ^5 and p.published == ^false)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            or: [
              [views: %{>: 10}, published: true],
              [views: %{<: 5}, published: false]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator under :or_where" do
      expected =
        from(p in Post,
          where: p.title == ^"test",
          or_where:
            (p.published == ^true and p.views == ^20) or
              (p.published == ^false and p.views == ^10)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            where: %{title: "test"},
            or_where: %{
              or: [
                [published: true, views: 20],
                [published: false, views: 10]
              ]
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator with empty list is a no-op" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{or: []}, [])
      assert q2 == q
    end

    test "composite operator with invalid field logs warning and skips field" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          _q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{
                id: %{
                  or: [
                    [published: true, does_not_exist: "value"]
                  ]
                }
              },
              []
            )

          send(self(), :done)
        end)

      assert log =~
               "Expected a query field for schema {\"posts\", EctoShorts.Schema.Post}, got: :does_not_exist"

      assert_received :done
    end

    test "invalid filter params container logs error and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{where: "bad"}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected params for where to be a map or keyword list, got: \"bad\""

      assert_received {:q2, q2}
      assert q2 == q
    end

    test "invalid filter format logs error and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{as: %{custom_alias: "bad"}},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~
               "Expected params to be a map or keyword list, got: \"bad\""

      assert_received {:q2, q2}
      assert q2 == q
    end

    test "invalid key logs warning and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{does_not_exist: true}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~
               "Expected a query field for schema {\"posts\", EctoShorts.Schema.Post}, got: :does_not_exist"

      assert_received {:q2, q2}
      assert q2 == q
    end
  end
end
