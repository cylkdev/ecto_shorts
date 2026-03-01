defmodule EctoShorts.CommonFilters.QueryOperationTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias Ecto.Query
  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 select" do
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
  end

  describe "convert_params_to_filter/3 distinct" do
    test "supports :distinct true" do
      expected = from p in Post, distinct: true
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: true}, [])

      assert_sql(expected, q2)
    end

    test "supports :distinct false" do
      expected = from p in Post, distinct: false
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: false}, [])

      assert_sql(expected, q2)
    end

    test "supports :distinct for a single field" do
      expected = from p in Post, distinct: :title
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: :title}, [])

      assert_sql(expected, q2)
    end

    test "supports :distinct with ordered fields" do
      expected = from p in Post, distinct: [desc: :title]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: [desc: :title]}, [])

      assert_sql(expected, q2)
    end

    test "supports :distinct with map payload" do
      expected = from p in Post, distinct: [desc: :title]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: %{desc: :title}}, [])

      assert_sql(expected, q2)
    end

    test "supports :distinct with :order_by in the same query" do
      expected = from p in Post, distinct: :title, order_by: [desc: :id]

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [distinct: :title, order_by: :id],
          []
        )

      assert_sql(expected, q2)
    end

    test "invalid :distinct payload logs warning and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{distinct: 123}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "distinct"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "multiple :distinct entries raises" do
      assert_raise Ecto.Query.CompileError, ~r/only one distinct expression is allowed in query/, fn ->
        CommonFilters.convert_params_to_filter(from(p in Post), [distinct: :title, distinct: :id], [])
      end
    end
  end

  describe "convert_params_to_filter/3 order_by" do
    test "supports :order_by for a single field" do
      expected = from(p in Post, order_by: [desc: p.title])
      q2 = CommonFilters.convert_params_to_filter(Post, %{order_by: :title}, [])

      assert_sql(expected, q2)
    end

    test "supports :prepend_order_by for a single field" do
      q = from(p in Post, order_by: [desc: :id])

      expected =
        from(p in Post,
          order_by: [desc: p.title, desc: p.id]
        )

      q2 = CommonFilters.convert_params_to_filter(q, %{prepend_order_by: :title}, [])

      assert_sql(expected, q2)
    end

    test "supports :prepend_order_by with ordered fields" do
      q = from(p in Post, order_by: [desc: :id])

      expected =
        from(p in Post,
          order_by: [asc: p.published_at, desc: p.title, desc: p.id]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{prepend_order_by: [asc: :published_at, desc: :title]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :reverse_order with existing order_by expressions" do
      q = from(p in Post, order_by: [asc: :id, desc: :title])

      expected =
        from(p in Post,
          order_by: [desc: p.id, asc: p.title]
        )

      q2 = CommonFilters.convert_params_to_filter(q, %{reverse_order: true}, [])

      assert_sql(expected, q2)
    end

    test "supports :reverse_order with no existing order_by expressions" do
      expected = reverse_order(Post)
      q2 = CommonFilters.convert_params_to_filter(Post, %{reverse_order: true}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 group_by and having" do
    test "supports :group_by for a single field" do
      expected = from p in Post, group_by: p.author_id
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{group_by: :author_id}, [])

      assert_sql(expected, q2)
    end

    test "supports :group_by with a list of fields" do
      expected = from p in Post, group_by: [p.author_id, p.published]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{group_by: [:author_id, :published]}, [])

      assert_sql(expected, q2)
    end

    test "supports :having with grouped query field" do
      expected =
        from(p in Post,
          group_by: p.published,
          having: p.published == ^true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :published, having: %{published: true}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :having boolean :and payload as a map" do
      expected =
        from(p in Post,
          group_by: [p.published, p.views],
          having: p.published == ^true and p.views > ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            group_by: [:published, :views],
            having: [and: [published: true, views: %{>: 10}]]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :having boolean :or payload as a keyword list" do
      expected =
        from(p in Post,
          group_by: p.views,
          having: p.views > ^10 or p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            group_by: :views,
            having: [or: [views: %{>: 10}, views: %{<: 5}]]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :having with operator payloads" do
      expected =
        from(p in Post,
          group_by: p.views,
          having: p.views > ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :views, having: %{views: %{>: 10}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :having with aggregate helper expressions" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: avg(p.views) > ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :having with Ecto.Query.dynamic/2 payload" do
      dyn = dynamic([p], p.views > ^10)

      expected =
        from(p in Post,
          group_by: p.views,
          having: p.views > ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :views, having: dyn},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :or_having with operator payloads" do
      expected =
        from(p in Post,
          group_by: p.views,
          having: p.views > ^10,
          or_having: p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :views, having: %{views: %{>: 10}}, or_having: %{views: %{<: 5}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "invalid :having params container logs warning and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{having: "bad"}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected params for having to be a map or keyword list, got: \"bad\""

      assert_received {:q2, q2}
      assert q2 === q
    end

    test "invalid :or_having params container logs warning and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{or_having: "bad"}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected params for or_having to be a map or keyword list, got: \"bad\""

      assert_received {:q2, q2}
      assert q2 === q
    end
  end

  describe "convert_params_to_filter/3 limit, offset, first, last" do
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

    test "supports :first filter (delegates to :limit)" do
      expected = from p in Post, limit: ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{first: 10}, [])

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

    test "supports :last with other filters (Rule 19: terminal filter processed last)" do
      expected =
        Post
        |> from(where: [published: ^true], limit: ^10)
        |> exclude(:order_by)
        |> from(order_by: [desc: :id], limit: ^2)
        |> subquery()
        |> order_by(:id)

      q2 = CommonFilters.convert_params_to_filter(Post, %{published: true, limit: 10, last: 2}, [])

      assert_sql(expected, q2)
    end

    test "supports :last with explicit sort key as a map" do
      expected =
        Post
        |> exclude(:order_by)
        |> from(order_by: [desc: :title], limit: ^2)
        |> subquery()
        |> order_by(:title)

      q2 = CommonFilters.convert_params_to_filter(Post, %{last: %{title: 2}}, [])

      assert_sql(expected, q2)
    end

    test "supports :last with explicit sort key as a keyword list" do
      expected =
        Post
        |> exclude(:order_by)
        |> from(order_by: [desc: :title], limit: ^2)
        |> subquery()
        |> order_by(:title)

      q2 = CommonFilters.convert_params_to_filter(Post, %{last: [title: 2]}, [])

      assert_sql(expected, q2)
    end

    test "supports :last with nil sort key (uses primary key)" do
      expected =
        Post
        |> exclude(:order_by)
        |> from(order_by: [desc: :id], limit: ^2)
        |> subquery()
        |> order_by(:id)

      q2 = CommonFilters.convert_params_to_filter(Post, %{last: {nil, 2}}, [])

      assert_sql(expected, q2)
    end

    test "supports :last with explicit id sort key as a map" do
      expected =
        Post
        |> exclude(:order_by)
        |> from(order_by: [desc: :id], limit: ^2)
        |> subquery()
        |> order_by(:id)

      q2 = CommonFilters.convert_params_to_filter(Post, %{last: %{id: 2}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 exclude" do
    test "supports :exclude for a single expression" do
      expected =
        from(p in Post,
          limit: ^10
        )

      q =
        from(p in Post,
          order_by: [desc: :id],
          limit: ^10
        )

      q2 = CommonFilters.convert_params_to_filter(q, %{exclude: :order_by}, [])

      assert_sql(expected, q2)
    end

    test "supports :exclude with a list of expressions" do
      expected =
        from(p in Post,
          where: p.published == ^true
        )

      q =
        from(p in Post,
          where: p.published == ^true,
          order_by: [desc: :id],
          limit: ^10
        )

      q2 = CommonFilters.convert_params_to_filter(q, %{exclude: [:order_by, :limit]}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 lock" do
    test "supports :lock with direct query-builder function" do
      expected = from(p in Post, lock: "FOR UPDATE")

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{lock: fn query -> from(p in query, lock: "FOR UPDATE") end},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :lock with expression resolver payload" do
      q = from(p in Post, where: p.published == ^true)

      expected =
        from(p in Post,
          where: p.published == ^true,
          lock: "FOR SHARE"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{lock: %{name: :for_share, values: []}},
          fragment_provider: EctoShorts.TestFragmentProvider
        )

      assert_sql(expected, q2)
    end

    test "supports :lock with keyword list resolver payload" do
      q = from(p in Post, where: p.published == ^true)

      expected =
        from(p in Post,
          where: p.published == ^true,
          lock: "FOR SHARE"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{lock: [name: :for_share, values: []]},
          fragment_provider: EctoShorts.TestFragmentProvider
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 put_query_prefix" do
    test "supports :put_query_prefix with schema source" do
      expected = Query.put_query_prefix(Post, "tenant_a")
      q2 = CommonFilters.convert_params_to_filter(Post, %{put_query_prefix: "tenant_a"}, [])

      assert_sql(expected, q2)
    end

    test "supports :put_query_prefix with table source" do
      expected = Query.put_query_prefix("posts", "tenant_a")
      q2 = CommonFilters.convert_params_to_filter("posts", %{put_query_prefix: "tenant_a"}, [])

      assert_query(expected, q2)
    end

    test "supports :put_query_prefix override order (last wins)" do
      expected =
        Post
        |> Query.put_query_prefix("tenant_a")
        |> Query.put_query_prefix("tenant_b")

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [put_query_prefix: "tenant_a", put_query_prefix: "tenant_b"],
          []
        )

      assert_sql(expected, q2)
    end

    test "invalid :put_query_prefix nil logs warning and leaves query unchanged" do
      q = Query.put_query_prefix(Post, "tenant_a")

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{put_query_prefix: nil}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :put_query_prefix value to be a string, got: nil"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "invalid :put_query_prefix value logs warning and leaves query unchanged" do
      q = from(p in Post, where: p.published == ^true)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{put_query_prefix: %{bad: "value"}}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :put_query_prefix value to be a string, got: [bad: \"value\"]"
      assert_received {:q2, q2}
      assert q2 === q
    end
  end

  describe "convert_params_to_filter/3 with_ties" do
    test "supports :with_ties true with :limit and :order_by" do
      q =
        from(p in Post,
          order_by: [desc: :views],
          limit: ^10
        )

      expected = Query.with_ties(q, true)

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: true}, [])

      assert_sql(expected, q2)
    end

    test "supports :with_ties false with :limit and :order_by" do
      q =
        from(p in Post,
          order_by: [desc: :views],
          limit: ^10
        )

      expected = Query.with_ties(q, false)

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: false}, [])

      assert_sql(expected, q2)
    end

    test "invalid :with_ties value logs warning and leaves query unchanged" do
      q =
        from(p in Post,
          order_by: [desc: :views],
          limit: ^10
        )

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: "yes"}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :with_ties value to be a boolean, got: \"yes\""
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "with_ties without limit defaults limit to 1000" do
      q = from(p in Post)

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: true, order_by: :title}, [])

      expected = q |> Query.limit(^1000) |> Query.with_ties(^true) |> Query.order_by(desc: :title)
      assert_sql(expected, q2)
    end

    test "with_ties map params with explicit limit" do
      q = from(p in Post, order_by: [desc: :views])

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: %{limit: 500}}, [])

      expected = q |> Query.limit(^500) |> Query.with_ties(^true)
      assert_sql(expected, q2)
    end

    test "with_ties keyword params with explicit limit" do
      q = from(p in Post, order_by: [desc: :views])

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: [limit: 500]}, [])

      expected = q |> Query.limit(^500) |> Query.with_ties(^true)
      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 CTE and recursive" do
    test "supports :with_cte with keyword payload" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = Query.with_cte(Post, "published_posts", as: ^cte_query)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [published_posts: [as: cte_query]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :with_cte with map payload" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = Query.with_cte(Post, "published_posts", as: ^cte_query)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: %{published_posts: %{as: cte_query}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :with_cte with :materialized and :operation options" do
      cte_query = from(p in Post, select: p)

      expected =
        Query.with_cte(
          Post,
          "published_posts",
          as: ^cte_query,
          materialized: false,
          operation: :all
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            with_cte: [
              published_posts: [as: cte_query, materialized: false, operation: :all]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :with_cte with nested :as payload" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = Query.with_cte(Post, "published_posts", as: ^cte_query)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [published_posts: [as: %{source: Post, query: %{published: true}}]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :with_cte with nested :as payload using :source" do
      cte_query = from(p in Post, where: p.id == ^1)
      expected = Query.with_cte(Post, "published_posts", as: ^cte_query)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [published_posts: [as: [source: Post, query: [id: 1]]]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "invalid :with_cte entries warn and leave query unchanged" do
      q = from(p in Post, where: p.published == ^true)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{with_cte: [published_posts: [operation: :all]]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected CTE :as query params"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "supports :recursive_ctes true with :with_cte" do
      cte_query = from(p in Post, where: p.published == ^true)

      expected =
        Post
        |> Query.recursive_ctes(true)
        |> Query.with_cte("published_posts", as: ^cte_query)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]],
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :recursive_ctes false with :with_cte" do
      cte_query = from(p in Post, where: p.published == ^true)

      q =
        Post
        |> Query.recursive_ctes(true)
        |> Query.with_cte("published_posts", as: ^cte_query)

      expected =
        Post
        |> Query.with_cte("published_posts", as: ^cte_query)
        |> Query.recursive_ctes(false)

      q2 = CommonFilters.convert_params_to_filter(q, %{recursive_ctes: false}, [])

      assert_sql(expected, q2)
    end

    test "invalid :recursive_ctes value logs warning and leaves query unchanged" do
      q = from(p in Post, where: p.published == ^true)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{recursive_ctes: "yes"}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :recursive_ctes value to be a boolean, got: \"yes\""
      assert_received {:q2, q2}
      assert q2 === q
    end
  end

  describe "convert_params_to_filter/3 with_named_binding" do
    test "supports :with_named_binding when binding is missing" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :with_named_binding as no-op when binding already exists" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}]},
          []
        )

      assert_sql(q, q2)
    end

    test "supports :with_named_binding with multiple entries" do
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
            with_named_binding: [
              author: %{join: [association: [source: :author, as: :author]]},
              users_table: %{join: [table: [source: "users", as: :users_table, on: true]]}
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :with_named_binding with map payload" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            with_named_binding: %{
              author: %{join: [association: [source: :author, as: :author]]}
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "invalid :with_named_binding key type warns and leaves query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{
                with_named_binding: [
                  {"author", %{join: [association: [source: :author, as: :author]]}}
                ]
              },
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :with_named_binding key to be an atom, got: \"author\""
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "invalid :with_named_binding params type warns and leaves query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{with_named_binding: [author: 123]}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~
               "Expected :with_named_binding params for :author to be a map or keyword list, got: 123"

      assert_received {:q2, q2}
      assert q2 === q
    end

    test "missing named binding callback result logs warning and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{with_named_binding: [author: %{where: %{published: true}}]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "should create a named binding for key :author"
      assert_received {:q2, q2}
      assert q2 === q
    end
  end

  describe "convert_params_to_filter/3 update" do
    test "supports :update with update operators" do
      updates = [set: [title: "After"], inc: [views: 1]]
      expected = Query.update(Post, ^updates)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{update: [set: [title: "After"], inc: [views: 1]]},
          []
        )

      assert_sql(expected, q2, :update_all)
    end

    test "supports :update with operation payload maps" do
      updates = [set: [title: "After"]]
      expected = Query.update(Post, ^updates)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{update: %{set: %{title: "After"}}},
          []
        )

      assert_sql(expected, q2, :update_all)
    end
  end

  describe "convert_params_to_filter/3 windows" do
    test "supports :windows with keyword payload" do
      expected =
        from(p in Post,
          windows: [post_window: [partition_by: p.author_id, order_by: [desc: p.inserted_at]]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :windows with map payload" do
      expected =
        from(p in Post,
          windows: [post_window: [partition_by: p.author_id, order_by: [desc: p.inserted_at]]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: %{post_window: %{partition_by: :author_id, order_by: [desc: :inserted_at]}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 subquery" do
    test "supports :subquery with nested filters" do
      expected_inner = from(p in Post, where: p.id == ^2)
      expected = subquery(expected_inner)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{subquery: %{id: 2}},
          []
        )

      assert_query(expected, q2)
    end

    test "supports :subquery with keyword list filters" do
      expected_inner = from(p in Post, where: p.id == ^2)
      expected = subquery(expected_inner)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{subquery: [id: 2]},
          []
        )

      assert_query(expected, q2)
    end

    test "applies :subquery after top-level filters" do
      expected_inner =
        from(p in Post,
          where: p.published == ^true,
          where: p.id == ^2
        )

      expected = subquery(expected_inner)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [published: true, subquery: %{id: 2}],
          []
        )

      assert_query(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 set operations" do
    test "supports :except with nested filter params" do
      q = from(p in Post, where: p.published == ^true)
      other_query = from(p in Post, where: p.published == ^false)

      expected =
        from(p in Post,
          where: p.published == ^true,
          except: ^other_query
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{except: %{published: false}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :except with pre-built query" do
      q = from(p in Post, where: p.published == ^true)
      other_query = from(p in Post, where: p.published == ^false)

      expected =
        from(p in Post,
          where: p.published == ^true,
          except: ^other_query
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{except: other_query},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :except_all with nested filter params" do
      q = from(p in Post, where: p.published == ^true)
      other_query = from(p in Post, where: p.published == ^false)

      expected =
        from(p in Post,
          where: p.published == ^true,
          except_all: ^other_query
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{except_all: %{published: false}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :intersect with nested filter params" do
      q = from(p in Post, where: p.published == ^true)
      other_query = from(p in Post, where: p.published == ^false)

      expected =
        from(p in Post,
          where: p.published == ^true,
          intersect: ^other_query
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{intersect: %{published: false}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :intersect_all with nested filter params" do
      q = from(p in Post, where: p.published == ^true)
      other_query = from(p in Post, where: p.published == ^false)

      expected =
        from(p in Post,
          where: p.published == ^true,
          intersect_all: ^other_query
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{intersect_all: %{published: false}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :union with nested filter params" do
      q = from(p in Post, where: p.published == ^true)
      other_query = from(p in Post, where: p.published == ^false)

      expected =
        from(p in Post,
          where: p.published == ^true,
          union: ^other_query
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{union: %{published: false}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :union_all with nested filter params" do
      q = from(p in Post, where: p.published == ^true)
      other_query = from(p in Post, where: p.published == ^false)

      expected =
        from(p in Post,
          where: p.published == ^true,
          union_all: ^other_query
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{union_all: %{published: false}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 preload" do
    test "supports plain :preload list" do
      expected = from p in Post, preload: [:author]
      q2 = CommonFilters.convert_params_to_filter(Post, %{preload: [:author]}, [])

      assert_query(expected, q2)
    end

    test "supports plain :preload atom" do
      expected = from p in Post, preload: [:author]
      q2 = CommonFilters.convert_params_to_filter(Post, %{preload: :author}, [])

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
          as: :example
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :example,
          preload: [author: a]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{preload: [bind: [as: [example: :author]]]},
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
          %{preload: [bind: [at: %{2 => :author}]]},
          []
        )

      assert_query(expected, q2)
    end

    test "supports binding-aware :preload with direct positional selector and shared nested payload" do
      q =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          preload: [author: {a, [posts: [:comments]]}]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{preload: [bind: [at: %{2 => :author}], posts: [:comments]]},
          []
        )

      assert_query(expected, q2)
    end

    test "supports mixed direct named and positional selectors in one assoc payload" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          preload: [author: {a, [posts: [:comments]]}],
          preload: [author: {a, [posts: [:comments]]}]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{preload: [bind: [as: [author: :author], at: %{2 => :author}], posts: [:comments]]},
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
          %{preload: [bind: [as: [author: :author]], posts: [:comments]]},
          []
        )

      assert_query(expected, q2)
    end

    test "invalid positional preload binding operator target logs warning and skips entry" do
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
              %{preload: [bind: [at: [2]]]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :bind -> :at entries to be {target, params} tuples, got: 2"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "missing preload binding alias logs warning and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{preload: [bind: [as: [missing: :author]]]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "unknown bind name `:missing`"
      assert_received {:q2, q2}
      assert q2 === q
    end
  end

  describe "convert_params_to_filter/3 order_by additional examples" do
    test "order_by — %{order_by: [desc: :title]}" do
      expected = from(p in Post, order_by: [desc: p.title])
      q2 = CommonFilters.convert_params_to_filter(Post, %{order_by: [desc: :title]}, [])

      assert_sql(expected, q2)
    end

    test "order_by — %{order_by: [asc: :title, desc: :id]}" do
      expected = from(p in Post, order_by: [asc: p.title, desc: p.id])
      q2 = CommonFilters.convert_params_to_filter(Post, %{order_by: [asc: :title, desc: :id]}, [])

      assert_sql(expected, q2)
    end

    test "order_by — %{order_by: %{desc: :title}} (map payload)" do
      expected = from(p in Post, order_by: [desc: p.title])
      q2 = CommonFilters.convert_params_to_filter(Post, %{order_by: %{desc: :title}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 combined examples" do
    test "combined — %{published: true, limit: 10, offset: 5}" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          limit: ^10,
          offset: ^5
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{published: true, limit: 10, offset: 5}, [])

      assert_sql(expected, q2)
    end

    test "combined — %{group_by: :published, having: %{published: true}}" do
      expected =
        from(p in Post,
          group_by: p.published,
          having: p.published == ^true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :published, having: %{published: true}},
          []
        )

      assert_sql(expected, q2)
    end

    test "combined — %{group_by: :views, having: %{views: %{>: 10}}, or_having: %{views: %{<: 5}}}" do
      expected =
        from(p in Post,
          group_by: p.views,
          having: p.views > ^10,
          or_having: p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :views, having: %{views: %{>: 10}}, or_having: %{views: %{<: 5}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "combined — %{group_by: :id, having: %{inserted_at: %{>: %{datetime: %{ago: ...}}}}}" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :id, having: %{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: "day"}}}}}},
          []
        )

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, EctoShorts.Repo, q2)
      # credo:disable-for-previous-line Credo.Check.Design.AliasUsage

      assert sql =~ "GROUP BY p0.\"id\""
      assert sql =~ "HAVING (p0.\"inserted_at\" > $1::timestamp + ($2::numeric * interval '1 day'))"
      assert match?([%DateTime{}, %Decimal{}], params)
      assert Enum.at(params, 1) === Decimal.new("-1")
    end

    test "combined — %{where: %{title: \"test\"}, or_where: %{or: [[published: true, views: 20], [published: false, views: 10]]}}" do
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
            or_where: %{or: [[published: true, views: 20], [published: false, views: 10]]}
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "combined — %{where: [published: true, views: %{or: [>: 10, <: 5]}]}" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          where: p.views > ^10 or p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{where: [published: true, views: %{or: [>: 10, <: 5]}]},
          []
        )

      assert_sql(expected, q2)
    end
  end
end
