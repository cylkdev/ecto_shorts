defmodule EctoShorts.CommonFilters.QueryOperationTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias Ecto.Query
  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 select" do
    test "returns the full struct when select is true" do
      expected = from p in Post, select: p
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{select: true}, [])

      assert_sql(expected, q2)
    end

    test "returns only the selected field" do
      expected = from p in Post, select: p.id
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: :id}, [])

      assert_sql(expected, q2)
    end

    test "returns only the listed fields when select is a list" do
      expected = from p in Post, select: ^[:id, :title]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: [:id, :title]}, [])

      assert_sql(expected, q2)
    end

    test "returns a map with renamed fields when select uses a map alias" do
      expected = from p in Post, select: %{custom_id: p.id}
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: %{map: %{custom_id: :id}}}, [])

      assert_sql(expected, q2)
    end

    test "returns a map with the listed fields when select uses a field list" do
      expected = from p in Post, select: map(p, [:id, :title])
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: %{map: [:id, :title]}}, [])

      assert_sql(expected, q2)
    end

    test "returns a struct with only the listed fields when select uses struct" do
      expected = from p in Post, select: struct(p, [:id])
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select: %{struct: [:id]}}, [])

      assert_sql(expected, q2)
    end

    test "merges renamed fields into the selection" do
      expected = from p in Post, select_merge: %{custom_id: p.id}
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{select_merge: %{map: %{custom_id: :id}}}, [])

      assert_sql(expected, q2)
    end

    test "merges a field list into the selection" do
      expected = from p in Post, select_merge: map(p, [:id, :title])
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{select_merge: %{map: [:id, :title]}}, [])

      assert_sql(expected, q2)
    end

    test "combines select and select_merge in the same query" do
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
    test "adds distinct true to the query" do
      expected = from p in Post, distinct: true
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: true}, [])

      assert_sql(expected, q2)
    end

    test "adds distinct false to the query" do
      expected = from p in Post, distinct: false
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: false}, [])

      assert_sql(expected, q2)
    end

    test "adds distinct on a single field" do
      expected = from p in Post, distinct: :title
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: :title}, [])

      assert_sql(expected, q2)
    end

    test "adds distinct with direction-field tuples" do
      expected = from p in Post, distinct: [desc: :title]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: [desc: :title]}, [])

      assert_sql(expected, q2)
    end

    test "adds distinct from a map payload" do
      expected = from p in Post, distinct: [desc: :title]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{distinct: %{desc: :title}}, [])

      assert_sql(expected, q2)
    end

    test "combines distinct and order_by in the same query" do
      expected = from p in Post, distinct: :title, order_by: [desc: :id]

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [distinct: :title, order_by: :id],
          []
        )

      assert_sql(expected, q2)
    end

    test "logs a warning and returns the query unchanged for an invalid distinct value" do
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

    test "raises when distinct is applied more than once" do
      assert_raise Ecto.Query.CompileError, ~r/only one distinct expression is allowed in query/, fn ->
        CommonFilters.convert_params_to_filter(from(p in Post), [distinct: :title, distinct: :id], [])
      end
    end
  end

  describe "convert_params_to_filter/3 order_by" do
    test "sorts by a single field in descending order by default" do
      expected = from(p in Post, order_by: [desc: p.title])
      q2 = CommonFilters.convert_params_to_filter(Post, %{order_by: :title}, [])

      assert_sql(expected, q2)
    end

    test "prepends a sort field before existing order_by" do
      q = from(p in Post, order_by: [desc: :id])

      expected =
        from(p in Post,
          order_by: [desc: p.title, desc: p.id]
        )

      q2 = CommonFilters.convert_params_to_filter(q, %{prepend_order_by: :title}, [])

      assert_sql(expected, q2)
    end

    test "prepends multiple sort fields before existing order_by" do
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

    test "reverses the direction of existing order_by expressions" do
      q = from(p in Post, order_by: [asc: :id, desc: :title])

      expected =
        from(p in Post,
          order_by: [desc: p.id, asc: p.title]
        )

      q2 = CommonFilters.convert_params_to_filter(q, %{reverse_order: true}, [])

      assert_sql(expected, q2)
    end

    test "has no effect when there are no existing order_by expressions" do
      expected = reverse_order(Post)
      q2 = CommonFilters.convert_params_to_filter(Post, %{reverse_order: true}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 group_by and having" do
    test "groups results by a single field" do
      expected = from p in Post, group_by: p.author_id
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{group_by: :author_id}, [])

      assert_sql(expected, q2)
    end

    test "groups results by multiple fields" do
      expected = from p in Post, group_by: [p.author_id, p.published]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{group_by: [:author_id, :published]}, [])

      assert_sql(expected, q2)
    end

    test "filters grouped results with a having clause" do
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

    test "combines having conditions with and using a map" do
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

    test "combines having conditions with or using a keyword list" do
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

    test "applies comparison operators in a having clause" do
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

    test "applies aggregate functions in a having clause" do
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

    test "applies a dynamic expression in a having clause" do
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

    test "applies comparison operators in an or_having clause" do
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

    test "logs a warning and returns the query unchanged for invalid having params" do
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

    test "logs a warning and returns the query unchanged for invalid or_having params" do
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
    test "limits the number of returned records" do
      expected = from p in Post, limit: ^10
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{limit: 10}, [])

      assert_sql(expected, q2)
    end

    test "skips the first N records" do
      expected = from p in Post, offset: ^5
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{offset: 5}, [])

      assert_sql(expected, q2)
    end

    test "combines limit, offset, and where in the same query" do
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

    test "limits the number of records using the :first shortcut" do
      expected = from p in Post, limit: ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{first: 10}, [])

      assert_sql(expected, q2)
    end

    test "returns the last N records in ascending order" do
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

    test "processes :last after other filters" do
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

    test "returns the last N records sorted by the given key as a map" do
      expected =
        Post
        |> exclude(:order_by)
        |> from(order_by: [desc: :title], limit: ^2)
        |> subquery()
        |> order_by(:title)

      q2 = CommonFilters.convert_params_to_filter(Post, %{last: %{title: 2}}, [])

      assert_sql(expected, q2)
    end

    test "returns the last N records sorted by the given key as a keyword list" do
      expected =
        Post
        |> exclude(:order_by)
        |> from(order_by: [desc: :title], limit: ^2)
        |> subquery()
        |> order_by(:title)

      q2 = CommonFilters.convert_params_to_filter(Post, %{last: [title: 2]}, [])

      assert_sql(expected, q2)
    end

    test "returns the last N records sorted by the primary key when no key is given" do
      expected =
        Post
        |> exclude(:order_by)
        |> from(order_by: [desc: :id], limit: ^2)
        |> subquery()
        |> order_by(:id)

      q2 = CommonFilters.convert_params_to_filter(Post, %{last: {nil, 2}}, [])

      assert_sql(expected, q2)
    end

    test "returns the last N records sorted by id when given as a map" do
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
    test "removes a single query expression using :exclude" do
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

    test "removes multiple query expressions using :exclude" do
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
    test "applies a lock using a query-builder function" do
      expected = from(p in Post, lock: "FOR UPDATE")

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{lock: fn query -> from(p in query, lock: "FOR UPDATE") end},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies a lock using a resolver payload map" do
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

    test "applies a lock using a resolver payload keyword list" do
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
    test "sets the query prefix on a schema source" do
      expected = Query.put_query_prefix(Post, "tenant_a")
      q2 = CommonFilters.convert_params_to_filter(Post, %{put_query_prefix: "tenant_a"}, [])

      assert_sql(expected, q2)
    end

    test "sets the query prefix on a table source" do
      expected =
        "posts"
        |> Query.put_query_prefix("tenant_a")
        |> select([p], p)

      q2 = CommonFilters.convert_params_to_filter("posts", %{put_query_prefix: "tenant_a"}, [])

      assert_query(expected, q2)
    end

    test "applies the last prefix when multiple are given" do
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

    test "logs a warning and returns the query unchanged when the prefix is nil" do
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

    test "logs a warning and returns the query unchanged when the prefix is not a string" do
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
    test "enables with_ties when set to true" do
      q =
        from(p in Post,
          order_by: [desc: :views],
          limit: ^10
        )

      expected = Query.with_ties(q, true)

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: true}, [])

      assert_sql(expected, q2)
    end

    test "disables with_ties when set to false" do
      q =
        from(p in Post,
          order_by: [desc: :views],
          limit: ^10
        )

      expected = Query.with_ties(q, false)

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: false}, [])

      assert_sql(expected, q2)
    end

    test "logs a warning and returns the query unchanged for an invalid with_ties value" do
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

    test "defaults to a limit of 1000 when with_ties is given without a limit" do
      q = from(p in Post)

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: true, order_by: :title}, [])

      expected = q |> Query.limit(^1000) |> Query.with_ties(^true) |> Query.order_by(desc: :title)
      assert_sql(expected, q2)
    end

    test "uses the explicit limit from a with_ties map" do
      q = from(p in Post, order_by: [desc: :views])

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: %{limit: 500}}, [])

      expected = q |> Query.limit(^500) |> Query.with_ties(^true)
      assert_sql(expected, q2)
    end

    test "uses the explicit limit from a with_ties keyword list" do
      q = from(p in Post, order_by: [desc: :views])

      q2 = CommonFilters.convert_params_to_filter(q, %{with_ties: [limit: 500]}, [])

      expected = q |> Query.limit(^500) |> Query.with_ties(^true)
      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 CTE and recursive" do
    test "adds a CTE from a keyword list payload" do
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

    test "adds a CTE from a map payload" do
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

    test "adds a materialized CTE with a custom operation" do
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

    test "adds a CTE with a nested :as query" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = Query.with_cte(Post, "published_posts", as: ^cte_query)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [published_posts: [as: %{from: %{query: Post, published: true}}]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "adds a CTE with a nested :as query built from :from params" do
      cte_query = from(p in Post, where: p.id == ^1)
      expected = Query.with_cte(Post, "published_posts", as: ^cte_query)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [published_posts: [as: [from: [query: Post, id: 1]]]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "logs a warning and returns the query unchanged for invalid CTE entries" do
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

    test "enables recursive CTEs when set to true" do
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

    test "disables recursive CTEs when set to false" do
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

    test "logs a warning and returns the query unchanged for an invalid recursive_ctes value" do
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
    test "adds a named binding when it does not exist on the query" do
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

    test "does nothing when the named binding already exists" do
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

    test "adds multiple named bindings in one call" do
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

    test "adds named bindings from a map payload" do
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

    test "logs a warning and returns the query unchanged for an invalid named binding key" do
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

    test "logs a warning and returns the query unchanged for an invalid named binding params type" do
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

    test "logs a warning and returns the query unchanged when the binding callback returns nil" do
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
    test "adds an update expression with set and inc operators" do
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

    test "adds an update expression from a map payload" do
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
    test "adds a window definition from a keyword list" do
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

    test "adds a window definition from a map" do
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

    test "adds a window definition with a frame clause" do
      frame = dynamic(fragment("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            windows: [
              post_window: [
                partition_by: :author_id,
                order_by: :inserted_at,
                frame: frame
              ]
            ]
          },
          []
        )

      assert q2.windows !== []
    end

    test "adds a window with partition_by as a list of fields" do
      expected =
        from(p in Post,
          windows: [post_window: [partition_by: [p.author_id, p.published], order_by: []]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [partition_by: [:author_id, :published], order_by: []]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "adds a window with order_by as a single field" do
      expected =
        from(p in Post,
          windows: [post_window: [partition_by: [], order_by: p.inserted_at]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [order_by: :inserted_at]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "adds a window with order_by as a direction-field tuple" do
      expected =
        from(p in Post,
          windows: [post_window: [partition_by: [], order_by: [desc: p.inserted_at]]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [order_by: %{desc: :inserted_at}]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "logs a warning and returns the query unchanged for non-keyword list window params" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{windows: [1, 2, 3]}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :windows params to be a keyword list"
      assert_received {:q2, q2}
      assert_sql(q, q2)
    end

    test "logs a warning and returns the query unchanged for an invalid window value" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{windows: "invalid"}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :windows params to be a keyword list/map"
      assert_received {:q2, q2}
      assert_sql(q, q2)
    end

    test "adds a window with partition_by as a single field" do
      expected =
        from(p in Post,
          windows: [post_window: [partition_by: p.author_id, order_by: []]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [partition_by: :author_id]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "logs a warning and returns the query unchanged for an invalid window definition" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{windows: [my_window: "not_valid"]}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected window definition for :my_window to be a map or keyword list"
      assert_received {:q2, q2}
      assert_sql(q, q2)
    end

    test "logs a warning and ignores unsupported keys in the window definition" do
      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              Post,
              %{windows: [my_window: [partition_by: :id, bogus: true]]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Ignoring unsupported window keys"
      assert_received {:q2, q2}
      assert q2.windows !== []
    end
  end

  describe "convert_params_to_filter/3 subquery" do
    test "wraps the filtered query in a subquery" do
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

    test "wraps the query in a subquery using keyword list filters" do
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

    test "applies filters before wrapping in a subquery" do
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
    test "removes rows from another query using except" do
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

    test "removes rows from a pre-built query using except" do
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

    test "removes rows using except_all" do
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

    test "keeps only common rows using intersect" do
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

    test "keeps only common rows using intersect_all" do
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

    test "combines rows from another query using union" do
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

    test "combines rows using union_all" do
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
    test "preloads a list of associations" do
      expected = from p in Post, preload: [:author]
      q2 = CommonFilters.convert_params_to_filter(Post, %{preload: [:author]}, [])

      assert_query(expected, q2)
    end

    test "preloads a single association by name" do
      expected = from p in Post, preload: [:author]
      q2 = CommonFilters.convert_params_to_filter(Post, %{preload: :author}, [])

      assert_query(expected, q2)
    end

    test "preloads nested associations from a keyword list" do
      expected = from p in Post, preload: [author: [:posts]]
      q2 = CommonFilters.convert_params_to_filter(Post, %{preload: [author: [:posts]]}, [])

      assert_query(expected, q2)
    end

    test "preloads an association on a named binding" do
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
          %{preload: [bind: %{as: :example, value: :author}]},
          []
        )

      assert_query(expected, q2)
    end

    test "preloads an association on a positional binding" do
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
          %{preload: [bind: %{at: 2, value: :author}]},
          []
        )

      assert_query(expected, q2)
    end

    test "preloads an association on a positional binding with a shared nested payload" do
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
          %{preload: [bind: %{at: 2, value: :author}, posts: [:comments]]},
          []
        )

      assert_query(expected, q2)
    end

    test "preloads associations on mixed named and positional bindings" do
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
          %{preload: [bind: [%{as: :author, value: :author}, %{at: 2, value: :author}], posts: [:comments]]},
          []
        )

      assert_query(expected, q2)
    end

    test "preloads nested associations alongside a binding selector" do
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
          %{preload: [bind: %{as: :author, value: :author}, posts: [:comments]]},
          []
        )

      assert_query(expected, q2)
    end

    test "logs a warning and skips the preload when the positional binding is invalid" do
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
              %{preload: [bind: %{at: "2", value: :author}]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :at value to be an integer, :first, or :last"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "logs a warning and skips the preload when the named binding does not exist" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{preload: [bind: %{as: :missing, value: :author}]},
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
    test "sorts by a field in descending order" do
      expected = from(p in Post, order_by: [desc: p.title])
      q2 = CommonFilters.convert_params_to_filter(Post, %{order_by: [desc: :title]}, [])

      assert_sql(expected, q2)
    end

    test "sorts by multiple fields with mixed directions" do
      expected = from(p in Post, order_by: [asc: p.title, desc: p.id])
      q2 = CommonFilters.convert_params_to_filter(Post, %{order_by: [asc: :title, desc: :id]}, [])

      assert_sql(expected, q2)
    end

    test "sorts by a field using a map payload" do
      expected = from(p in Post, order_by: [desc: p.title])
      q2 = CommonFilters.convert_params_to_filter(Post, %{order_by: %{desc: :title}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 combined examples" do
    test "combines a field filter with limit and offset" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          limit: ^10,
          offset: ^5
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{published: true, limit: 10, offset: 5}, [])

      assert_sql(expected, q2)
    end

    test "combines group_by with a having clause" do
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

    test "combines group_by with having and or_having" do
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

    test "combines group_by with a having clause using a datetime ago expression" do
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

    test "combines a where filter with an or_where boolean group" do
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

    test "combines a field filter with an or operator on another field" do
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
