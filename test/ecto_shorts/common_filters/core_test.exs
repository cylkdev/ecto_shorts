defmodule EctoShorts.CommonFilters.CoreTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 core" do
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

    test "supports top-level :dynamic payload (defaults to :where)" do
      dyn = dynamic([p], p.views > ^10)
      expected = from(p in Post, where: p.views > ^10)

      q2 = CommonFilters.convert_params_to_filter(Post, %{dynamic: dyn}, [])

      assert_sql(expected, q2)
    end

    test "supports :where with :dynamic payload" do
      dyn = dynamic([p], p.published == ^true)
      expected = from(p in Post, where: p.published == ^true)

      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{dynamic: dyn}}, [])

      assert_sql(expected, q2)
    end

    test "supports :or_where with :dynamic payload" do
      dyn = dynamic([p], p.views > ^100)
      expected = from(p in Post, or_where: p.views > ^100)

      q2 = CommonFilters.convert_params_to_filter(Post, %{or_where: %{dynamic: dyn}}, [])

      assert_sql(expected, q2)
    end

    test "invalid :dynamic payload logs warning and leaves query unchanged" do
      q = from(p in Post, where: p.published == ^true)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{dynamic: "bad"}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :dynamic payload to be an Ecto.Query.DynamicExpr, got: \"bad\""
      assert_received {:q2, q2}
      assert q2 === q
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

    test "treats non-keyword lists as values (defaults operator to ==)" do
      expected = from p in Post, where: p.published in ^[true, false]
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{published: [true, false]}, [])

      assert_sql(expected, q2)
    end

    test "invalid top-level params logs warning and leaves query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{123 => "oops"}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected params to be a map or list, got: {123, \"oops\"}"
      assert_received {:q2, q2}
      assert q2 === q
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
      assert q2 === q
    end

    test "invalid filter format logs error and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{bind: %{as: %{custom_alias: "bad"}}},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~
               "Expected params to be a map or keyword list, got: \"bad\""

      assert_received {:q2, q2}
      assert q2 === q
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
      assert q2 === q
    end

    test "supports :ids custom filter" do
      expected = from p in Post, where: p.id in ^[1, 2, 3]
      q2 = CommonFilters.convert_params_to_filter(Post, %{ids: [1, 2, 3]}, [])

      assert_sql(expected, q2)
    end

    test "supports :after custom filter" do
      expected = from p in Post, where: p.id > ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{after: 10}, [])

      assert_sql(expected, q2)
    end

    test "supports :before custom filter" do
      expected = from p in Post, where: p.id < ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{before: 10}, [])

      assert_sql(expected, q2)
    end

    test "supports :start_date custom filter" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.inserted_at >= ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{start_date: dt}, [])

      assert_sql(expected, q2)
    end

    test "supports :end_date custom filter" do
      dt = ~U[2026-12-31 23:59:59Z]
      expected = from p in Post, where: p.inserted_at <= ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{end_date: dt}, [])

      assert_sql(expected, q2)
    end

    test "supports :source meta-key to override source module" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [source: Post, query: %{id: 1}], [])

      assert_sql(expected, q2)
    end

    test "supports :source meta-key with direct field params" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [source: Post, id: 1], [])

      assert_sql(expected, q2)
    end

    test "supports :query meta-key merged with field params" do
      expected =
        from(p in Post,
          where: p.id == ^1,
          where: p.published == ^true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [query: %{id: 1}, published: true],
          []
        )

      assert_sql(expected, q2)
    end
  end
end
