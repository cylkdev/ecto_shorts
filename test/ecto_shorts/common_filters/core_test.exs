defmodule EctoShorts.CommonFilters.CoreTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 field equality" do
    test "field equality - %{id: 1}" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: 1}, [])

      assert_sql(expected, q2)
    end

    test "field equality - %{published: true}" do
      expected = from p in Post, where: p.published == ^true
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: true}, [])

      assert_sql(expected, q2)
    end

    test "field equality - %{title: \"hello\"}" do
      expected = from p in Post, where: p.title == ^"hello"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: "hello"}, [])

      assert_sql(expected, q2)
    end

    test "field equality - %{published_at: ~U[2026-01-01 00:00:00Z]}" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.published_at == ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: dt}, [])

      assert_sql(expected, q2)
    end

    test "field equality - %{published_at: nil}" do
      expected = from p in Post, where: is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: nil}, [])

      assert_sql(expected, q2)
    end

    test "field equality - %{published: [true, false]} (non-keyword list defaults to IN)" do
      expected = from p in Post, where: p.published in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: [true, false]}, [])

      assert_sql(expected, q2)
    end

    test "field equality - [id: 1] (keyword list)" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [id: 1], [])

      assert_sql(expected, q2)
    end

    test "field equality - [%{id: 1}] (list of maps)" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [%{id: 1}], [])

      assert_sql(expected, q2)
    end

    test "field equality - [[id: 1]] (list of keyword lists)" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [[id: 1]], [])

      assert_sql(expected, q2)
    end

    test "field equality - [%{id: 1}, %{published: true}] (list of maps, multiple entries)" do
      expected =
        from(p in Post,
          where: p.id == ^1,
          where: p.published == ^true
        )

      q2 = CommonFilters.convert_params_to_filter(Post, [%{id: 1}, %{published: true}], [])

      assert_sql(expected, q2)
    end

    test "field equality - [[id: 1], [published: true]] (list of keyword lists, multiple entries)" do
      expected =
        from(p in Post,
          where: p.id == ^1,
          where: p.published == ^true
        )

      q2 = CommonFilters.convert_params_to_filter(Post, [[id: 1], [published: true]], [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 source types" do
    test "supports Ecto.Query.t() source" do
      expected = from p in Post, where: p.published == ^true
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{published: true}, [])

      assert_sql(expected, q2)
    end

    test "supports Ecto.Schema.t() schema module source" do
      expected = from p in Post, select: p
      q2 = CommonFilters.convert_params_to_filter(Post, %{select: true}, [])

      assert_sql(expected, q2)
    end

    test "supports binary() table name source" do
      expected = from p in "posts", select: [:id]
      q2 = CommonFilters.convert_params_to_filter("posts", %{select: [:id]}, [])

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
  end

  describe "convert_params_to_filter/3 :from params" do
    test ":from - [published: true, subquery: %{id: 2}]" do
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

    test ":from - %{from: %{query: Post, id: 1}, published: true}" do
      expected =
        from(p in Post,
          where: p.id == ^1,
          where: p.published == ^true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{from: %{query: Post, id: 1}, published: true},
          []
        )

      assert_sql(expected, q2)
    end

    test ":from - %{from: %{query: Post, id: 1}}" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{from: %{query: Post, id: 1}}, [])

      assert_sql(expected, q2)
    end

    test ":from - [from: [query: Post, id: 1]]" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [from: [query: Post, id: 1]], [])

      assert_sql(expected, q2)
    end

    test ":from - %{from: %{query: \"posts\", id: 1}}" do
      q2 = CommonFilters.convert_params_to_filter(Post, %{from: %{query: "posts", id: 1}}, [])

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, EctoShorts.Repo, q2)

      assert sql =~ "WHERE"
      assert sql =~ "\"id\" = $1"
      assert params == [1]
    end

    test ":from - %{from: %{query: \"posts\", select: [:id]}}" do
      expected = from p in "posts", select: ^[:id]
      q2 = CommonFilters.convert_params_to_filter(Post, %{from: %{query: "posts", select: [:id]}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 custom filters" do
    test "custom filter - %{ids: [1, 2, 3]}" do
      expected = from p in Post, where: p.id in ^[1, 2, 3]
      q2 = CommonFilters.convert_params_to_filter(Post, %{ids: [1, 2, 3]}, [])

      assert_sql(expected, q2)
    end

    test "custom filter - %{after: 10}" do
      expected = from p in Post, where: p.id > ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{after: 10}, [])

      assert_sql(expected, q2)
    end

    test "custom filter - %{before: 10}" do
      expected = from p in Post, where: p.id < ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{before: 10}, [])

      assert_sql(expected, q2)
    end

    test "custom filter - %{start_date: ~U[2026-01-01 00:00:00Z]}" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.inserted_at >= ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{start_date: dt}, [])

      assert_sql(expected, q2)
    end

    test "custom filter - %{end_date: ~U[2026-12-31 23:59:59Z]}" do
      dt = ~U[2026-12-31 23:59:59Z]
      expected = from p in Post, where: p.inserted_at <= ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{end_date: dt}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 error handling" do
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
  end

  describe "convert_params_to_filter/3 with nil source" do
    test "nil source with :from in map params" do
      expected = from p in "posts", where: p.id == ^1, select: ^[:id]
      q2 = CommonFilters.convert_params_to_filter(nil, %{from: %{query: "posts", id: 1}, select: [:id]}, [])

      assert_sql(expected, q2)
    end

    test "nil source with :from in keyword list params" do
      expected = from p in "posts", where: p.id == ^1, select: ^[:id]

      q2 =
        CommonFilters.convert_params_to_filter(
          nil,
          [from: %{query: "posts", id: 1}, select: [:id]],
          []
        )

      assert_sql(expected, q2)
    end

    test "nil source auto-adds select: true when :select is omitted" do
      q2 = CommonFilters.convert_params_to_filter(nil, %{from: %{query: "posts", id: 1}}, [])

      assert %Ecto.Query{select: %Ecto.Query.SelectExpr{}} = q2
    end

    test "nil source raises when :from is missing from params" do
      assert_raise ArgumentError, ~r/from/, fn ->
        CommonFilters.convert_params_to_filter(nil, %{id: 1}, [])
      end
    end

    test "nil source raises for non-keyword list params" do
      assert_raise ArgumentError, ~r/from/, fn ->
        CommonFilters.convert_params_to_filter(nil, [%{from: %{query: "posts"}, id: 1}], [])
      end
    end

    test "nil source raises when :from is missing :query key" do
      assert_raise ArgumentError, ~r/:query/, fn ->
        CommonFilters.convert_params_to_filter(nil, %{from: %{id: 1}}, [])
      end
    end
  end
end
