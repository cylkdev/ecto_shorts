defmodule EctoShorts.CommonFilters.CoreTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 field equality" do
    test "filters by integer field value" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: 1}, [])

      assert_sql(expected, q2)
    end

    test "filters by boolean field value" do
      expected = from p in Post, where: p.published == ^true
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: true}, [])

      assert_sql(expected, q2)
    end

    test "filters by string field value" do
      expected = from p in Post, where: p.title == ^"hello"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: "hello"}, [])

      assert_sql(expected, q2)
    end

    test "filters by datetime field value" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.published_at == ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: dt}, [])

      assert_sql(expected, q2)
    end

    test "filters for nil field value using IS NULL" do
      expected = from p in Post, where: is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: nil}, [])

      assert_sql(expected, q2)
    end

    test "treats a plain list value as an IN check" do
      expected = from p in Post, where: p.published in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: [true, false]}, [])

      assert_sql(expected, q2)
    end

    test "accepts a keyword list as params" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [id: 1], [])

      assert_sql(expected, q2)
    end

    test "accepts a list of maps as params" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [%{id: 1}], [])

      assert_sql(expected, q2)
    end

    test "accepts a list of keyword lists as params" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [[id: 1]], [])

      assert_sql(expected, q2)
    end

    test "applies each map in a list as a separate WHERE condition" do
      expected =
        from(p in Post,
          where: p.id == ^1,
          where: p.published == ^true
        )

      q2 = CommonFilters.convert_params_to_filter(Post, [%{id: 1}, %{published: true}], [])

      assert_sql(expected, q2)
    end

    test "applies each keyword list in a list as a separate WHERE condition" do
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
    test "accepts an Ecto.Query as the source" do
      expected = from p in Post, where: p.published == ^true
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{published: true}, [])

      assert_sql(expected, q2)
    end

    test "accepts a schema module as the source" do
      expected = from p in Post, select: p
      q2 = CommonFilters.convert_params_to_filter(Post, %{select: true}, [])

      assert_sql(expected, q2)
    end

    test "accepts a table name string as the source" do
      expected = from p in "posts", select: [:id]
      q2 = CommonFilters.convert_params_to_filter("posts", %{select: [:id]}, [])

      assert_sql(expected, q2)
    end

    test "accepts a table-and-schema tuple as the source" do
      expected = from p in {"custom_posts", Post}, select: p
      q2 = CommonFilters.convert_params_to_filter({"custom_posts", Post}, %{select: true}, [])

      assert_sql(expected, q2)
    end

    test "accepts a nil-and-schema tuple as the source" do
      expected = from p in Post, select: p
      q2 = CommonFilters.convert_params_to_filter({nil, Post}, %{select: true}, [])

      assert_sql(expected, q2)
    end

    test "accepts a table-and-nil tuple as the source" do
      expected = from p in "posts", select: [:id]
      q2 = CommonFilters.convert_params_to_filter({"posts", nil}, %{select: [:id]}, [])

      assert_sql(expected, q2)
    end

    test "adds default select when the source is a bare table string and select is not given" do
      q2 = CommonFilters.convert_params_to_filter("posts", %{id: 1}, [])

      assert %Ecto.Query{select: %Ecto.Query.SelectExpr{}} = q2
    end

    test "adds default select when the source is a {table, nil} tuple and select is not given" do
      q2 = CommonFilters.convert_params_to_filter({"posts", nil}, %{id: 1}, [])

      assert %Ecto.Query{select: %Ecto.Query.SelectExpr{}} = q2
    end

    test "preserves explicit select when the source is a bare table string" do
      expected = from p in "posts", where: p.id == ^1, select: [:id]
      q2 = CommonFilters.convert_params_to_filter("posts", %{id: 1, select: [:id]}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 :from params" do
    test "wraps the query in a subquery when the subquery key is present" do
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

    test "merges :from source with top-level filters using a map" do
      expected =
        from(p in Post,
          where: p.id == ^1,
          where: p.published == ^true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{from: Post, id: 1, published: true},
          []
        )

      assert_sql(expected, q2)
    end

    test "builds a query from the flat :from key alone" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{from: Post, id: 1}, [])

      assert_sql(expected, q2)
    end

    test "builds a query from the flat :from keyword list" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, [from: Post, id: 1], [])

      assert_sql(expected, q2)
    end

    test "builds a query from a table name string as :from value" do
      q2 = CommonFilters.convert_params_to_filter(Post, %{from: "posts", id: 1}, [])

      assert %Ecto.Query{} = q2
      assert %Ecto.Query.SelectExpr{} = q2.select
      assert [%Ecto.Query.BooleanExpr{}] = q2.wheres
    end

    test "adds a select when the :from source is a table name string" do
      expected = from p in "posts", select: ^[:id]
      q2 = CommonFilters.convert_params_to_filter(Post, %{from: "posts", select: [:id]}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 custom filters" do
    test "filters by a list of ids using the :ids shortcut" do
      expected = from p in Post, where: p.id in ^[1, 2, 3]
      q2 = CommonFilters.convert_params_to_filter(Post, %{ids: [1, 2, 3]}, [])

      assert_sql(expected, q2)
    end

    test "filters for records after the given id" do
      expected = from p in Post, where: p.id > ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{after: 10}, [])

      assert_sql(expected, q2)
    end

    test "filters for records before the given id" do
      expected = from p in Post, where: p.id < ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{before: 10}, [])

      assert_sql(expected, q2)
    end

    test "filters for records on or after the start date" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.inserted_at >= ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{start_date: dt}, [])

      assert_sql(expected, q2)
    end

    test "filters for records on or before the end date" do
      dt = ~U[2026-12-31 23:59:59Z]
      expected = from p in Post, where: p.inserted_at <= ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{end_date: dt}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 error handling" do
    test "logs a warning and returns the query unchanged for invalid params" do
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

    test "logs a warning and returns the query unchanged for an invalid where value" do
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

    test "logs a warning and returns the query unchanged for an invalid bind value" do
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
               "Expected :as value to be a non-nil atom"

      assert_received {:q2, q2}
      assert q2 === q
    end

    test "logs a warning and returns the query unchanged for an unknown field" do
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

    test "logs a warning and returns the query unchanged for a non-dynamic value" do
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

  describe "convert_params_to_filter/3 with Source" do
    alias EctoShorts.Source

    test "builds a query when the table resolves to a schema module" do
      sq = %Source{tables: %{"posts" => Post}}
      expected = from p in Post, where: p.id == ^1

      q2 = CommonFilters.convert_params_to_filter(sq, %{table: "posts", id: 1}, [])

      assert_sql(expected, q2)
    end

    test "builds a query when the table resolves to a table string" do
      sq = %Source{tables: %{"posts" => "posts"}}
      expected = from p in "posts", select: ^[:id]

      q2 = CommonFilters.convert_params_to_filter(sq, %{table: "posts", select: [:id]}, [])

      assert_sql(expected, q2)
    end

    test "merges source_key with top-level params" do
      sq = %Source{tables: %{"posts" => Post}}

      expected =
        from(p in Post,
          where: p.id == ^1,
          where: p.published == ^true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          sq,
          %{table: "posts", id: 1, published: true},
          []
        )

      assert_sql(expected, q2)
    end

    test "adds default select when resolved source is schemaless and select is not given" do
      sq = %Source{tables: %{"posts" => "posts"}}

      q2 = CommonFilters.convert_params_to_filter(sq, %{table: "posts", id: 1}, [])

      assert %Ecto.Query{select: %Ecto.Query.SelectExpr{}} = q2
    end

    test "accepts source_key as a keyword list" do
      sq = %Source{tables: %{"posts" => Post}}
      expected = from p in Post, where: p.id == ^1

      q2 =
        CommonFilters.convert_params_to_filter(
          sq,
          [table: "posts", id: 1],
          []
        )

      assert_sql(expected, q2)
    end

    test "raises when source_key is missing from params" do
      sq = %Source{tables: %{"posts" => Post}}

      assert_raise ArgumentError, ~r/:table/, fn ->
        CommonFilters.convert_params_to_filter(sq, %{id: 1}, [])
      end
    end

    test "raises when the table name is not found in the tables map" do
      sq = %Source{tables: %{"posts" => Post}}

      assert_raise ArgumentError, ~r/table not found/, fn ->
        CommonFilters.convert_params_to_filter(sq, %{table: "missing"}, [])
      end
    end

    test "resolves the table using a custom source_key" do
      sq = %Source{tables: %{"posts" => Post}, source_key: :source}
      expected = from p in Post, where: p.id == ^1

      q2 = CommonFilters.convert_params_to_filter(sq, %{source: "posts", id: 1}, [])

      assert_sql(expected, q2)
    end

    test "raises when the custom source_key is missing from params" do
      sq = %Source{tables: %{"posts" => Post}, source_key: :source}

      assert_raise ArgumentError, ~r/:source/, fn ->
        CommonFilters.convert_params_to_filter(sq, %{table: "posts"}, [])
      end
    end

    test "raises when nil is passed as source" do
      assert_raise ArgumentError, fn ->
        CommonFilters.convert_params_to_filter(nil, %{id: 1}, [])
      end
    end
  end
end
