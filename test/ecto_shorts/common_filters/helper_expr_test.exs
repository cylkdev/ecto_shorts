defmodule EctoShorts.CommonFilters.HelperExprTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias Ecto.Adapters.SQL
  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 helper expressions" do
    test "supports scalar all helper expression in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: p.id > all(subquery_expr)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{all: %{>: subquery_expr}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports scalar all helper expression query-builder payload in :where" do
      subquery_expr =
        Post
        |> CommonFilters.convert_params_to_filter(%{id: 1}, [])
        |> select([p], p.id)

      expected =
        from(p in Post,
          where: p.id > all(subquery_expr)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{all: %{>: [source: Post, query: %{id: 1}]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports exists helper expression in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: exists(subquery_expr)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{where: %{exists: subquery_expr}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated exists helper expression in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: not exists(subquery_expr)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{where: %{exists: %{not: subquery_expr}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports arithmetic helper expressions in :where" do
      expected =
        from(p in Post,
          where: p.views > p.views + ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>: %{+: [:views, 10]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports datetime_add helper map payload in :where" do
      expected =
        from(p in Post,
          where: p.inserted_at >= datetime_add(p.inserted_at, ^1, ^"day")
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            inserted_at: %{>=: %{datetime_add: %{field: :inserted_at, count: 1, interval: "day"}}}
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports ago helper map payload in :having" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            group_by: :id,
            having: %{inserted_at: %{>: %{ago: %{count: 1, interval: "day"}}}}
          },
          []
        )

      {sql, params} = SQL.to_sql(:all, EctoShorts.Repo, q2)

      assert sql =~ "GROUP BY p0.\"id\""

      assert sql =~
               "HAVING (p0.\"inserted_at\" > $1::timestamp + ($2::numeric * interval '1 day'))"

      assert match?([%DateTime{}, %Decimal{}], params)
      assert Enum.at(params, 1) === Decimal.new("-1")
    end

    test "supports scalar any helper expression with > in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: p.id > any(subquery_expr)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{any: %{>: subquery_expr}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports scalar any helper expression with implicit == in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: p.id == any(subquery_expr)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{any: subquery_expr}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated scalar any helper expression in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: not (p.id > any(subquery_expr))
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{not: %{any: %{>: subquery_expr}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated scalar any helper expression with implicit == in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: p.id != any(subquery_expr)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{not: %{any: subquery_expr}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated scalar all helper expression in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: not (p.id > all(subquery_expr))
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{not: %{all: %{>: subquery_expr}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports scalar all helper expression with implicit == in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: p.id == all(subquery_expr)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{all: subquery_expr}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated scalar all helper expression with implicit == in :where" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected =
        from(p in Post,
          where: p.id != all(subquery_expr)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{not: %{all: subquery_expr}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :count aggregate operator in :having" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: count(p.views) > ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{count: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :max aggregate operator in :having" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: max(p.views) > ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{max: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :min aggregate operator in :having" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: min(p.views) > ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{min: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :sum aggregate operator in :having" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: sum(p.views) > ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{sum: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports aggregate with implicit == operator" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: avg(p.views) == ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: 10}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated aggregate operator" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: not (avg(p.views) > ^10)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{not: %{avg: %{>: 10}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports aggregate with alias comparison operator" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: avg(p.views) >= ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{gte: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports datetime_add helper with implicit == operator" do
      expected =
        from(p in Post,
          where: p.inserted_at == datetime_add(p.inserted_at, ^1, ^"day")
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            inserted_at: %{datetime_add: %{field: :inserted_at, count: 1, interval: "day"}}
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports date_add helper with >= operator" do
      expected =
        from(p in Post,
          where: p.inserted_at >= date_add(p.inserted_at, ^1, ^"day")
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            inserted_at: %{>=: %{date_add: %{field: :inserted_at, count: 1, interval: "day"}}}
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports from_now helper with > operator" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            inserted_at: %{>: %{from_now: %{count: 1, interval: "day"}}}
          },
          []
        )

      {sql, params} = SQL.to_sql(:all, EctoShorts.Repo, q2)

      assert sql =~
               "WHERE (p0.\"inserted_at\" > $1::timestamp + ($2::numeric * interval '1 day'))"

      assert match?([%DateTime{}, %Decimal{}], params)
      assert Enum.at(params, 1) === Decimal.new("1")
    end

    test "supports ago helper with > operator in :where" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            inserted_at: %{>: %{ago: %{count: 1, interval: "day"}}}
          },
          []
        )

      {sql, params} = SQL.to_sql(:all, EctoShorts.Repo, q2)

      assert sql =~
               "WHERE (p0.\"inserted_at\" > $1::timestamp + ($2::numeric * interval '1 day'))"

      assert match?([%DateTime{}, %Decimal{}], params)
      assert Enum.at(params, 1) === Decimal.new("-1")
    end

    test "supports subtraction arithmetic helper expression in :where" do
      expected =
        from(p in Post,
          where: p.views > p.views - ^10
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>: %{-: [:views, 10]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports multiplication arithmetic helper expression in :where" do
      expected =
        from(p in Post,
          where: p.views > p.views * ^2
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>: %{*: [:views, 2]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports division arithmetic helper expression in :where" do
      expected =
        from(p in Post,
          where: p.views > p.views / ^2
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>: %{/: [:views, 2]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated arithmetic helper expression in :where" do
      expected =
        from(p in Post,
          where: not (p.views > p.views + ^10)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{not: %{>: %{+: [:views, 10]}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end
end
