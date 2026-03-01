defmodule EctoShorts.CommonFilters.HelperExprTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias Ecto.Adapters.SQL
  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 aggregate functions" do
    test "aggregate — %{views: %{avg: %{>: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{>=: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) >= ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{>=: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{<: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) < ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{<: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{<=: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) <= ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{<=: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{==: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) == ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{==: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{!=: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) != ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{!=: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{gt: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{gt: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{gte: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) >= ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{gte: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{lt: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) < ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{lt: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{lte: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) <= ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{lte: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: %{eq: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) == ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{eq: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{count: %{>: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: count(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{count: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{max: %{>: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: max(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{max: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{min: %{>: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: min(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{min: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{sum: %{>: 10}}}" do
      expected = from(p in Post, group_by: p.author_id, having: sum(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{sum: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{avg: 10}} (implicit ==)" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) == ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: 10}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{not: %{avg: %{>: 10}}}}" do
      expected = from(p in Post, group_by: p.author_id, having: not (avg(p.views) > ^10))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{not: %{avg: %{>: 10}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "aggregate — %{views: %{not: %{avg: %{==: 10}}}}" do
      expected = from(p in Post, group_by: p.author_id, having: not (avg(p.views) == ^10))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{not: %{avg: %{==: 10}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 subquery and set comparison" do
    test "subquery/set — %{id: %{>: %{all: subquery_expr}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id > all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{>: %{all: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{>=: %{all: subquery_expr}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id >= all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{>=: %{all: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{<: %{all: subquery_expr}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id < all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{<: %{all: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{<=: %{all: subquery_expr}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id <= all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{<=: %{all: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{==: %{all: subquery_expr}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id == all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{==: %{all: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{!=: %{all: subquery_expr}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id != all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{!=: %{all: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{all: subquery_expr}} (implicit ==)" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id == all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{all: subquery_expr}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{>: %{all: [source: Post, query: %{id: 1}]}}} (query-builder payload)" do
      subquery_expr =
        Post
        |> CommonFilters.convert_params_to_filter(%{id: 1}, [])
        |> select([p], p.id)

      expected = from(p in Post, where: p.id > all(subquery_expr))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{>: %{all: [source: Post, query: %{id: 1}]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{not: %{>: %{all: subquery_expr}}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: not (p.id > all(subquery_expr)))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{>: %{all: subquery_expr}}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{not: %{all: subquery_expr}}} (negated implicit ==)" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id != all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{all: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{>: %{any: subquery_expr}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id > any(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{>: %{any: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{any: subquery_expr}} (implicit ==)" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id == any(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{any: subquery_expr}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{not: %{>: %{any: subquery_expr}}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: not (p.id > any(subquery_expr)))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{>: %{any: subquery_expr}}}}, [])

      assert_sql(expected, q2)
    end

    test "subquery/set — %{id: %{not: %{any: subquery_expr}}} (negated implicit ==)" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id != any(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{any: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 arithmetic expressions" do
    test "arithmetic — %{views: %{>: %{+: [:views, 10]}}}" do
      expected = from(p in Post, where: p.views > p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{>: %{-: [:views, 10]}}}" do
      expected = from(p in Post, where: p.views > p.views - ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: %{-: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{>: %{*: [:views, 2]}}}" do
      expected = from(p in Post, where: p.views > p.views * ^2)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: %{*: [:views, 2]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{==: %{*: [:views, 2]}}}" do
      expected = from(p in Post, where: p.views == p.views * ^2)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{==: %{*: [:views, 2]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{>: %{/: [:views, 2]}}}" do
      expected = from(p in Post, where: p.views > p.views / ^2)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: %{/: [:views, 2]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{>=: %{+: [:views, 10]}}}" do
      expected = from(p in Post, where: p.views >= p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>=: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{<: %{+: [:views, 10]}}}" do
      expected = from(p in Post, where: p.views < p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{<=: %{+: [:views, 10]}}}" do
      expected = from(p in Post, where: p.views <= p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<=: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{==: %{+: [:views, 10]}}}" do
      expected = from(p in Post, where: p.views == p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{==: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{!=: %{+: [:views, 10]}}}" do
      expected = from(p in Post, where: p.views != p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{!=: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "arithmetic — %{views: %{not: %{>: %{+: [:views, 10]}}}}" do
      expected = from(p in Post, where: not (p.views > p.views + ^10))
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{>: %{+: [:views, 10]}}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 date/time expressions" do
    test "datetime — %{inserted_at: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: \"day\"}}}}}" do
      expected = from(p in Post, where: p.inserted_at >= datetime_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "datetime — %{inserted_at: %{==: %{datetime: %{add: ...}}}}" do
      expected = from(p in Post, where: p.inserted_at == datetime_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{==: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "datetime — %{inserted_at: %{>=: %{date: %{add: ...}}}}" do
      expected = from(p in Post, where: p.inserted_at >= date_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>=: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "datetime — %{inserted_at: %{==: %{date: %{add: ...}}}}" do
      expected = from(p in Post, where: p.inserted_at == date_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{==: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "datetime — implicit == with datetime: add" do
      expected = from(p in Post, where: p.inserted_at == datetime_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "datetime — %{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: \"day\"}}}}}" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: "day"}}}}},
          []
        )

      {sql, params} = SQL.to_sql(:all, EctoShorts.Repo, q2)

      assert sql =~ "WHERE (p0.\"inserted_at\" > $1::timestamp + ($2::numeric * interval '1 day'))"
      assert match?([%DateTime{}, %Decimal{}], params)
      assert Enum.at(params, 1) === Decimal.new("-1")
    end

    test "datetime — %{inserted_at: %{==: %{datetime: %{ago: ...}}}}" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{==: %{datetime: %{ago: %{count: 1, interval: "day"}}}}},
          []
        )

      {sql, params} = SQL.to_sql(:all, EctoShorts.Repo, q2)

      assert sql =~ "WHERE (p0.\"inserted_at\" = $1::timestamp + ($2::numeric * interval '1 day'))"
      assert match?([%DateTime{}, %Decimal{}], params)
      assert Enum.at(params, 1) === Decimal.new("-1")
    end

    test "datetime — %{inserted_at: %{>: %{datetime: %{from_now: %{count: 1, interval: \"day\"}}}}}" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>: %{datetime: %{from_now: %{count: 1, interval: "day"}}}}},
          []
        )

      {sql, params} = SQL.to_sql(:all, EctoShorts.Repo, q2)

      assert sql =~ "WHERE (p0.\"inserted_at\" > $1::timestamp + ($2::numeric * interval '1 day'))"
      assert match?([%DateTime{}, %Decimal{}], params)
      assert Enum.at(params, 1) === Decimal.new("1")
    end

    test "datetime — %{inserted_at: %{==: %{datetime: %{from_now: ...}}}}" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{==: %{datetime: %{from_now: %{count: 1, interval: "day"}}}}},
          []
        )

      {sql, params} = SQL.to_sql(:all, EctoShorts.Repo, q2)

      assert sql =~ "WHERE (p0.\"inserted_at\" = $1::timestamp + ($2::numeric * interval '1 day'))"
      assert match?([%DateTime{}, %Decimal{}], params)
      assert Enum.at(params, 1) === Decimal.new("1")
    end

    test "datetime — negated %{inserted_at: %{not: %{>=: %{datetime: %{add: ...}}}}}" do
      expected =
        from(p in Post,
          where: not (p.inserted_at >= datetime_add(p.inserted_at, ^1, ^"day"))
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            inserted_at: %{
              not: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "datetime — negated %{inserted_at: %{not: %{==: %{datetime: %{add: ...}}}}}" do
      expected =
        from(p in Post,
          where: p.inserted_at != datetime_add(p.inserted_at, ^1, ^"day")
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            inserted_at: %{
              not: %{==: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "datetime — ago helper in :having" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :id, having: %{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: "day"}}}}}},
          []
        )

      {sql, params} = SQL.to_sql(:all, EctoShorts.Repo, q2)

      assert sql =~ "GROUP BY p0.\"id\""
      assert sql =~ "HAVING (p0.\"inserted_at\" > $1::timestamp + ($2::numeric * interval '1 day'))"
      assert match?([%DateTime{}, %Decimal{}], params)
      assert Enum.at(params, 1) === Decimal.new("-1")
    end
  end
end
