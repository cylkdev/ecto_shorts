defmodule EctoShorts.CommonFilters.HelperExprTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias Ecto.Adapters.SQL
  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 aggregate functions" do
    test "filters where the average is greater than the value" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average is greater than or equal to the value" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) >= ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{>=: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average is less than the value" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) < ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{<: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average is less than or equal to the value" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) <= ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{<=: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average equals the value" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) == ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{==: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average does not equal the value" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) != ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{!=: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average is greater than the value using the gt alias" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{gt: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average is greater than or equal using the gte alias" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) >= ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{gte: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average is less than the value using the lt alias" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) < ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{lt: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average is less than or equal using the lte alias" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) <= ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{lte: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average equals the value using the eq alias" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) == ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{eq: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the average does not equal the value using the ne alias" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) != ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: %{ne: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the count is greater than the value" do
      expected = from(p in Post, group_by: p.author_id, having: count(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{count: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the max is greater than the value" do
      expected = from(p in Post, group_by: p.author_id, having: max(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{max: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the min is greater than the value" do
      expected = from(p in Post, group_by: p.author_id, having: min(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{min: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters where the sum is greater than the value" do
      expected = from(p in Post, group_by: p.author_id, having: sum(p.views) > ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{sum: %{>: 10}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "defaults to equality when no comparison operator is given for an aggregate" do
      expected = from(p in Post, group_by: p.author_id, having: avg(p.views) == ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{avg: 10}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "excludes records where the average is greater than the value" do
      expected = from(p in Post, group_by: p.author_id, having: not (avg(p.views) > ^10))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{views: %{not: %{avg: %{>: 10}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "excludes records where the average equals the value" do
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
    test "compares the field against all values in a subquery using >" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id > all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{all: %{>: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against all values in a subquery using >=" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id >= all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{all: %{>=: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against all values in a subquery using <" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id < all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{all: %{<: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against all values in a subquery using <=" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id <= all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{all: %{<=: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against all values in a subquery using ==" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id == all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{all: %{==: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against all values in a subquery using !=" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id != all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{all: %{!=: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against all values in a subquery using the ne alias" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id != all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{all: %{ne: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "defaults to equality when comparing against all subquery values" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id == all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{all: subquery_expr}}, [])

      assert_sql(expected, q2)
    end

    test "builds a subquery from a query-builder payload for an all comparison" do
      subquery_expr =
        Post
        |> CommonFilters.convert_params_to_filter(%{id: 1}, [])
        |> select([p], p.id)

      expected = from(p in Post, where: p.id == all(subquery_expr))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{all: %{from: Post, id: 1}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "negates a greater-than comparison against all subquery values" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: not (p.id > all(subquery_expr)))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{all: %{>: subquery_expr}}}}, [])

      assert_sql(expected, q2)
    end

    test "negates an equality comparison against all subquery values" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id != all(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{all: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against any value in a subquery using >" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id > any(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{any: %{>: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "defaults to equality when comparing against any subquery value" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id == any(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{any: subquery_expr}}, [])

      assert_sql(expected, q2)
    end

    test "negates a greater-than comparison against any subquery value" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: not (p.id > any(subquery_expr)))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{any: %{>: subquery_expr}}}}, [])

      assert_sql(expected, q2)
    end

    test "negates an equality comparison against any subquery value" do
      subquery_expr = from(c in "comments", select: c.post_id)
      expected = from(p in Post, where: p.id != any(subquery_expr))
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{not: %{any: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 arithmetic expressions" do
    test "compares the field against an addition expression" do
      expected = from(p in Post, where: p.views > p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against a subtraction expression" do
      expected = from(p in Post, where: p.views > p.views - ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: %{-: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against a multiplication expression using >" do
      expected = from(p in Post, where: p.views > p.views * ^2)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: %{*: [:views, 2]}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against a multiplication expression using ==" do
      expected = from(p in Post, where: p.views == p.views * ^2)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{==: %{*: [:views, 2]}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against a division expression" do
      expected = from(p in Post, where: p.views > p.views / ^2)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: %{/: [:views, 2]}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against an addition expression using >=" do
      expected = from(p in Post, where: p.views >= p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>=: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against an addition expression using <" do
      expected = from(p in Post, where: p.views < p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against an addition expression using <=" do
      expected = from(p in Post, where: p.views <= p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<=: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against an addition expression using ==" do
      expected = from(p in Post, where: p.views == p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{==: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "compares the field against an addition expression using !=" do
      expected = from(p in Post, where: p.views != p.views + ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{!=: %{+: [:views, 10]}}}, [])

      assert_sql(expected, q2)
    end

    test "negates an arithmetic comparison" do
      expected = from(p in Post, where: not (p.views > p.views + ^10))
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{>: %{+: [:views, 10]}}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 date/time expressions" do
    test "compares the field against a datetime_add expression using >=" do
      expected = from(p in Post, where: p.inserted_at >= datetime_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "compares the field against a datetime_add expression using ==" do
      expected = from(p in Post, where: p.inserted_at == datetime_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{==: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "compares the field against a date_add expression using >=" do
      expected = from(p in Post, where: p.inserted_at >= date_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{>=: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "compares the field against a date_add expression using ==" do
      expected = from(p in Post, where: p.inserted_at == date_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{==: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "defaults to equality for a datetime_add expression" do
      expected = from(p in Post, where: p.inserted_at == datetime_add(p.inserted_at, ^1, ^"day"))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{inserted_at: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "compares the field against an ago expression using >" do
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

    test "compares the field against an ago expression using ==" do
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

    test "compares the field against a from_now expression using >" do
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

    test "compares the field against a from_now expression using ==" do
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

    test "negates a datetime_add comparison" do
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

    test "negates an equality datetime_add comparison" do
      expected =
        from(p in Post,
          where: not (p.inserted_at == datetime_add(p.inserted_at, ^1, ^"day"))
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

    test "uses an ago expression inside a having clause" do
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
