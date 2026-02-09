defmodule EctoShorts.DynamicsTest do
  use ExUnit.Case
  use EctoShorts.Testing

  import Ecto.Query

  alias EctoShorts.Dynamics
  alias EctoShorts.Schema.Post

  test "convert_to_dynamic supports common operator :ids" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{ids: [1, 2]})
    expected = dynamic([q], field(q, :id) in ^[1, 2])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports common operator :after" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{after: 10})
    expected = dynamic([q], field(q, :id) > ^10)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports common operator :before" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{before: 5})
    expected = dynamic([q], field(q, :id) < ^5)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports common operator :start_date" do
    binding = {:as, nil}
    date = ~U[2024-01-01 00:00:00Z]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{start_date: date})
    expected = dynamic([q], field(q, :inserted_at) >= ^date)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports common operator :end_date" do
    binding = {:as, nil}
    date = ~U[2024-12-31 00:00:00Z]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{end_date: date})
    expected = dynamic([q], field(q, :inserted_at) <= ^date)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports boolean operator :or" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{or: [published: true, published: false]})

    expected =
      dynamic(
        [q],
        field(q, ^:published) == ^true or field(q, ^:published) == ^false
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports composite boolean :or expressions" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        or: [
          %{id: 1, published: true},
          %{id: 2, published: false}
        ]
      })

    expected =
      dynamic(
        [q],
        (field(q, ^:id) == ^1 and field(q, ^:published) == ^true) or
          (field(q, ^:id) == ^2 and field(q, ^:published) == ^false)
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports boolean operator :and" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{and: [published: true, title: "hi"]})

    expected =
      dynamic(
        [q],
        field(q, ^:published) == ^true and field(q, ^:title) == ^"hi"
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports boolean operator :or with map value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{or: [published: true, title: "hi"]})

    expected =
      dynamic(
        [q],
        field(q, ^:published) == ^true or field(q, ^:title) == ^"hi"
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic preserves struct values (DateTime) for scalar comparisons" do
    binding = {:as, nil}
    dt = ~U[2026-01-01 00:00:00Z]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{published_at: dt})

    expected =
      dynamic(
        [q],
        field(q, ^:published_at) == ^dt
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic preserves struct values (DateTime) for operator map comparisons" do
    binding = {:as, nil}
    dt = ~U[2026-01-01 00:00:00Z]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{published_at: %{>=: dt}})

    expected =
      dynamic(
        [q],
        field(q, ^:published_at) >= ^dt
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar nil equality" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{==: nil}})
    expected = dynamic([q], is_nil(field(q, ^:title)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar nil equality with :eq alias" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{eq: nil}})
    expected = dynamic([q], is_nil(field(q, ^:title)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar nil inequality" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{!=: nil}})
    expected = dynamic([q], not is_nil(field(q, ^:title)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar :in with {:all, values} shape" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{in: {:all, ["a", "b"]}}})
    expected = dynamic([q], field(q, ^:title) in ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not :in with {:all, values} shape" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{in: {:all, ["a", "b"]}}}})

    expected = dynamic([q], field(q, ^:title) not in ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar lower" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{lower: "foo"}})
    expected = dynamic([q], fragment("lower(?)", field(q, ^:title)) == ^"foo")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar upper" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{upper: "foo"}})
    expected = dynamic([q], fragment("upper(?)", field(q, ^:title)) == ^"foo")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not lower" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{lower: "foo"}}})

    expected =
      dynamic([q], not (fragment("lower(?)", field(q, ^:title)) == ^"foo"))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not upper" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{upper: "foo"}}})

    expected =
      dynamic([q], not (fragment("upper(?)", field(q, ^:title)) == ^"foo"))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar == lower" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{==: %{lower: "foo"}}})
    expected = dynamic([q], fragment("lower(?)", field(q, ^:title)) == ^"foo")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar == upper" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{==: %{upper: "foo"}}})
    expected = dynamic([q], fragment("upper(?)", field(q, ^:title)) == ^"foo")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar != lower" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{!=: %{lower: "foo"}}})
    expected = dynamic([q], fragment("lower(?)", field(q, ^:title)) != ^"foo")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar != upper" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{!=: %{upper: "foo"}}})
    expected = dynamic([q], fragment("upper(?)", field(q, ^:title)) != ^"foo")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar like with list" do
    binding = {:as, nil}
    patterns = ["%foo%", "%bar%"]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{like: ["foo", "bar"]}})
    expected = dynamic([q], fragment("? LIKE ANY(?)", field(q, ^:title), ^patterns))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar like with value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{like: "foo"}})
    expected = dynamic([q], like(field(q, ^:title), ^"%foo%"))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not like with list" do
    binding = {:as, nil}
    patterns = ["%foo%"]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{like: ["foo"]}}})
    expected = dynamic([q], not fragment("? LIKE ANY(?)", field(q, ^:title), ^patterns))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not like with value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{like: "foo"}}})
    expected = dynamic([q], not like(field(q, ^:title), ^"%foo%"))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar ilike with list" do
    binding = {:as, nil}
    patterns = ["%foo%"]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{ilike: ["foo"]}})
    expected = dynamic([q], fragment("? ILIKE ANY(?)", field(q, ^:title), ^patterns))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar ilike with value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{ilike: "foo"}})
    expected = dynamic([q], ilike(field(q, ^:title), ^"%foo%"))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not ilike with list" do
    binding = {:as, nil}
    patterns = ["%foo%"]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{ilike: ["foo"]}}})
    expected = dynamic([q], not fragment("? ILIKE ANY(?)", field(q, ^:title), ^patterns))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not ilike with value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{ilike: "foo"}}})
    expected = dynamic([q], not ilike(field(q, ^:title), ^"%foo%"))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not == with list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{==: ["a", "b"]}}})
    expected = dynamic([q], field(q, ^:title) not in ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not != with list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{!=: ["a", "b"]}}})
    expected = dynamic([q], field(q, ^:title) in ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar == with list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{==: ["a", "b"]}})
    expected = dynamic([q], field(q, ^:title) in ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar != with list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{!=: ["a", "b"]}})
    expected = dynamic([q], field(q, ^:title) not in ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not == with value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{==: "a"}}})
    expected = dynamic([q], field(q, ^:title) != ^"a")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not != with value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{!=: "a"}}})
    expected = dynamic([q], field(q, ^:title) == ^"a")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not in" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{not: %{in: ["a", "b"]}}})
    expected = dynamic([q], field(q, ^:title) not in ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not greater than" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{not: %{>: 1}}})
    expected = dynamic([q], not (field(q, ^:views) > ^1))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not greater than or equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{not: %{>=: 1}}})
    expected = dynamic([q], not (field(q, ^:views) >= ^1))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not less than" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{not: %{<: 1}}})
    expected = dynamic([q], not (field(q, ^:views) < ^1))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not less than or equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{not: %{<=: 1}}})
    expected = dynamic([q], not (field(q, ^:views) <= ^1))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar gt" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{gt: 1}})
    expected = dynamic([q], field(q, ^:views) > ^1)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar gte" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{gte: 1}})
    expected = dynamic([q], field(q, ^:views) >= ^1)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar lt" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{lt: 1}})
    expected = dynamic([q], field(q, ^:views) < ^1)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar lte" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{lte: 1}})
    expected = dynamic([q], field(q, ^:views) <= ^1)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar eq" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{eq: "a"}})
    expected = dynamic([q], field(q, ^:title) == ^"a")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar eq nil" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{eq: nil}})
    expected = dynamic([q], is_nil(field(q, ^:title)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not gt" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{not: %{gt: 1}}})
    expected = dynamic([q], not (field(q, ^:views) > ^1))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar greater than" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{>: 1}})
    expected = dynamic([q], field(q, ^:views) > ^1)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar greater than or equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{>=: 1}})
    expected = dynamic([q], field(q, ^:views) >= ^1)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar less than" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{<: 1}})
    expected = dynamic([q], field(q, ^:views) < ^1)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar less than or equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{<=: 1}})
    expected = dynamic([q], field(q, ^:views) <= ^1)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{==: "a"}})
    expected = dynamic([q], field(q, ^:title) == ^"a")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar not equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{!=: "a"}})
    expected = dynamic([q], field(q, ^:title) != ^"a")

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar in" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{title: %{in: ["a"]}})
    expected = dynamic([q], field(q, ^:title) in ^["a"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar avg helper expression on a field" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{avg: %{>: 10}}})
    expected = dynamic([q], avg(field(q, ^:views)) > ^10)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports top-level avg helper expression" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{avg: %{views: %{>: 10}}})
    expected = dynamic([q], avg(field(q, ^:views)) > ^10)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar count helper expression on a field" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{id: %{count: %{>: 1}}})
    expected = dynamic([q], count(field(q, ^:id)) > ^1)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar sum helper expression on a field" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{sum: %{>=: 10}}})
    expected = dynamic([q], sum(field(q, ^:views)) >= ^10)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar min helper expression on a field" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{min: %{<: 100}}})
    expected = dynamic([q], min(field(q, ^:views)) < ^100)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar max helper expression on a field" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{views: %{max: %{<=: 100}}})
    expected = dynamic([q], max(field(q, ^:views)) <= ^100)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar all helper expression on a field" do
    binding = {:as, nil}
    subquery_expr = from(c in "comments", select: c.post_id)

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{id: %{all: %{>: subquery_expr}}})

    expected = dynamic([q], field(q, ^:id) > all(subquery_expr))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar all helper expression query-builder payload" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        id: %{all: %{>: [source: Post, query: %{id: 1}]}}
      })

    subquery_expr =
      Post
      |> EctoShorts.CommonFilters.convert_params_to_filter(%{id: 1}, [])
      |> select([p], p.id)

    expected = dynamic([q], field(q, ^:id) > all(subquery_expr))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports negated scalar all helper expression on a field" do
    binding = {:as, nil}
    subquery_expr = from(c in "comments", select: c.post_id)

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{id: %{not: %{all: %{>: subquery_expr}}}})

    expected = dynamic([q], not (field(q, ^:id) > all(subquery_expr)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports negated scalar all helper expression query-builder payload" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        id: %{not: %{all: %{>: [source: Post, query: %{id: 1}]}}}
      })

    subquery_expr =
      Post
      |> EctoShorts.CommonFilters.convert_params_to_filter(%{id: 1}, [])
      |> select([p], p.id)

    expected = dynamic([q], not (field(q, ^:id) > all(subquery_expr)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar any helper expression on a field" do
    binding = {:as, nil}
    subquery_expr = from(c in "comments", select: c.post_id)

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{id: %{any: %{>: subquery_expr}}})

    expected = dynamic([q], field(q, ^:id) > any(subquery_expr))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports scalar any helper expression query-builder payload" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        id: %{any: %{>: [source: Post, query: %{id: 1}]}}
      })

    subquery_expr =
      Post
      |> EctoShorts.CommonFilters.convert_params_to_filter(%{id: 1}, [])
      |> select([p], p.id)

    expected = dynamic([q], field(q, ^:id) > any(subquery_expr))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports negated scalar any helper expression on a field" do
    binding = {:as, nil}
    subquery_expr = from(c in "comments", select: c.post_id)

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{id: %{not: %{any: %{>: subquery_expr}}}})

    expected = dynamic([q], not (field(q, ^:id) > any(subquery_expr)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports negated scalar any helper expression query-builder payload" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        id: %{not: %{any: %{>: [source: Post, query: %{id: 1}]}}}
      })

    subquery_expr =
      Post
      |> EctoShorts.CommonFilters.convert_params_to_filter(%{id: 1}, [])
      |> select([p], p.id)

    expected = dynamic([q], not (field(q, ^:id) > any(subquery_expr)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array nil equality" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{==: nil}})
    expected = dynamic([q], is_nil(field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array nil inequality" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{!=: nil}})
    expected = dynamic([q], not is_nil(field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array eq nil" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{eq: nil}})
    expected = dynamic([q], is_nil(field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array lower" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{lower: "foo"}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE lower(t) = ?
          )
          """,
          field(q, ^:tags),
          ^"foo"
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array upper" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{upper: "foo"}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE upper(t) = ?
          )
          """,
          field(q, ^:tags),
          ^"foo"
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not lower" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{lower: "foo"}}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          NOT EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE lower(t) = ?
          )
          """,
          field(q, ^:tags),
          ^"foo"
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not upper" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{upper: "foo"}}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          NOT EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE upper(t) = ?
          )
          """,
          field(q, ^:tags),
          ^"foo"
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array == lower" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{==: %{lower: "foo"}}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE lower(t) = ?
          )
          """,
          field(q, ^:tags),
          ^"foo"
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array == upper" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{==: %{upper: "foo"}}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE upper(t) = ?
          )
          """,
          field(q, ^:tags),
          ^"foo"
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array != lower" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{!=: %{lower: "foo"}}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          NOT EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE lower(t) = ?
          )
          """,
          field(q, ^:tags),
          ^"foo"
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array != upper" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{!=: %{upper: "foo"}}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          NOT EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE upper(t) = ?
          )
          """,
          field(q, ^:tags),
          ^"foo"
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array ilike" do
    binding = {:as, nil}
    patterns = ["%elixir%"]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{ilike: "elixir"}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE t ILIKE ANY (?)
          )
          """,
          field(q, ^:tags),
          ^patterns
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array like" do
    binding = {:as, nil}
    patterns = ["%elixir%"]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{like: "elixir"}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE t LIKE ANY (?)
          )
          """,
          field(q, ^:tags),
          ^patterns
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not ilike" do
    binding = {:as, nil}
    patterns = ["%ecto%"]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{ilike: "ecto"}}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          NOT EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE t ILIKE ANY (?)
          )
          """,
          field(q, ^:tags),
          ^patterns
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not like" do
    binding = {:as, nil}
    patterns = ["%ecto%"]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{like: "ecto"}}})

    expected =
      dynamic(
        [q],
        fragment(
          """
          NOT EXISTS (
            SELECT 1
            FROM unnest(?) AS t
            WHERE t LIKE ANY (?)
          )
          """,
          field(q, ^:tags),
          ^patterns
        )
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not == with list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{==: ["a", "b"]}}})
    expected = dynamic([q], field(q, ^:tags) != ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not != with list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{!=: ["a", "b"]}}})
    expected = dynamic([q], field(q, ^:tags) == ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not in list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{in: ["a", "b"]}}})

    expected =
      dynamic(
        [q],
        not fragment("? && ?", field(q, ^:tags), ^["a", "b"])
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not greater than" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{>: "a"}}})
    expected = dynamic([q], not fragment("? < ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not greater than or equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{>=: "a"}}})
    expected = dynamic([q], not fragment("? <= ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not less than" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{<: "a"}}})
    expected = dynamic([q], not fragment("? > ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not less than or equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{<=: "a"}}})
    expected = dynamic([q], not fragment("? >= ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not in all" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{in: %{all: ["a", "b"]}}}})

    expected = dynamic([q], not fragment("? @> ?", field(q, ^:tags), ^["a", "b"]))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array == list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{==: ["a", "b"]}})
    expected = dynamic([q], field(q, ^:tags) == ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array != list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{!=: ["a", "b"]}})
    expected = dynamic([q], field(q, ^:tags) != ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array in list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{in: ["a", "b"]}})
    expected = dynamic([q], fragment("? && ?", field(q, ^:tags), ^["a", "b"]))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array in all" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{in: %{all: ["a", "b"]}}})
    expected = dynamic([q], fragment("? @> ?", field(q, ^:tags), ^["a", "b"]))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array greater than" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{>: "a"}})
    expected = dynamic([q], fragment("? < ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array greater than or equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{>=: "a"}})
    expected = dynamic([q], fragment("? <= ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array less than" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{<: "a"}})
    expected = dynamic([q], fragment("? > ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array less than or equal" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{<=: "a"}})
    expected = dynamic([q], fragment("? >= ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array gt" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{gt: "a"}})
    expected = dynamic([q], fragment("? < ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array gte" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{gte: "a"}})
    expected = dynamic([q], fragment("? <= ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array lt" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{lt: "a"}})
    expected = dynamic([q], fragment("? > ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array lte" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{lte: "a"}})
    expected = dynamic([q], fragment("? >= ANY(?)", ^"a", field(q, ^:tags)))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array eq list" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{eq: ["a", "b"]}})
    expected = dynamic([q], field(q, ^:tags) == ^["a", "b"])

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array eq value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{eq: "a"}})
    expected = dynamic([q], ^"a" in field(q, ^:tags))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array not eq value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{not: %{eq: "a"}}})
    expected = dynamic([q], ^"a" not in field(q, ^:tags))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array == value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{==: "a"}})
    expected = dynamic([q], ^"a" in field(q, ^:tags))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array != value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{!=: "a"}})
    expected = dynamic([q], ^"a" not in field(q, ^:tags))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports array in value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{tags: %{in: "a"}})
    expected = dynamic([q], ^"a" in field(q, ^:tags))

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic uses scalar expressions without schema metadata" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic({"posts", nil}, binding, %{title: %{==: "hi"}})
    expected = dynamic([q], field(q, ^:title) == ^"hi")

    assert_dynamic(expected, actual)
  end
end
