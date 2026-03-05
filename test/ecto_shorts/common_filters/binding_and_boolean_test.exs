# credo:disable-for-this-file Credo.Check.Warning.BoolOperationOnSameValues
defmodule EctoShorts.CommonFilters.BindingAndBooleanTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias Ecto.Query
  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 field-level logical operators" do
    test "combines two conditions on the same field with and" do
      expected = from(p in Post, where: p.views > ^10 and p.views < ^20)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{and: [>: 10, <: 20]}}, [])

      assert_sql(expected, q2)
    end

    test "does nothing when the and list is empty" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{and: []}}, [])
      assert q2 === q
    end

    test "combines equality and inequality conditions with and" do
      expected =
        from(p in Post,
          where: p.published == ^true and p.published != ^false
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{and: [==: true, !=: false]}}, [])

      assert_sql(expected, q2)
    end

    test "combines two conditions on the same field with or" do
      expected =
        from(p in Post,
          where: p.views > ^10 or p.views < ^5
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{or: [>: 10, <: 5]}}, [])

      assert_sql(expected, q2)
    end

    test "combines two equality conditions with or" do
      expected =
        from(p in Post,
          where: p.published == ^true or p.published == ^false
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{or: [==: true, ==: false]}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 top-level logical operators" do
    test "joins two filter groups with or at the top level" do
      expected =
        from(p in Post,
          where:
            (p.published == ^true and p.views == ^20) or
              (p.published == ^false and p.views == ^10)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{or: [[published: true, views: 20], [published: false, views: 10]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "joins two filter groups with and at the top level" do
      expected =
        from(p in Post,
          where: p.published == ^true and p.views == ^20 and (p.title == ^"hello" and p.views == ^15)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{and: [[published: true, views: 20], [title: "hello", views: 15]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "nests a field-level or inside a top-level or" do
      expected =
        from(p in Post,
          where:
            p.published == ^true or p.published == ^false or
              (p.published == ^true or p.published == ^false)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{or: [[published: [or: [==: true, ==: false]]], [published: [or: [==: true, ==: false]]]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "wraps a single filter group in an or clause" do
      expected = from(p in Post, where: p.published == ^true and p.views == ^20)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{or: [[published: true, views: 20]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "joins two mixed filter groups with or" do
      expected =
        from(p in Post,
          where:
            (p.views > ^10 and p.published == ^true) or
              (p.views < ^5 and p.published == ^false)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{or: [[views: %{>: 10}, published: true], [views: %{<: 5}, published: false]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "does nothing when the top-level or list is empty" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{or: []}, [])
      assert q2 === q
    end

    test "does nothing when the top-level and list is empty" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{and: []}, [])
      assert q2 === q
    end

    test "combines a keyword field filter with a top-level or group" do
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
          [title: "test", or: [[published: true, views: 20], [published: false, views: 10]]],
          []
        )

      assert_sql(expected, q2)
    end

    test "logs a warning and skips the field when a boolean group references an unknown field" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          _q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{id: %{or: [[published: true, does_not_exist: "value"]]}},
              []
            )

          send(self(), :done)
        end)

      assert log =~
               "Adapter EctoShorts.Dynamics.Postgres returned nil for field :id with expression: {:does_not_exist, \"value\"}"

      assert_received :done
    end
  end

  describe "convert_params_to_filter/3 binding selectors" do
    test "filters on a named binding using :as" do
      expected = from p in Post, as: :post, where: p.published == ^true
      q = from p in Post, as: :post

      q2 = CommonFilters.convert_params_to_filter(q, %{bind: %{as: :post, published: true}}, [])

      assert_sql(expected, q2)
    end

    test "filters on multiple named bindings given as a list" do
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
          %{bind: [%{as: :post, published: true}, %{as: :author, first_name: "John"}]},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters on a positional binding using :at" do
      expected = from p in Post, where: p.published == ^true

      q2 = CommonFilters.convert_params_to_filter(Post, %{bind: %{at: 1, published: true}}, [])

      assert_sql(expected, q2)
    end

    test "filters on a positional binding given as a single-element list" do
      expected = from p in Post, where: p.published == ^true

      q2 = CommonFilters.convert_params_to_filter(Post, %{bind: [%{at: 1, published: true}]}, [])

      assert_sql(expected, q2)
    end

    test "logs a warning and returns the query unchanged for an invalid :as value" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{bind: 123}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :bind payload to be a map or list of maps, got: 123"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "logs a warning and returns the query unchanged when both :as and :at are missing" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{bind: %{published: true}}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :bind entry to be a map"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "logs a warning and returns the query unchanged when the position exceeds the maximum" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{bind: %{at: 1_000, published: true}},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Binding position 1000 exceeds the configured :max_binding_positions"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "logs a warning and returns the query unchanged when the position exceeds the actual binding count" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{bind: %{at: 3, published: true}},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Binding position 3 exceeds the number of bindings in the query (1)"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "logs a warning and returns the query unchanged when the position is less than 1" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{bind: %{at: 0, published: true}},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Binding position must be >= 1, got: 0"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "logs a warning and returns the query unchanged when the named binding does not exist" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{bind: %{as: :nonexistent, published: true}},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Named binding :nonexistent does not exist in the query"
      assert_received {:q2, q2}
      assert q2 === q
    end
  end

  describe "convert_params_to_filter/3 schema filter precedence" do
    test "applies a where filter from a map" do
      expected = from p in Post, where: p.published == ^true
      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{published: true}}, [])

      assert_sql(expected, q2)
    end

    test "applies a where filter from a keyword list" do
      expected = from p in Post, where: p.published == ^true
      q2 = CommonFilters.convert_params_to_filter(Post, %{where: [published: true]}, [])

      assert_sql(expected, q2)
    end

    test "applies multiple where conditions from the same map" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          where: p.views == ^10
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{published: true, views: 10}}, [])

      assert_sql(expected, q2)
    end

    test "applies an or_where filter from a map" do
      expected = from p in Post, or_where: p.published == ^false
      q2 = CommonFilters.convert_params_to_filter(Post, %{or_where: %{published: false}}, [])

      assert_sql(expected, q2)
    end

    test "applies an or_where filter from a keyword list" do
      expected = from p in Post, or_where: p.published == ^false
      q2 = CommonFilters.convert_params_to_filter(Post, %{or_where: [published: false]}, [])

      assert_sql(expected, q2)
    end

    test "processes where before or_where regardless of map key order" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          or_where: p.published == ^false
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{where: %{published: true}, or_where: %{published: false}},
          []
        )

      assert_sql(expected, q2)
    end

    test "processes multiple where conditions before the or_where condition" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          where: p.views == ^10,
          or_where: p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{where: [published: true, views: 10], or_where: %{views: %{<: 5}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "processes field filters before or_where filters" do
      expected =
        from(p in Post,
          where: p.views > ^10,
          or_where: p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [views: %{>: 10}, or_where: %{views: %{<: 5}}],
          []
        )

      assert_sql(expected, q2)
    end

    test "processes field filters before or_where even when or_where appears first in keyword list" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          or_where: p.views > ^10 or p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [or_where: %{views: %{or: [>: 10, <: 5]}}, published: true],
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 binding-targeted query operations" do
    test "applies a select on the named binding" do
      q = from p in Post, as: :post
      expected = from p in Post, as: :post, select: p.id

      q2 = CommonFilters.convert_params_to_filter(q, %{bind: %{as: :post, select: :id}}, [])

      assert_sql(expected, q2)
    end

    test "applies a select_merge on the named binding" do
      q = from p in Post, as: :post
      expected = from p in Post, as: :post, select_merge: %{custom_id: p.id}

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :post, select_merge: %{map: %{custom_id: :id}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "sorts by a field on the named binding" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          order_by: [asc: a.first_name]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :author, order_by: %{asc: :first_name}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "sorts by a field on a positional binding" do
      q = from(p in Post, join: a in assoc(p, :author))

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          order_by: [asc: a.first_name]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: 2, order_by: %{asc: :first_name}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "prepends an order_by on the named binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          order_by: [desc: :id]
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          order_by: [asc: a.first_name, desc: p.id]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :author, prepend_order_by: %{asc: :first_name}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "prepends an order_by on a positional binding" do
      q = from(p in Post, join: a in assoc(p, :author), order_by: [desc: :id])

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          order_by: [asc: a.first_name, desc: p.id]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: 2, prepend_order_by: %{asc: :first_name}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "groups by a field on the named binding" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: a.first_name
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :author, group_by: :first_name}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies a having clause on the named binding" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: a.first_name,
          having: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :author, group_by: :first_name, having: %{first_name: "John"}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies an or_having clause on the named binding" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: [a.first_name, a.age],
          having: a.first_name == ^"John",
          or_having: a.age > ^30
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{
            bind: %{
              as: :author,
              group_by: [:first_name, :age],
              having: %{first_name: "John"},
              or_having: %{age: %{>: 30}}
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "applies a having clause with boolean operators on the named binding" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: [a.first_name, a.age],
          having: a.first_name == ^"John" and a.age > ^30
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{
            bind: %{
              as: :author,
              group_by: [:first_name, :age],
              having: [and: [first_name: "John", age: %{>: 30}]]
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "applies a having clause with a dynamic expression on the named binding" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)
      dyn = dynamic([_p, a], a.first_name == ^"John")

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: a.first_name,
          having: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :author, group_by: :first_name, having: dyn}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies a having clause with a dynamic expression on a positional binding" do
      q = from(p in Post, join: a in assoc(p, :author))
      dyn = dynamic([_p, a], a.first_name == ^"John")

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          group_by: a.first_name,
          having: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: 2, group_by: :first_name, having: dyn}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies distinct on the named binding" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          distinct: a.first_name
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :author, distinct: :first_name}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies a window function on the named binding" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          windows: [author_window: [partition_by: a.first_name, order_by: [asc: a.age]]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{
            bind: %{
              as: :author,
              windows: [author_window: [partition_by: :first_name, order_by: [asc: :age]]]
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "applies a window function on a positional binding" do
      q = from(p in Post, join: a in assoc(p, :author))

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          windows: [author_window: [partition_by: a.first_name, order_by: [asc: a.age]]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{
            bind: %{
              at: 2,
              windows: [author_window: [partition_by: :first_name, order_by: [asc: :age]]]
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "applies an update on the named binding" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)
      dynamic_title = dynamic([author: a], a.first_name)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          update: [set: [title: a.first_name]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :author, update: [set: [title: dynamic_title]]}},
          []
        )

      assert_sql(expected, q2, :update_all)
    end

    test "applies an update on a positional binding" do
      q = from(p in Post, join: a in assoc(p, :author))
      dynamic_title = dynamic([_p, a], a.first_name)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          update: [set: [title: a.first_name]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: 2, update: [set: [title: dynamic_title]]}},
          []
        )

      assert_sql(expected, q2, :update_all)
    end

    test "applies with_ties on the named binding" do
      q = from(p in Post, as: :post, order_by: [desc: :views], limit: ^10)
      expected = Query.with_ties(q, [post: p], true)

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{with_ties: %{bind: %{as: :post, value: true}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies with_ties on a positional binding" do
      q = from(p in Post, order_by: [desc: :views], limit: ^10)
      expected = Query.with_ties(q, [p], true)

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{with_ties: %{bind: %{at: 1, value: true}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "filters on the joined association when using shorthand under a binding selector" do
      q = from(p in Post, as: :post)

      expected =
        from(p in Post,
          as: :post,
          join: a in assoc(p, :author),
          as: :author,
          where: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :post, author: [as: :author, first_name: "John"]}},
          []
        )

      assert_sql(expected, q2)
    end

    test "adds a join under a named binding selector" do
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
          %{bind: %{as: :post, join: [author: [as: :author]]}},
          []
        )

      assert_sql(expected, q2)
    end

    test "adds a join under a positional binding selector" do
      q =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comment
        )

      expected =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comment,
          join: a in assoc(c, :author),
          as: :author
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: 2, join: [author: [as: :author]]}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies sibling filters on the joined binding alongside a canonical join" do
      q = from(p in Post, as: :post)

      expected =
        from(p in Post,
          as: :post,
          join: a in assoc(p, :author),
          as: :author,
          where: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          [
            bind: %{as: :post, join: [author: [as: :author]]},
            bind: %{as: :author, first_name: "John"}
          ],
          []
        )

      assert_sql(expected, q2)
    end

    test "wraps the query in a subquery under a named binding selector" do
      q = from(p in Post, as: :post)
      expected_inner = from(p in Post, as: :post, where: p.id == ^2)
      expected = subquery(expected_inner)

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: :post, subquery: %{id: 2}}},
          []
        )

      assert_query(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 :first binding selector" do
    test "targets the from binding when :at is :first on a bare schema" do
      expected = from(p in Post, where: p.published == ^true)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{bind: %{at: :first, published: true}},
          []
        )

      assert_sql(expected, q2)
    end

    test "targets the from binding when :at is :first on a query with joins" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          where: p.published == ^true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: :first, published: true}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies order_by on the from binding when :at is :first" do
      expected = from(p in Post, order_by: [asc: p.title])

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{bind: %{at: :first, order_by: %{asc: :title}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies group_by on the from binding when :at is :first" do
      expected = from(p in Post, group_by: p.title)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{bind: %{at: :first, group_by: :title}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 :last binding selector" do
    test "targets the from binding when :at is :last on a bare schema" do
      expected = from(p in Post, where: p.published == ^true)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{bind: %{at: :last, published: true}},
          []
        )

      assert_sql(expected, q2)
    end

    test "targets the last join when :at is :last on a query with joins" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          where: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: :last, first_name: "John"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies order_by on the last join when :at is :last" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          order_by: [asc: a.first_name]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: :last, order_by: %{asc: :first_name}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "applies group_by on the last join when :at is :last" do
      q = from(p in Post, join: a in assoc(p, :author), as: :author)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: a.first_name
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: :last, group_by: :first_name}},
          []
        )

      assert_sql(expected, q2)
    end
  end
end
