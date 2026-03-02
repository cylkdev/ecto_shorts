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
    test "field-level - %{views: %{and: [>: 10, <: 20]}}" do
      expected = from(p in Post, where: p.views > ^10 and p.views < ^20)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{and: [>: 10, <: 20]}}, [])

      assert_sql(expected, q2)
    end

    test "field-level - %{views: %{and: []}} (no-op)" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{views: %{and: []}}, [])
      assert q2 === q
    end

    test "field-level - %{published: %{and: [==: true, !=: false]}}" do
      expected =
        from(p in Post,
          where: p.published == ^true and p.published != ^false
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{and: [==: true, !=: false]}}, [])

      assert_sql(expected, q2)
    end

    test "field-level - %{views: %{or: [>: 10, <: 5]}}" do
      expected =
        from(p in Post,
          where: p.views > ^10 or p.views < ^5
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{or: [>: 10, <: 5]}}, [])

      assert_sql(expected, q2)
    end

    test "field-level - %{published: %{or: [==: true, ==: false]}}" do
      expected =
        from(p in Post,
          where: p.published == ^true or p.published == ^false
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{or: [==: true, ==: false]}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 top-level logical operators" do
    test "top-level - %{or: [[published: true, views: 20], [published: false, views: 10]]}" do
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

    test "top-level - %{and: [[published: true, views: 20], [title: \"hello\", views: 15]]}" do
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

    test "top-level - %{or: [[published: %{or: [==: true, ==: false]}], ...]}" do
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

    test "top-level - %{or: [[published: true, views: 20]]} (single entry)" do
      expected = from(p in Post, where: p.published == ^true and p.views == ^20)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{or: [[published: true, views: 20]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "top-level - %{or: [[views: %{>: 10}, published: true], [views: %{<: 5}, published: false]]}" do
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

    test "top-level - %{or: []} (no-op)" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{or: []}, [])
      assert q2 === q
    end

    test "top-level - %{and: []} (no-op)" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{and: []}, [])
      assert q2 === q
    end

    test "top-level - [title: \"test\", or: [[published: true, views: 20], [published: false, views: 10]]]" do
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

    test "composite operator with invalid field logs warning and skips field" do
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
               "Expected a query field for schema {\"posts\", EctoShorts.Schema.Post}, got: :does_not_exist"

      assert_received :done
    end
  end

  describe "convert_params_to_filter/3 binding selectors" do
    test "binding - %{bind: %{as: :post, published: true}}" do
      expected = from p in Post, as: :post, where: p.published == ^true
      q = from p in Post, as: :post

      q2 = CommonFilters.convert_params_to_filter(q, %{bind: %{as: :post, published: true}}, [])

      assert_sql(expected, q2)
    end

    test "binding - multiple named bindings as list" do
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

    test "binding - %{bind: %{at: 1, published: true}}" do
      expected = from p in Post, where: p.published == ^true

      q2 = CommonFilters.convert_params_to_filter(Post, %{bind: %{at: 1, published: true}}, [])

      assert_sql(expected, q2)
    end

    test "binding - %{bind: [%{at: 1, published: true}]}" do
      expected = from p in Post, where: p.published == ^true

      q2 = CommonFilters.convert_params_to_filter(Post, %{bind: [%{at: 1, published: true}]}, [])

      assert_sql(expected, q2)
    end

    test "invalid binding params logs warning and leaves query unchanged" do
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

    test "binding entry missing :as and :at logs warning and leaves query unchanged" do
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

    test "positional binding exceeding max_binding_positions logs warning and leaves query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{bind: %{at: 4, published: true}},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Binding position 4 exceeds the configured :max_binding_positions"
      assert_received {:q2, q2}
      assert q2 === q
    end
  end

  describe "convert_params_to_filter/3 schema filter precedence" do
    test "precedence - %{where: %{published: true}}" do
      expected = from p in Post, where: p.published == ^true
      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{published: true}}, [])

      assert_sql(expected, q2)
    end

    test "precedence - %{where: [published: true]}" do
      expected = from p in Post, where: p.published == ^true
      q2 = CommonFilters.convert_params_to_filter(Post, %{where: [published: true]}, [])

      assert_sql(expected, q2)
    end

    test "precedence - %{where: %{published: true, views: 10}}" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          where: p.views == ^10
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{published: true, views: 10}}, [])

      assert_sql(expected, q2)
    end

    test "precedence - %{or_where: %{published: false}}" do
      expected = from p in Post, or_where: p.published == ^false
      q2 = CommonFilters.convert_params_to_filter(Post, %{or_where: %{published: false}}, [])

      assert_sql(expected, q2)
    end

    test "precedence - %{or_where: [published: false]}" do
      expected = from p in Post, or_where: p.published == ^false
      q2 = CommonFilters.convert_params_to_filter(Post, %{or_where: [published: false]}, [])

      assert_sql(expected, q2)
    end

    test "precedence - %{where: %{published: true}, or_where: %{published: false}}" do
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

    test "precedence - %{where: [published: true, views: 10], or_where: %{views: %{<: 5}}} (Rule 17)" do
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

    test "precedence - %{views: %{>: 10}, or_where: %{views: %{<: 5}}} (Rule 18)" do
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

    test "precedence - [or_where: %{views: %{or: [>: 10, <: 5]}}, published: true] (Rule 18: field before or_where)" do
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
    test "binding selector :select supports non-tuple select value" do
      q = from p in Post, as: :post
      expected = from p in Post, as: :post, select: p.id

      q2 = CommonFilters.convert_params_to_filter(q, %{bind: %{as: :post, select: :id}}, [])

      assert_sql(expected, q2)
    end

    test "binding selector :select_merge supports non-tuple select_merge value" do
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

    test "binding selector :order_by targets the selected binding" do
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

    test "positional binding selector :order_by targets the selected binding" do
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

    test "binding selector :prepend_order_by targets the selected binding" do
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

    test "positional binding selector :prepend_order_by targets the selected binding" do
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

    test "binding selector :group_by targets the selected binding" do
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

    test "binding selector :having targets the selected binding" do
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

    test "binding selector :or_having targets the selected binding" do
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

    test "binding selector :having supports boolean map payloads" do
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

    test "binding selector :having supports Ecto.Query.dynamic/2 payloads" do
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

    test "positional binding selector :having supports Ecto.Query.dynamic/2 payloads" do
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

    test "binding selector :distinct targets the selected binding" do
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

    test "binding selector :windows targets the selected binding" do
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

    test "positional binding selector :windows targets the selected binding" do
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

    test "binding selector :update targets the selected binding" do
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

    test "positional binding selector :update targets the selected binding" do
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

    test "binding selector :with_ties targets the selected binding via :as" do
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

    test "binding selector :with_ties targets the selected binding via :at" do
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

    test "association shorthand applies filters on the joined binding under selector scope" do
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

    test "supports canonical :join under named binding selector" do
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

    test "supports canonical :join under positional binding selector" do
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

    test "canonical :join keeps sibling bind filters on the joined binding" do
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

    test "supports :subquery under named binding selector" do
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
    test "binding - %{bind: %{at: :first, published: true}} on bare schema" do
      expected = from(p in Post, where: p.published == ^true)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{bind: %{at: :first, published: true}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding - %{bind: %{at: :first, published: true}} on query with join targets from binding" do
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

    test "binding - %{bind: %{at: :first, order_by: %{asc: :title}}}" do
      expected = from(p in Post, order_by: [asc: p.title])

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{bind: %{at: :first, order_by: %{asc: :title}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding - %{bind: %{at: :first, group_by: :title}}" do
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
    test "binding - %{bind: %{at: :last, published: true}} on bare schema targets from binding" do
      expected = from(p in Post, where: p.published == ^true)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{bind: %{at: :last, published: true}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding - %{bind: %{at: :last, first_name: \"John\"}} on query with join targets last join" do
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

    test "binding - %{bind: %{at: :last, order_by: %{asc: :first_name}}} on query with join" do
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

    test "binding - %{bind: %{at: :last, group_by: :first_name}} on query with join" do
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
