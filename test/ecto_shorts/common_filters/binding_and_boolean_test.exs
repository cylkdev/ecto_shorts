# credo:disable-for-this-file Credo.Check.Warning.BoolOperationOnSameValues
defmodule EctoShorts.CommonFilters.BindingAndBooleanTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias Ecto.Query
  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 binding selectors" do
    test "supports positional binding selector via :at" do
      expected = from p in Post, where: p.published == ^true
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{bind: %{at: %{1 => %{published: true}}}}, [])

      assert_sql(expected, q2)
    end

    test "invalid :at binding selector key logs warning and skips bind operation" do
      q = Post

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{
                bind: %{
                  at: %{
                    "1" => %{published: false},
                    1 => %{published: true}
                  }
                }
              },
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~
               "Expected binding selector to be one of {:as, atom()} or {:at, integer()}, got: {:at, \"1\"}"

      assert_received {:q2, q2}
      assert_sql(q, q2)
    end

    test "supports named binding selector via :as" do
      expected = from p in Post, as: :post, where: p.published == ^true
      q = from p in Post, as: :post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{bind: %{as: %{post: %{published: true}}}}, [])

      assert_sql(expected, q2)
    end

    test "supports filtering on both root field and association field via :as" do
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
          %{
            bind: %{
              as: [
                post: %{published: true},
                author: %{first_name: "John"}
              ]
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "invalid binding params logs warning and leaves query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{bind: 123}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :bind payload to be a keyword list or map, got: 123"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "supports binding selector with explicit operator tuple" do
      q = from p in Post, as: :post

      expected =
        from(p in Post,
          as: :post,
          where: p.published == ^true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: %{post: %{published: %{==: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :where supports list of params for a field" do
      q = from p in Post, as: :post

      expected = from p in Post, as: :post, where: p.published in ^[true, false]

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: %{post: %{published: [true, false]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector with non-map params logs error and returns query unchanged" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{bind: %{as: %{post: 123}}}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected params to be a map or keyword list, got: 123"
      assert_received {:q2, q2}
      assert q2 === q
    end

    test "supports binding target params as a keyword list" do
      expected = from p in Post, where: p.published == ^true
      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{bind: %{at: %{1 => [published: true]}}}, [])

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
          [bind: [as: [post: [author: [as: :author, first_name: "John"]]]]],
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
          %{bind: %{as: %{post: %{join: [author: [as: :author]]}}}},
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
          %{bind: %{at: %{2 => %{join: [author: [as: :author]]}}}},
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
            bind: [
              as: [
                post: [
                  join: [author: [as: :author]],
                  bind: [as: [author: [first_name: "John"]]]
                ]
              ]
            ]
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
          %{bind: %{as: %{post: %{subquery: %{id: 2}}}}},
          []
        )

      assert_query(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 binding-targeted query operations" do
    test "binding selector :select supports non-tuple select value" do
      q = from p in Post, as: :post

      expected = from p in Post, as: :post, select: p.id

      q2 = CommonFilters.convert_params_to_filter(q, %{bind: %{as: %{post: %{select: :id}}}}, [])

      assert_sql(expected, q2)
    end

    test "binding selector :select_merge supports non-tuple select_merge value" do
      q = from p in Post, as: :post

      expected = from p in Post, as: :post, select_merge: %{custom_id: p.id}

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: %{post: %{select_merge: %{map: %{custom_id: :id}}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :order_by targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          order_by: [asc: a.first_name]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: %{author: %{order_by: %{asc: :first_name}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "positional binding selector :order_by targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          order_by: [asc: a.first_name]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: %{2 => %{order_by: %{asc: :first_name}}}}},
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
          %{bind: %{as: %{author: %{prepend_order_by: %{asc: :first_name}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "positional binding selector :prepend_order_by targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          order_by: [desc: :id]
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          order_by: [asc: a.first_name, desc: p.id]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: %{2 => %{prepend_order_by: %{asc: :first_name}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :group_by targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: a.first_name
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: %{author: %{group_by: :first_name}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :having targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

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
          %{bind: %{as: %{author: %{group_by: :first_name, having: %{first_name: "John"}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :or_having targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

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
              as: %{
                author: %{
                  group_by: [:first_name, :age],
                  having: %{first_name: "John"},
                  or_having: %{age: %{>: 30}}
                }
              }
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :having supports boolean map payloads" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

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
              as: %{
                author: %{
                  group_by: [:first_name, :age],
                  having: [and: [first_name: "John", age: %{>: 30}]]
                }
              }
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :having supports Ecto.Query.dynamic/2 payloads" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

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
          %{bind: %{as: %{author: %{group_by: :first_name, having: dyn}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "positional binding selector :having supports Ecto.Query.dynamic/2 payloads" do
      q =
        from(p in Post,
          join: a in assoc(p, :author)
        )

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
          %{bind: %{at: %{2 => %{group_by: :first_name, having: dyn}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :distinct targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          distinct: a.first_name
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: %{author: %{distinct: :first_name}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :windows targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

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
              as: %{
                author: %{
                  windows: [author_window: [partition_by: :first_name, order_by: [asc: :age]]]
                }
              }
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "positional binding selector :windows targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author)
        )

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
              at: %{
                2 => %{
                  windows: [author_window: [partition_by: :first_name, order_by: [asc: :age]]]
                }
              }
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "binding selector :update targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      dynamic_title = dynamic([author: a], a.first_name)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          update: [set: [first_name: a.first_name]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{as: %{author: %{update: [set: [first_name: dynamic_title]]}}}},
          []
        )

      assert_sql(expected, q2, :update_all)
    end

    test "positional binding selector :update targets the selected binding" do
      q =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      dynamic_title = dynamic([_p, a], a.first_name)

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          update: [set: [first_name: a.first_name]]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{bind: %{at: %{2 => %{update: [set: [first_name: dynamic_title]]}}}},
          []
        )

      assert_sql(expected, q2, :update_all)
    end

    test "binding selector :with_ties targets the selected binding" do
      q =
        from(p in Post,
          as: :post,
          order_by: [desc: :views],
          limit: ^10
        )

      expected = Query.with_ties(q, [post: p], true)

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{with_ties: %{bind: %{as: %{post: true}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "positional binding selector :with_ties targets the selected binding" do
      q =
        from(p in Post,
          order_by: [desc: :views],
          limit: ^10
        )

      expected = Query.with_ties(q, [p], true)

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{with_ties: %{bind: %{at: %{1 => true}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 boolean operators" do
    test "boolean operator group is correctly grouped with other where conditions" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          where: p.views > ^10 or p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            where: [
              published: true,
              views: %{or: [>: 10, <: 5]}
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports boolean operator group under :or_where" do
      expected =
        from(p in Post,
          where: p.published == ^true,
          or_where: p.views > ^10 or p.views < ^5
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          [
            or_where: %{views: %{or: [>: 10, <: 5]}},
            published: true
          ],
          []
        )

      assert_sql(expected, q2)
    end

    test "supports composite :or operator with multiple field maps" do
      expected =
        from(p in Post,
          where:
            (p.published == ^true and p.views == ^20) or
              (p.published == ^false and p.views == ^10)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            or: [
              [published: true, views: 20],
              [published: false, views: 10]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports composite :or where inner maps also use field-level :or" do
      expected =
        from(p in Post,
          where:
            p.published == ^true or p.published == ^false or
              (p.published == ^true or p.published == ^false)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            or: [
              [published: [or: [==: true, ==: false]]],
              [published: [or: [==: true, ==: false]]]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports composite :and operator with multiple field maps" do
      expected =
        from(p in Post,
          where: p.published == ^true and p.views == ^20 and (p.title == ^"hello" and p.views == ^15)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            and: [
              [published: true, views: 20],
              [title: "hello", views: 15]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator with single map in list" do
      expected =
        from(p in Post,
          where: p.published == ^true and p.views == ^20
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{or: [[published: true, views: 20]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator is correctly grouped with other where conditions" do
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
          [
            title: "test",
            or: [[published: true, views: 20], [published: false, views: 10]]
          ],
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator supports keyword list syntax" do
      expected =
        from(p in Post,
          where:
            (p.published == ^true and p.views == ^20) or
              (p.published == ^false and p.views == ^10)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            or: [
              [published: true, views: 20],
              [published: false, views: 10]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator with operators in field values" do
      expected =
        from(p in Post,
          where:
            (p.views > ^10 and p.published == ^true) or
              (p.views < ^5 and p.published == ^false)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            or: [
              [views: %{>: 10}, published: true],
              [views: %{<: 5}, published: false]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator under :or_where" do
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
            or_where: %{
              or: [
                [published: true, views: 20],
                [published: false, views: 10]
              ]
            }
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "composite operator with empty list is a no-op" do
      q = from(p in Post)
      q2 = CommonFilters.convert_params_to_filter(q, %{or: []}, [])
      assert q2 === q
    end

    test "composite operator with invalid field logs warning and skips field" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          _q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{
                id: %{
                  or: [
                    [published: true, does_not_exist: "value"]
                  ]
                }
              },
              []
            )

          send(self(), :done)
        end)

      assert log =~
               "Expected a query field for schema {\"posts\", EctoShorts.Schema.Post}, got: :does_not_exist"

      assert_received :done
    end
  end
end
