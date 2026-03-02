defmodule EctoShorts.CommonFilters.JoinTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.User

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 association shorthand" do
    test "joins on an association with a named binding and filters" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          where: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: [as: :author, first_name: "John"]},
          []
        )

      assert_sql(expected, q2)
    end

    test "joins on an association without a named binding" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: [first_name: "John"]},
          []
        )

      assert %Ecto.Query{} = q2
    end

    test "joins on an association with an explicit on condition" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          where: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: [as: :author, on: true, first_name: "John"]},
          []
        )

      assert_sql(expected, q2)
    end

    test "joins on an association with a left join qualifier" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: [as: :author, type: :left, first_name: "John"]},
          []
        )

      assert %Ecto.Query{} = q2
    end

    test "joins on an association using a map with a named binding" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          where: a.first_name == ^"John"
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: %{as: :author, first_name: "John"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "joins on an association using a map without a named binding" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: %{first_name: "John"}},
          []
        )

      assert %Ecto.Query{} = q2
    end

    test "joins on an association using a map with a left join qualifier" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: %{as: :author, type: :left, first_name: "John"}},
          []
        )

      assert %Ecto.Query{} = q2
    end

    test "logs a warning and skips the join when the association payload is invalid" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{author: 123}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected association params for :author to be a map or keyword list, got: 123"
      assert_received {:q2, q2}
      assert_sql(q, q2)
    end
  end

  describe "convert_params_to_filter/3 dynamic" do
    test "applies a dynamic expression as a where condition" do
      dyn = dynamic([p], p.views > ^10)
      expected = from(p in Post, where: p.views > ^10)

      q2 = CommonFilters.convert_params_to_filter(Post, %{dynamic: dyn}, [])

      assert_sql(expected, q2)
    end

    test "applies a dynamic expression inside an explicit where" do
      dyn = dynamic([p], p.published == ^true)
      expected = from(p in Post, where: p.published == ^true)

      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{dynamic: dyn}}, [])

      assert_sql(expected, q2)
    end

    test "applies a dynamic expression inside an or_where" do
      dyn = dynamic([p], p.views > ^100)
      expected = from(p in Post, or_where: p.views > ^100)

      q2 = CommonFilters.convert_params_to_filter(Post, %{or_where: %{dynamic: dyn}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 exists" do
    test "filters by subquery existence" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected = from(p in Post, where: exists(subquery_expr))

      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{exists: subquery_expr}}, [])

      assert_sql(expected, q2)
    end

    test "filters by negated subquery existence" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected = from(p in Post, where: not exists(subquery_expr))

      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{exists: %{not: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end

    test "uses the parent source when the exists payload omits :query" do
      subquery_expr =
        CommonFilters.convert_params_to_filter(
          User,
          [from: %{query: User, first_name: "John"}, select: true],
          []
        )

      expected = from(u in User, where: exists(subquery_expr))

      q2 =
        CommonFilters.convert_params_to_filter(
          User,
          %{where: %{exists: %{from: %{first_name: "John"}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "builds an exists subquery from a query-builder payload" do
      subquery_expr =
        CommonFilters.convert_params_to_filter(
          Post,
          [from: %{query: Post, id: 1}, select: true],
          []
        )

      expected = from(p in Post, where: exists(subquery_expr))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{where: %{exists: %{from: %{query: Post, id: 1}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "builds a negated exists subquery from a query-builder payload" do
      subquery_expr =
        CommonFilters.convert_params_to_filter(
          Post,
          [from: %{query: Post, id: 1}, select: true],
          []
        )

      expected = from(p in Post, where: not exists(subquery_expr))

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{where: %{exists: %{not: %{from: %{query: Post, id: 1}}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 canonical :join" do
    test "adds an association join using the canonical :join key" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{join: [author: [as: :author]]}, [])

      assert_sql(expected, q2)
    end

    test "targets the root binding when no selector is given for a canonical join" do
      q = from(p in Post, as: :post)

      expected =
        from(p in Post,
          as: :post,
          join: a in assoc(p, :author),
          as: :author
        )

      q2 = CommonFilters.convert_params_to_filter(q, [join: [author: [as: :author]]], [])

      assert_sql(expected, q2)
    end

    test "joins on a schema module using the canonical :join key" do
      expected =
        from(p in Post,
          join: u in User,
          as: :user_join,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [schema: [source: User, as: :user_join, on: true]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "joins on a table name using the canonical :join key" do
      expected =
        from(p in Post,
          join: u in "users",
          as: :users_table,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [table: [source: "users", as: :users_table, on: true]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "joins on an Ecto.Query using the canonical :join key" do
      user_query = from(u in User, where: u.age >= ^18)

      expected =
        from(p in Post,
          join: u in ^user_query,
          as: :adult_users,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [query: [source: user_query, as: :adult_users, on: true]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "joins on a subquery using the canonical :join key" do
      user_query = from(u in User, where: u.age >= ^18)

      expected =
        from(p in Post,
          join: u in subquery(user_query),
          as: :adult_users_subquery,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [subquery: [source: user_query, as: :adult_users_subquery, on: true]]},
          []
        )

      assert_sql(expected, q2)
    end

    test "builds a subquery join from filter params with a :from key" do
      expected_subquery = from(u in User, where: u.age >= ^18)

      expected =
        from(p in Post,
          join: u in subquery(expected_subquery),
          as: :adult_users_subquery,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              subquery: [
                source: %{from: %{query: User, age: %{>=: 18}}},
                as: :adult_users_subquery,
                on: true
              ]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "builds a subquery join using the parent source when :query is omitted" do
      expected_subquery = from(p in Post, where: p.published == ^true)

      expected =
        from(p in Post,
          join: s in subquery(expected_subquery),
          as: :published_posts_subquery,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              subquery: [
                source: %{from: %{published: true}},
                as: :published_posts_subquery,
                on: true
              ]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "adds multiple joins from a list and preserves their order" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          join: u in "users",
          as: :users_table,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              author: [as: :author],
              table: [source: "users", as: :users_table, on: true]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "logs a warning and skips the join when the :on payload is invalid" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{join: [association: [source: :author, as: :author, on: [1, 2]]]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :on to be a keyword list, map, or true, got: [1, 2]"
      assert_received {:q2, q2}
      assert_sql(q, q2)
    end

    test "applies valid join entries and skips invalid ones" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              Post,
              %{join: [[author: [as: :author]], 123]},
              []
            )

          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected :join params to be a map or keyword list, got: 123"
      assert_received {:q2, q2}
      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 fragment joins" do
    test "resolves a fragment source through the configured fragment provider" do
      expected_source_query =
        from(u in fragment("SELECT * FROM users WHERE age >= ?", ^21), select: u)

      expected =
        from(p in Post,
          join: a in ^expected_source_query,
          as: :active_users,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: [name: :active_users, values: [min_age: 21]],
                as: :active_users,
                on: true
              ]
            ]
          },
          fragment_provider: EctoShorts.TestFragmentProvider
        )

      assert_sql(expected, q2)
    end

    test "resolves a fragment source through a runtime fragment provider option" do
      expected_source_query =
        from(u in fragment("SELECT * FROM users WHERE age >= ?", ^21), select: u)

      expected =
        from(p in Post,
          join: a in ^expected_source_query,
          as: :active_users,
          on: true
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: %{name: :active_users, values: [min_age: 21]},
                as: :active_users,
                on: true
              ]
            ]
          },
          fragment_provider: EctoShorts.TestFragmentProvider
        )

      assert_sql(expected, q2)
    end

    test "resolves join hints through a runtime fragment provider option" do
      expected_source_query =
        from(u in fragment("SELECT * FROM users WHERE age >= ?", ^21), select: u)

      expected =
        from(p in Post,
          join: a in ^expected_source_query,
          as: :active_users,
          on: true,
          hints: ["USE INDEX(test_index)"]
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: [
                  name: :active_users,
                  values: [min_age: 21]
                ],
                hints: :test_index,
                as: :active_users,
                on: true
              ]
            ]
          },
          fragment_provider: EctoShorts.TestFragmentProvider
        )

      assert_sql(expected, q2)
    end

    test "logs a warning and skips the join when the source key is unknown" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{
                join: [
                  fragment: [
                    source: %{name: :unknown_key, values: [min_age: 18]},
                    as: :x,
                    on: true
                  ]
                ]
              },
              fragment_provider: EctoShorts.TestFragmentProvider
            )

          send(self(), {:q2, q2})
        end)

      assert log =~
               "Join source callback returned error for key :unknown_key: :unsupported_fragment_key"

      assert_received {:q2, q2}
      assert_sql(q, q2)
    end

    test "logs a warning and skips the join when the source payload is malformed" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 =
            CommonFilters.convert_params_to_filter(
              q,
              %{
                join: [
                  fragment: [
                    source: [
                      name: :active_users,
                      values: [a: [x: 1], b: [y: 2]]
                    ],
                    as: :x,
                    on: true
                  ]
                ]
              },
              fragment_provider: EctoShorts.TestFragmentProvider
            )

          send(self(), {:q2, q2})
        end)

      assert log =~
               "Join source callback returned error for key :active_users: {:missing_or_invalid, :min_age}"

      assert_received {:q2, q2}
      assert_sql(q, q2)
    end
  end
end
