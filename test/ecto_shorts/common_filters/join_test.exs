defmodule EctoShorts.CommonFilters.JoinTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.User

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 association shorthand" do
    test "association - %{author: [as: :author, first_name: \"John\"]}" do
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

    test "association - %{author: [first_name: \"John\"]} (no :as)" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: [first_name: "John"]},
          []
        )

      assert %Ecto.Query{} = q2
    end

    test "association - %{author: [as: :author, on: true, first_name: \"John\"]}" do
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

    test "association - %{author: [as: :author, type: :left, first_name: \"John\"]}" do
      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{author: [as: :author, type: :left, first_name: "John"]},
          []
        )

      assert %Ecto.Query{} = q2
    end

    test "invalid association filter payload logs warning and skips entry" do
      q = from(p in Post)

      log =
        capture_log(fn ->
          q2 = CommonFilters.convert_params_to_filter(q, %{author: 123}, [])
          send(self(), {:q2, q2})
        end)

      assert log =~ "Expected association params for :author to be a keyword list, got: 123"
      assert_received {:q2, q2}
      assert_sql(q, q2)
    end
  end

  describe "convert_params_to_filter/3 dynamic" do
    test "dynamic - %{dynamic: dynamic([p], p.views > ^10)}" do
      dyn = dynamic([p], p.views > ^10)
      expected = from(p in Post, where: p.views > ^10)

      q2 = CommonFilters.convert_params_to_filter(Post, %{dynamic: dyn}, [])

      assert_sql(expected, q2)
    end

    test "dynamic - %{where: %{dynamic: dynamic([p], p.published === ^true)}}" do
      dyn = dynamic([p], p.published == ^true)
      expected = from(p in Post, where: p.published == ^true)

      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{dynamic: dyn}}, [])

      assert_sql(expected, q2)
    end

    test "dynamic - %{or_where: %{dynamic: dynamic([p], p.views > ^100)}}" do
      dyn = dynamic([p], p.views > ^100)
      expected = from(p in Post, or_where: p.views > ^100)

      q2 = CommonFilters.convert_params_to_filter(Post, %{or_where: %{dynamic: dyn}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 exists" do
    test "exists - %{where: %{exists: subquery_expr}}" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected = from(p in Post, where: exists(subquery_expr))

      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{exists: subquery_expr}}, [])

      assert_sql(expected, q2)
    end

    test "exists - %{where: %{exists: %{not: subquery_expr}}}" do
      subquery_expr = from(c in "comments", select: c.post_id)

      expected = from(p in Post, where: not exists(subquery_expr))

      q2 = CommonFilters.convert_params_to_filter(Post, %{where: %{exists: %{not: subquery_expr}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 canonical :join" do
    test "supports canonical :join association entry" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{join: [author: [as: :author]]}, [])

      assert_sql(expected, q2)
    end

    test "canonical :join defaults to root from binding when selector is omitted" do
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

    test "supports canonical :join schema source entry" do
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

    test "supports canonical :join table source entry" do
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

    test "supports canonical :join query source entry" do
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

    test "supports canonical :join subquery source entry" do
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

    test "supports canonical :join subquery source params composed into a query" do
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
                source: [
                  source: User,
                  query: [age: [>=: 18]]
                ],
                as: :adult_users_subquery,
                on: true
              ]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join subquery source params without :from using existing source" do
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
                source: [query: [published: true]],
                as: :published_posts_subquery,
                on: true
              ]
            ]
          },
          []
        )

      assert_sql(expected, q2)
    end

    test "supports canonical :join list of entries and preserves order" do
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

    test "invalid :join :on payload logs warning and skips join entry" do
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

    test "mixed valid and invalid canonical :join entries applies valid entries" do
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
    test "supports join source key dispatch through canonical :join source entry" do
      previous = Application.get_env(:ecto_shorts, :fragment_provider)

      on_exit(fn ->
        Application.put_env(:ecto_shorts, :fragment_provider, previous)
      end)

      Application.put_env(
        :ecto_shorts,
        :fragment_provider,
        EctoShorts.TestFragmentProvider
      )

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
          []
        )

      assert_sql(expected, q2)
    end

    test "supports join source key dispatch through runtime :fragment_provider option" do
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

    test "supports join hint key resolution through runtime :fragment_provider option" do
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

    test "unknown join source key logs warning and skips entry" do
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

    test "malformed join source payload logs warning and skips entry" do
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
