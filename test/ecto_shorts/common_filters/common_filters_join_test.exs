defmodule EctoShorts.CommonFilters.JoinTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 join shapes" do
    test "matches Ecto.Query for an association join payload" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [association: [source: :author, as: :author]]},
          []
        )

      assert_query(expected, actual)
    end

    # Three outer-key forms are supported for association joins: the explicit form
    # `[association: [source: :author, as: :author]]`, the type-selector form
    # `[type: :association, source: :author, ...]`, and this shorthand where the
    # outer key is the association name itself.
    test "matches Ecto.Query for an association shorthand join payload" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [author: [as: :author]]},
          []
        )

      assert_query(expected, actual)
    end

    # `type:` selects the source family (`:association`, `:schema`, `:table`, etc.).
    # `qualifier:` selects the join mode (`:left`, `:right`, `:inner`, etc.).
    # The two keys are independent; `type:` does not set the join mode.
    test "matches Ecto.Query for an explicit association join payload using the type source selector" do
      expected =
        from(p in Post,
          left_join: a in assoc(p, :author),
          as: :author,
          on: true
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [type: :association, source: :author, as: :author, qualifier: :left, on: true]},
          []
        )

      assert_sql(expected, actual)
    end

    test "matches Ecto.Query for a schema join payload" do
      expected =
        from(p in Post,
          join: u in EctoShorts.Schema.User,
          as: :user,
          on: p.author_id == ^1
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              schema: [
                source: EctoShorts.Schema.User,
                as: :user,
                on: %{author_id: 1}
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for an explicit schema join payload using the type source selector" do
      expected =
        from(p in Post,
          join: u in EctoShorts.Schema.User,
          as: :user,
          on: p.author_id == ^1
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [type: :schema, source: EctoShorts.Schema.User, as: :user, on: %{author_id: 1}]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a table join payload" do
      expected =
        from(p in Post,
          join: u in "users",
          as: :user,
          on: p.author_id == ^1
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              table: [
                source: "users",
                as: :user,
                on: %{author_id: 1}
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    # `hints:` accepts an atom. The atom is resolved to a SQL hint string by the
    # provider before being forwarded to Ecto.
    test "matches Ecto.Query for a table join payload with configured hints" do
      expected =
        from(p in Post,
          join: u in "users",
          as: :user,
          on: p.author_id == ^1,
          hints: ["USE INDEX(test_index)"]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              table: [
                source: "users",
                as: :user,
                on: %{author_id: 1},
                hints: :test_index
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a query join payload" do
      user_query = from(u in EctoShorts.Schema.User, where: u.age > ^18)

      expected =
        from(p in Post,
          join: u in ^user_query,
          as: :user,
          on: p.author_id == ^1
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              query: [
                source: user_query,
                as: :user,
                on: %{author_id: 1}
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a subquery join payload built from params" do
      user_query = from(u in EctoShorts.Schema.User, where: u.age > ^18)

      expected =
        from(p in Post,
          join: u in subquery(user_query),
          as: :user,
          on: p.author_id == ^1
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              subquery: [
                source: [from: EctoShorts.Schema.User, age: {:>, 18}],
                as: :user,
                on: %{author_id: 1}
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding association join payload" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          join: ap in assoc(a, :posts),
          as: :author_posts
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                join: [
                  association: [source: :posts, as: :author_posts]
                ]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    # Fragment joins require the `query_provider:` opt. The provider receives the
    # source name and values and must return `{:ok, %Ecto.Query{}}`. The three
    # tests below cover the nil, error, and invalid-return branches.
    test "matches Ecto.Query for a fragment join payload through the provider contract" do
      active_users =
        from(u in fragment("SELECT * FROM users WHERE age >= ?", ^18), select: u)

      expected =
        from(p in Post,
          join: u in ^active_users,
          as: :users,
          on: true
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: [name: :active_users, values: %{min_age: 18}],
                as: :users,
                on: true
              ]
            ]
          },
          query_provider: EctoShorts.TestQueryProvider
        )

      assert_query(expected, actual)
    end

    # A nil return from the provider leaves the query unchanged.
    test "keeps the query unchanged when the fragment provider returns nil" do
      expected = from(p in Post)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: [
                  name: :active_users,
                  values: %{min_age: 18}
                ],
                as: :users,
                on: true
              ]
            ]
          },
          query_provider: EctoShorts.TestNoOpQueryProvider
        )

      assert_query(expected, actual)
    end

    # An `{:error, reason}` return from the provider logs a warning and leaves the
    # query unchanged.
    test "keeps the query unchanged when the fragment provider returns an error" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{
                join: [
                  fragment: [
                    source: [
                      name: :error_fragment,
                      values: %{}
                    ],
                    as: :users,
                    on: true
                  ]
                ]
              },
              query_provider: EctoShorts.TestQueryProvider
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Join source callback returned error for key :error_fragment: :forced_error"
    end

    # A return value that is not `{:ok, query}`, `{:error, reason}`, or `nil` is
    # rejected with a log warning and leaves the query unchanged.
    test "keeps the query unchanged when the fragment provider returns a raw source" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{
                join: [
                  fragment: [
                    source: [name: :legacy_active_users, values: %{min_age: 18}],
                    as: :users,
                    on: true
                  ]
                ]
              },
              query_provider: EctoShorts.TestQueryProvider
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected join source callback to return {:ok, source} | {:error, reason} | nil"
    end
  end
end
