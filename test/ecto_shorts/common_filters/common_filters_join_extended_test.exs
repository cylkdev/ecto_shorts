defmodule EctoShorts.CommonFilters.JoinExtendedTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 join extended paths" do
    test "keeps the query unchanged and logs when join type key is unrecognised" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{join: [bad_key: [source: Post]]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected join type to be one of"
    end

    test "keeps the query unchanged and logs when join options has no :source key" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{join: [association: [as: :author]]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected join options to have a :source key"
    end

    test "keeps the query unchanged and logs when a join entry is not a map or keyword list" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{join: ["not_a_pair"]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :join params to be a map or keyword list"
    end

    test "raises when fragment join source name is missing" do
      assert_raise ArgumentError, ~r/Join source name is required/, fn ->
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [fragment: [source: [values: %{}], as: :users, on: true]]},
          query_provider: EctoShorts.TestQueryProvider
        )
      end
    end

    test "raises when fragment join source values are missing" do
      assert_raise ArgumentError, ~r/Join source values are required/, fn ->
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [fragment: [source: [name: :active_users], as: :users, on: true]]},
          query_provider: EctoShorts.TestQueryProvider
        )
      end
    end

    test "raises when schema join target is not a valid atom or {table, schema} tuple" do
      assert_raise ArgumentError, ~r/Expected target schema/, fn ->
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [schema: [source: 123, as: :user]]},
          []
        )
      end
    end

    test "matches Ecto.Query for a schema join using a {table, schema} tuple" do
      expected =
        from(p in Post,
          join: u in {"users", EctoShorts.Schema.User},
          as: :user,
          on: true
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              schema: [
                source: {"users", EctoShorts.Schema.User},
                as: :user,
                on: true
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    test "raises when query join source is not an Ecto.Query struct" do
      assert_raise ArgumentError, ~r/Expected source query to be a struct/, fn ->
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [query: [source: "not_a_query", as: :user]]},
          []
        )
      end
    end

    test "keeps the query unchanged when :on list is not a keyword list" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{
                join: [
                  association: [source: :author, as: :author, on: [:bad]]
                ]
              },
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :on to be a keyword list, map, or true"
    end

    test "keeps the query unchanged when :on is an invalid term" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{
                join: [
                  association: [source: :author, as: :author, on: 999]
                ]
              },
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :on to be a keyword list, map, or true"
    end

    test "matches Ecto.Query for a subquery join from a prebuilt Ecto.Query" do
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

    test "processes a nested non-keyword list of join entries" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          join: c in assoc(p, :comments),
          as: :comments
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              [association: [source: :author, as: :author]],
              [association: [source: :comments, as: :comments]]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end
  end
end
