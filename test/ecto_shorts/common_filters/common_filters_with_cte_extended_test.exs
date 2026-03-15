defmodule EctoShorts.CommonFilters.WithCteExtendedTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 with_cte extended paths" do
    test "matches Ecto.Query for with_cte using a string CTE name" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = with_cte(Post, "published_posts", as: ^cte_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [%{"published_posts" => [as: cte_query]}]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for with_cte with both materialized and operation" do
      cte_query =
        Post
        |> where([p], p.published == ^false)
        |> update([p], set: [published: true])
        |> select([p], p)

      expected =
        with_cte(Post, "published_posts",
          as: ^cte_query,
          materialized: true,
          operation: :update_all
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: %{published_posts: %{as: cte_query, materialized: true, operation: :update_all}}},
          []
        )

      assert_sql(expected, actual)
    end

    test "keeps the query unchanged when a with_cte entry is not a pair" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_cte: [:not_a_pair]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :with_cte params to be a map or keyword list"
    end

    test "keeps the query unchanged when the with_cte name is invalid" do
      expected = from(p in Post)
      cte_query = from(p in Post, where: p.published == ^true)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [%{123 => [as: cte_query]}]},
          []
        )

      assert_query(expected, actual)
    end

    test "keeps the query unchanged when a with_cte entry is missing :as key" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_cte: [published_posts: [materialized: false]]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :with_cte params for \"published_posts\" to include an :as key"
    end

    test "keeps the query unchanged when with_cte :materialized is invalid" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_cte: [published_posts: [as: cte_query, materialized: "yes"]]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :materialized for \"published_posts\" to be a boolean"
    end
  end
end
