defmodule EctoShorts.CommonFiltersTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 preload shapes" do
    test "matches Ecto.Query for a root preload atom" do
      expected = from p in Post, preload: :author

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{preload: :author},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a nested preload keyword list" do
      expected = from p in Post, preload: [comments: :author]

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{preload: [comments: :author]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named join-backed preload" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          preload: [author: a]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                preload: :author
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named nested join-backed preload tuple" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          preload: [author: {a, [posts: :comments]}]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                preload: [author: [posts: :comments]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional nested join-backed preload tuple" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          preload: [author: {a, [posts: :comments]}]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                preload: [author: [posts: :comments]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end
end
