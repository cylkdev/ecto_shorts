defmodule EctoShorts.CommonFilters.PreloadWithFilterTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.Comment

  import Ecto.Query

  describe "convert_params_to_filter/3 join + preload + filter combinations" do
    # V1 — join on :comments, preload it back, no filter on the joined binding
    test "V1: matches Ecto.Query for a join-backed preload with no filter on the binding" do
      source =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments
        )

      expected =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments,
          preload: [comments: c]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              comments: %{
                preload: :comments
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    # V2 — join on :comments, filter on the joined binding, preload it back (no nested preload)
    test "V2: matches Ecto.Query for a join-backed preload with a where filter on the same binding" do
      source =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments
        )

      expected =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments,
          where: c.published == ^true,
          preload: [comments: c]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              comments: %{
                where: %{published: true},
                preload: :comments
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    # V3 — two joins (:comments and :author off comment), filter on comments binding,
    #       nested join-backed preload: preload [comments: {c, [author: a]}]
    test "V3: matches Ecto.Query for two joins with filter on top-level binding and nested join-backed preload tuple" do
      source =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments,
          join: a in assoc(c, :author),
          as: :comment_author
        )

      expected =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments,
          join: a in assoc(c, :author),
          as: :comment_author,
          where: c.published == ^true,
          preload: [comments: {c, [author: a]}]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              comments: %{
                where: %{published: true},
                preload: [comments: [author: []]]
              },
              comment_author: %{
                preload: :author
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    # V4 — through association (:comments_authors) preloaded without join, filter on root
    test "V4: matches Ecto.Query for a through-association preload with a root-level where filter" do
      expected =
        from(p in Post,
          where: p.title == ^"hello",
          preload: [:comments_authors]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            where: %{title: "hello"},
            preload: :comments_authors
          },
          []
        )

      assert_query(expected, actual)
    end

    # V5 — explicit join, top-level preload list, filter on the named binding
    test "V5: matches Ecto.Query for a top-level preload list with a where filter on the named binding" do
      source =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments
        )

      expected =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments,
          where: c.published == ^true,
          preload: [comments: c]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            preload: [comments: :author],
            as: %{
              comments: %{
                where: %{published: true},
                preload: :comments
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    # V6 — join on :comments, filter on binding, nested preload tuple form [comments: [posts: :comments]]
    test "V6: matches Ecto.Query for a join-backed nested preload tuple with a where filter on the binding" do
      source =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments
        )

      expected =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments,
          where: c.published == ^true,
          preload: [comments: {c, [post: []]}]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              comments: %{
                where: %{published: true},
                preload: [comments: [post: []]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    # V7 — positional `at:` selector instead of `as:`, join on :comments, filter + preload
    test "V7: matches Ecto.Query for a positional at: binding with a where filter and preload" do
      source =
        from(p in Post,
          join: c in assoc(p, :comments)
        )

      expected =
        from(p in Post,
          join: c in assoc(p, :comments),
          where: c.published == ^true,
          preload: [comments: c]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                where: %{published: true},
                preload: [comments: []]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    # V8 — join on :comments, multiple field filters on binding + nested preload with two associations
    test "V8: matches Ecto.Query for a join with multiple where filters on binding and nested preload" do
      source =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments
        )

      expected =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments,
          where: c.published == ^true,
          where: c.replies > ^0,
          preload: [comments: {c, [:author, :post]}]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              comments: %{
                where: %{published: true, replies: {:>, 0}},
                preload: [comments: [:author, :post]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end
end
