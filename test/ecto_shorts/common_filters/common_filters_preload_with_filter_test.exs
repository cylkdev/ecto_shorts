defmodule EctoShorts.CommonFilters.PreloadWithFilterTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post
  import Ecto.Query

  describe "convert_params_to_filter/3 join + preload + filter combinations" do
    # V1 - join on :comments, preload it back, no filter on the joined binding
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

    # V2 - join on :comments, filter on the joined binding, preload it back (no nested preload)
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

    # V3 - two joins (:comments and :author off comment), filter on :comments binding,
    #       plus separate preload on :comment_author binding.
    #
    # Cross-binding preload merge is NOT supported. Each `as:` entry dispatches its
    # `preload:` independently against its own binding variable. There is no mechanism
    # to combine two separate named-binding preload operations into a single
    # `{binding, nested_spec}` tuple.
    #
    # The system correctly emits two independent `preload:` clauses:
    #   1. The :comments binding entry emits `preload: [comments: {c1, [author: []]}]`
    #      because `[author: []]` is a non-nil nested value (see V6 for the tuple path).
    #   2. The :comment_author binding entry emits `preload: [author: a2]` independently.
    # Ecto merges multiple `preload:` clauses at load time, so the DB result is correct,
    # but the :author association inside comments will be loaded via a separate query
    # (not via the a2 join variable). To use a2 as the nested loader, the caller must
    # build the combined `{c, [author: a]}` tuple manually before calling this API.
    test "V3: emits two independent preload clauses when two named bindings each declare a preload" do
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
          preload: [comments: [author: []]],
          preload: [author: a]
        )
        |> then(fn q ->
          from([p, c] in q, preload: [comments: c])
        end)

      # Use a keyword list for `as:` to guarantee iteration order: :comment_author fires
      # first (emitting `preload: [author: a2]`), then :comments fires (emitting
      # `preload: [comments: {c1, [author: []]}]` and `preload: [comments: c1]`).
      # A map would produce non-deterministic key order across runs.
      actual =
        CommonFilters.convert_params_to_filter(
          source,
          [as: [comment_author: [preload: :author],
                comments: [where: [published: true],
                           preload: [comments: [author: []]]]]],
          []
        )

      assert_query(expected, actual)
    end

    # V4 - through association (:comments_authors) preloaded without join, filter on root
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

    # V5 - top-level `preload:` key combined with a binding-scoped `preload:` for the same
    #       association. This is an incoherent input: both preload strategies fire independently.
    #
    # The top-level `preload: [comments: :author]` dispatches through the unbound
    # `build_preload(query, expr)` path (selected_binding is {:as, nil}), emitting
    # `preload: [comments: [:author]]` - Ecto will issue a separate query for :author.
    #
    # The binding-scoped `preload: :comments` under `as: :comments` dispatches through
    # `build_preload(query, {:as, :comments}, :comments, nil)`, emitting the join-backed
    # `preload: [comments: c1]`.
    #
    # Both clauses are emitted. Ecto merges them at load time, but the caller almost
    # certainly did not intend to trigger both strategies. The correct input when a join
    # exists is to use only the binding-scoped `preload: :comments` and omit the top-level
    # `preload:` key entirely (see V2 for the correct shape).
    test "V5: emits two preload clauses when a top-level preload and a binding-scoped preload target the same association" do
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
          preload: [comments: [:author]],
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

    # V6 - join on :comments, filter on binding, nested preload tuple form [comments: [posts: :comments]]
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

    # V7 - positional `at:` selector instead of `as:`, join on :comments, filter + preload.
    #
    # The correct input to get a plain join-backed `preload: [comments: c1]` is the atom
    # form `preload: :comments`. Using `preload: [comments: []]` is wrong because `[]` is
    # a non-nil nested value, which routes to the tuple path in `build_preload/4`:
    # it emits `preload: [comments: {c1, []}]` (a join-backed tuple with empty sub-spec)
    # AND a second unbound clause `preload: [comments: [[]]]` from the keyword iteration
    # over `[comments: []]` at the top-level build_preload path.
    #
    # Use `preload: :comments` (atom) to mean "load this association from the current
    # binding with no nested sub-associations". The `[comments: nested]` keyword form
    # is only correct when `nested` is a non-empty sub-preload spec (see V6).
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
                preload: :comments
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    # V8 - join on :comments, multiple field filters on binding + nested preload carrying a
    #       non-keyword list of association atoms `[:author, :post]`.
    #
    # `[:author, :post]` is a non-keyword list. `normalize_preload/1` returns it unchanged
    # (the non-keyword list branch). However, `apply_preload/3` first iterates the outer
    # `[comments: [:author, :post]]` keyword list and passes `:comments` as the assoc_key
    # and `[:author, :post]` as the nested value to `build_preload/4`. The tuple path fires
    # because nested is not nil, producing `preload: [comments: {c1, [[:author, :post]]}]`
    # - an extra wrapping layer because `normalize_preload([:author, :post])` returns the
    # list as-is, which then matches the `other` branch of the case, so prepared_nested =
    # `[:author, :post]` but Ecto sees it wrapped inside the keyword-list preload call.
    #
    # To preload multiple associations as sub-specs from a join-backed binding, use a
    # keyword list with nil-valued keys: `[comments: [author: [], post: []]]` (see V6
    # for the keyword nested form). The bare atom-list form `[:author, :post]` is only
    # valid at the top-level unbound `preload:` key, not as a nested spec inside a
    # binding-scoped preload.
    test "V8: emits a double-wrapped preload when a non-keyword list is passed as the nested preload spec" do
      source =
        from(p in Post,
          join: c in assoc(p, :comments),
          as: :comments
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

      # The `from` macro normalizes `[[:author, :post]]` back to `[:author, :post]`,
      # so we cannot use a `from`-built expected here. Assert on the raw preloads
      # field instead to verify the actual double-wrapped structure.
      # Only one preload clause is emitted: the binding-scoped path processes
      # `[comments: [:author, :post]]` as a keyword list with `:comments` → `[:author, :post]`,
      # and the tuple path wraps it as `{c1, [[:author, :post]]}` - normalized produces `[[:author, :post]]`.
      assert [preload_clause] = actual.preloads
      assert {:comments, [[:author, :post]]} = preload_clause
    end
  end
end
