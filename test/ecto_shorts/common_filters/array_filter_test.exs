defmodule EctoShorts.CommonFilters.ArrayFilterTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 array field filters" do
    test "supports === nil comparisons for array fields (generates IS NULL)" do
      expected = from p in Post, where: is_nil(p.tags)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: nil}, [])

      assert_sql(expected, q2)
    end

    test "supports !== nil comparisons for array fields (generates IS NOT NULL)" do
      expected = from p in Post, where: not is_nil(p.tags)
      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{!=: nil}}, [])

      assert_sql(expected, q2)
    end

    test "array field supports overlaps-any via :in with list RHS" do
      expected =
        from(p in Post,
          where: fragment("? && ?", p.tags, ^["elixir"])
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{in: ["elixir"]}}, [])

      assert_sql(expected, q2)
    end

    test "array field supports NOT overlap via not in with list RHS" do
      expected =
        from(p in Post,
          where: not fragment("? && ?", p.tags, ^["elixir"])
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{in: ["elixir"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports contains-all via in: %{all: list}" do
      expected =
        from(p in Post,
          where: fragment("? @> ?", p.tags, ^["elixir", "erlang"])
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{in: %{all: ["elixir", "erlang"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports negated contains-all via not in: %{all: list}" do
      expected =
        from(p in Post,
          where: not fragment("? @> ?", p.tags, ^["elixir", "erlang"])
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{in: %{all: ["elixir", "erlang"]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports membership via :in with scalar RHS" do
      expected =
        from(p in Post,
          where: ^"elixir" in p.tags
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{in: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field defaults scalar RHS to membership (== value becomes in value)" do
      expected =
        from(p in Post,
          where: ^"elixir" in p.tags
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: "elixir"}, [])

      assert_sql(expected, q2)
    end

    test "array field compares equality when RHS is a list and operator defaults to ==" do
      expected =
        from(p in Post,
          where: p.tags == ^["elixir", "erlang"]
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: ["elixir", "erlang"]}, [])

      assert_sql(expected, q2)
    end

    test "array field supports negated equality when RHS is a list" do
      expected =
        from(p in Post,
          where: p.tags != ^["elixir"]
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{not: %{==: ["elixir"]}}}, [])

      assert_sql(expected, q2)
    end

    test "array field supports > comparison against scalar (any element matches)" do
      expected =
        from(p in Post,
          where: fragment("? < ANY(?)", ^"elixir", p.tags)
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{>: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array field supports >= comparison against scalar (any element matches)" do
      expected =
        from(p in Post,
          where: fragment("? <= ANY(?)", ^"elixir", p.tags)
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{>=: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports < comparison against scalar (any element matches)" do
      expected =
        from(p in Post,
          where: fragment("? > ANY(?)", ^"elixir", p.tags)
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{<: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array field supports <= comparison against scalar (any element matches)" do
      expected =
        from(p in Post,
          where: fragment("? >= ANY(?)", ^"elixir", p.tags)
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{<=: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array field supports negated > comparison against scalar" do
      expected =
        from(p in Post,
          where: not fragment("? < ANY(?)", ^"elixir", p.tags)
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{not: %{>: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports LIKE operator for array fields with list RHS (EXISTS ... LIKE ANY)" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t LIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{tags: %{like: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "supports ILIKE operator for array fields with list RHS (EXISTS ... ILIKE ANY)" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t ILIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(q, %{tags: %{ilike: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "supports LOWER operator for array fields" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE lower(t) = ?
              )
              """,
              p.tags,
              ^"elixir"
            )
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{lower: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for array fields" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE upper(t) = ?
              )
              """,
              p.tags,
              ^"ELIXIR"
            )
        )

      q = Post
      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{upper: "ELIXIR"}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated LOWER operator for array fields" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE lower(t) = ?
              )
              """,
              p.tags,
              ^"elixir"
            )
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{not: %{lower: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated UPPER operator for array fields" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE upper(t) = ?
              )
              """,
              p.tags,
              ^"ELIXIR"
            )
        )

      q = Post

      q2 = CommonFilters.convert_params_to_filter(q, %{tags: %{not: %{upper: "ELIXIR"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports LIKE operator for array fields (casts to text)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t LIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{like: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports ILIKE operator for array fields (casts to text)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t ILIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{ilike: "elixir"}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated LIKE operator for array fields (casts to text)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t LIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{like: "elixir"}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated ILIKE operator for array fields (casts to text)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t ILIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{ilike: "elixir"}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated LIKE operator for array fields with list RHS (NOT EXISTS ... LIKE ANY)" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t LIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{like: ["elixir", "erlang"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports negated ILIKE operator for array fields with list RHS (NOT EXISTS ... ILIKE ANY)" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE t ILIKE ANY (?)
              )
              """,
              p.tags,
              ^patterns
            )
        )

      q = Post

      q2 =
        CommonFilters.convert_params_to_filter(
          q,
          %{tags: %{not: %{ilike: ["elixir", "erlang"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :gt alias operator for array fields" do
      expected =
        from(p in Post,
          where: fragment("? < ANY(?)", ^"elixir", p.tags)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{gt: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "supports :gte alias operator for array fields" do
      expected =
        from(p in Post,
          where: fragment("? <= ANY(?)", ^"elixir", p.tags)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{gte: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "supports :lt alias operator for array fields" do
      expected =
        from(p in Post,
          where: fragment("? > ANY(?)", ^"elixir", p.tags)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{lt: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "supports :lte alias operator for array fields" do
      expected =
        from(p in Post,
          where: fragment("? >= ANY(?)", ^"elixir", p.tags)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{lte: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "supports :eq alias operator for array fields (membership)" do
      expected =
        from(p in Post,
          where: ^"elixir" in p.tags
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{eq: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "supports :count aggregate operator for array fields in :having" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: count(p.tags) > ^0
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{tags: %{count: %{>: 0}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports :count aggregate operator for array fields with implicit == in :having" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: count(p.tags) == ^0
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{tags: %{count: 0}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "supports LOWER operator for array fields via explicit ==" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE lower(t) = ?
              )
              """,
              p.tags,
              ^"elixir"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: %{lower: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for array fields via explicit ==" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE upper(t) = ?
              )
              """,
              p.tags,
              ^"ELIXIR"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: %{upper: "ELIXIR"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports LOWER operator for array fields via explicit !=" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE lower(t) = ?
              )
              """,
              p.tags,
              ^"elixir"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: %{lower: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports UPPER operator for array fields via explicit !=" do
      expected =
        from(p in Post,
          where:
            fragment(
              """
              NOT EXISTS (
                SELECT 1
                FROM unnest(?) AS t
                WHERE upper(t) = ?
              )
              """,
              p.tags,
              ^"ELIXIR"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: %{upper: "ELIXIR"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated >= comparison for array fields" do
      expected =
        from(p in Post,
          where: not fragment("? <= ANY(?)", ^"elixir", p.tags)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{>=: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated < comparison for array fields" do
      expected =
        from(p in Post,
          where: not fragment("? > ANY(?)", ^"elixir", p.tags)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{<: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "supports negated <= comparison for array fields" do
      expected =
        from(p in Post,
          where: not fragment("? >= ANY(?)", ^"elixir", p.tags)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{<=: "elixir"}}}, [])

      assert_sql(expected, q2)
    end
  end
end
