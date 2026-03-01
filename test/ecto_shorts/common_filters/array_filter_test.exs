defmodule EctoShorts.CommonFilters.ArrayFilterTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 array field equality and membership" do
    test "array - %{tags: \"elixir\"} (scalar defaults to membership)" do
      expected = from(p in Post, where: ^"elixir" in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: "elixir"}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{==: \"elixir\"}} (explicit == scalar is membership)" do
      expected = from(p in Post, where: ^"elixir" in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{!=: \"elixir\"}}" do
      expected = from(p in Post, where: ^"elixir" not in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{ne: \"elixir\"}}" do
      expected = from(p in Post, where: ^"elixir" not in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{ne: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{in: \"elixir\"}} (scalar in array membership)" do
      expected = from(p in Post, where: ^"elixir" in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{in: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: [\"elixir\", \"erlang\"]} (list defaults to == equality)" do
      expected = from(p in Post, where: p.tags == ^["elixir", "erlang"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: ["elixir", "erlang"]}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{==: [\"elixir\", \"erlang\"]}} (explicit == list equality)" do
      expected = from(p in Post, where: p.tags == ^["elixir", "erlang"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{!=: [\"elixir\"]}}" do
      expected = from(p in Post, where: p.tags != ^["elixir"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: ["elixir"]}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{ne: [\"elixir\"]}}" do
      expected = from(p in Post, where: p.tags != ^["elixir"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{ne: ["elixir"]}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: nil} (IS NULL)" do
      expected = from p in Post, where: is_nil(p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: nil}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{==: nil}} (IS NULL)" do
      expected = from p in Post, where: is_nil(p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: nil}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{!=: nil}} (IS NOT NULL)" do
      expected = from p in Post, where: not is_nil(p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: nil}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{ne: nil}} (IS NOT NULL)" do
      expected = from p in Post, where: not is_nil(p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{ne: nil}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{eq: \"elixir\"}} (alias membership)" do
      expected = from(p in Post, where: ^"elixir" in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{eq: "elixir"}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 array field overlaps and contains-all" do
    test "array - %{tags: %{in: [\"elixir\"]}} (overlaps-any)" do
      expected = from(p in Post, where: fragment("? && ?", p.tags, ^["elixir"]))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{in: ["elixir"]}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{in: [\"elixir\", \"erlang\"]}} (overlaps-any)" do
      expected = from(p in Post, where: fragment("? && ?", p.tags, ^["elixir", "erlang"]))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{in: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{all: %{in: [\"elixir\", \"erlang\"]}}} (contains-all)" do
      expected = from(p in Post, where: fragment("? @> ?", p.tags, ^["elixir", "erlang"]))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{all: %{in: ["elixir", "erlang"]}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{==: [\"elixir\"]}}} (negated list equality)" do
      expected = from(p in Post, where: p.tags != ^["elixir"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{==: ["elixir"]}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{in: [\"elixir\"]}}} (NOT overlap)" do
      expected = from(p in Post, where: not fragment("? && ?", p.tags, ^["elixir"]))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{in: ["elixir"]}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{in: \"elixir\"}}} (negated scalar membership)" do
      expected = from(p in Post, where: ^"elixir" not in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{in: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{all: %{in: [\"elixir\", \"erlang\"]}}}}" do
      expected = from(p in Post, where: not fragment("? @> ?", p.tags, ^["elixir", "erlang"]))

      q2 =
        CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{all: %{in: ["elixir", "erlang"]}}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 array field comparisons" do
    test "array - %{tags: %{>: \"elixir\"}}" do
      expected = from(p in Post, where: fragment("? < ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{>: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{>=: \"elixir\"}}" do
      expected = from(p in Post, where: fragment("? <= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{>=: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{<: \"elixir\"}}" do
      expected = from(p in Post, where: fragment("? > ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{<: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{<=: \"elixir\"}}" do
      expected = from(p in Post, where: fragment("? >= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{<=: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{gt: \"elixir\"}}" do
      expected = from(p in Post, where: fragment("? < ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{gt: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{gte: \"elixir\"}}" do
      expected = from(p in Post, where: fragment("? <= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{gte: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{lt: \"elixir\"}}" do
      expected = from(p in Post, where: fragment("? > ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{lt: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{lte: \"elixir\"}}" do
      expected = from(p in Post, where: fragment("? >= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{lte: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{>: \"elixir\"}}}" do
      expected = from(p in Post, where: not fragment("? < ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{>: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{>=: \"elixir\"}}}" do
      expected = from(p in Post, where: not fragment("? <= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{>=: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{<: \"elixir\"}}}" do
      expected = from(p in Post, where: not fragment("? > ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{<: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{<=: \"elixir\"}}}" do
      expected = from(p in Post, where: not fragment("? >= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{<=: "elixir"}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 array field string matching" do
    test "array - %{tags: %{like: \"elixir\"}} (scalar, casts to list)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{like: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{ilike: \"elixir\"}} (scalar, casts to list)" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{ilike: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{like: [\"elixir\", \"erlang\"]}}" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{like: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{ilike: [\"elixir\", \"erlang\"]}}" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{ilike: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{like: \"elixir\"}}}" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{like: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{ilike: \"elixir\"}}}" do
      patterns = ["%elixir%"]

      expected =
        from(p in Post,
          where:
            fragment(
              "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{ilike: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{like: [\"elixir\", \"erlang\"]}}}" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{like: ["elixir", "erlang"]}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{ilike: [\"elixir\", \"erlang\"]}}}" do
      patterns = ["%elixir%", "%erlang%"]

      expected =
        from(p in Post,
          where:
            fragment(
              "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{ilike: ["elixir", "erlang"]}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 array field string transformations" do
    test "array - %{tags: %{==: %{lower: \"elixir\"}}}" do
      expected =
        from(p in Post,
          where:
            fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n",
              p.tags,
              ^"elixir"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: %{lower: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{==: %{upper: \"ELIXIR\"}}}" do
      expected =
        from(p in Post,
          where:
            fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n",
              p.tags,
              ^"ELIXIR"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: %{upper: "ELIXIR"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{!=: %{lower: \"elixir\"}}}" do
      expected =
        from(p in Post,
          where:
            fragment(
              "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n",
              p.tags,
              ^"elixir"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: %{lower: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{!=: %{upper: \"ELIXIR\"}}}" do
      expected =
        from(p in Post,
          where:
            fragment(
              "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n",
              p.tags,
              ^"ELIXIR"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: %{upper: "ELIXIR"}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{==: %{lower: \"elixir\"}}}}" do
      expected =
        from(p in Post,
          where:
            fragment(
              "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n",
              p.tags,
              ^"elixir"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{==: %{lower: "elixir"}}}}, [])

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{not: %{==: %{upper: \"ELIXIR\"}}}}" do
      expected =
        from(p in Post,
          where:
            fragment(
              "NOT EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n",
              p.tags,
              ^"ELIXIR"
            )
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{==: %{upper: "ELIXIR"}}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 array field aggregates" do
    test "array - %{tags: %{count: %{>: 0}}} (in :having)" do
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

    test "array - %{tags: %{not: %{count: %{==: 0}}}} (negated count in :having)" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: not (count(p.tags) == ^0)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{tags: %{not: %{count: %{==: 0}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "array - %{tags: %{count: %{==: 0}}} (in :having)" do
      expected =
        from(p in Post,
          group_by: p.author_id,
          having: count(p.tags) == ^0
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id, having: %{tags: %{count: %{==: 0}}}},
          []
        )

      assert_sql(expected, q2)
    end
  end
end
