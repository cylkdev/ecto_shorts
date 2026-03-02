defmodule EctoShorts.CommonFilters.ArrayFilterTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 array field equality and membership" do
    test "checks if the array contains the value when given a plain string" do
      expected = from(p in Post, where: ^"elixir" in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: "elixir"}, [])

      assert_sql(expected, q2)
    end

    test "checks if the array contains the value using explicit ==" do
      expected = from(p in Post, where: ^"elixir" in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the array contains the value using !=" do
      expected = from(p in Post, where: ^"elixir" not in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the array contains the value using the ne alias" do
      expected = from(p in Post, where: ^"elixir" not in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{ne: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "checks if the array contains the value using the in operator" do
      expected = from(p in Post, where: ^"elixir" in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{in: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "checks if the array exactly equals the given list" do
      expected = from(p in Post, where: p.tags == ^["elixir", "erlang"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: ["elixir", "erlang"]}, [])

      assert_sql(expected, q2)
    end

    test "checks if the array exactly equals the given list using explicit ==" do
      expected = from(p in Post, where: p.tags == ^["elixir", "erlang"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the array exactly equals the given list using !=" do
      expected = from(p in Post, where: p.tags != ^["elixir"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: ["elixir"]}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the array exactly equals the given list using the ne alias" do
      expected = from(p in Post, where: p.tags != ^["elixir"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{ne: ["elixir"]}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the array field is nil" do
      expected = from p in Post, where: is_nil(p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: nil}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the array field is nil using explicit ==" do
      expected = from p in Post, where: is_nil(p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{==: nil}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the array field is not nil using !=" do
      expected = from p in Post, where: not is_nil(p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{!=: nil}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the array field is not nil using the ne alias" do
      expected = from p in Post, where: not is_nil(p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{ne: nil}}, [])

      assert_sql(expected, q2)
    end

    test "checks if the array contains the value using the eq alias" do
      expected = from(p in Post, where: ^"elixir" in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{eq: "elixir"}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 array field overlaps and contains-all" do
    test "checks if the array overlaps with a single-element list" do
      expected = from(p in Post, where: fragment("? && ?", p.tags, ^["elixir"]))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{in: ["elixir"]}}, [])

      assert_sql(expected, q2)
    end

    test "checks if the array overlaps with a multi-element list" do
      expected = from(p in Post, where: fragment("? && ?", p.tags, ^["elixir", "erlang"]))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{in: ["elixir", "erlang"]}}, [])

      assert_sql(expected, q2)
    end

    test "checks if the array contains all elements in the list" do
      expected = from(p in Post, where: fragment("? @> ?", p.tags, ^["elixir", "erlang"]))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{all: %{in: ["elixir", "erlang"]}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the array exactly equals the list using negated ==" do
      expected = from(p in Post, where: p.tags != ^["elixir"])
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{==: ["elixir"]}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the array overlaps with the list" do
      expected = from(p in Post, where: not fragment("? && ?", p.tags, ^["elixir"]))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{in: ["elixir"]}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the array contains the value using negated in" do
      expected = from(p in Post, where: ^"elixir" not in p.tags)
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{in: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the array contains all elements in the list" do
      expected = from(p in Post, where: not fragment("? @> ?", p.tags, ^["elixir", "erlang"]))

      q2 =
        CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{all: %{in: ["elixir", "erlang"]}}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 array field comparisons" do
    test "matches records where any array element is greater than the value" do
      expected = from(p in Post, where: fragment("? < ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{>: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where any array element is greater than or equal to the value" do
      expected = from(p in Post, where: fragment("? <= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{>=: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where any array element is less than the value" do
      expected = from(p in Post, where: fragment("? > ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{<: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where any array element is less than or equal to the value" do
      expected = from(p in Post, where: fragment("? >= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{<=: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the gt alias on an array field" do
      expected = from(p in Post, where: fragment("? < ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{gt: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the gte alias on an array field" do
      expected = from(p in Post, where: fragment("? <= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{gte: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the lt alias on an array field" do
      expected = from(p in Post, where: fragment("? > ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{lt: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the lte alias on an array field" do
      expected = from(p in Post, where: fragment("? >= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{lte: "elixir"}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where any array element is greater than the value" do
      expected = from(p in Post, where: not fragment("? < ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{>: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where any array element is greater than or equal to the value" do
      expected = from(p in Post, where: not fragment("? <= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{>=: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where any array element is less than the value" do
      expected = from(p in Post, where: not fragment("? > ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{<: "elixir"}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where any array element is less than or equal to the value" do
      expected = from(p in Post, where: not fragment("? >= ANY(?)", ^"elixir", p.tags))
      q2 = CommonFilters.convert_params_to_filter(Post, %{tags: %{not: %{<=: "elixir"}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 array field string matching" do
    test "matches records where any array element matches the like pattern" do
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

    test "matches records where any array element matches the ilike pattern" do
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

    test "matches records where any array element matches any like pattern in the list" do
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

    test "matches records where any array element matches any ilike pattern in the list" do
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

    test "excludes records where any array element matches the like pattern" do
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

    test "excludes records where any array element matches the ilike pattern" do
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

    test "excludes records where any array element matches any like pattern in the list" do
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

    test "excludes records where any array element matches any ilike pattern in the list" do
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
    test "matches records where any lowercased array element equals the value" do
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

    test "matches records where any uppercased array element equals the value" do
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

    test "excludes records where any lowercased array element equals the value" do
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

    test "excludes records where any uppercased array element equals the value" do
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

    test "excludes records where any lowercased array element matches using negated ==" do
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

    test "excludes records where any uppercased array element matches using negated ==" do
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
    test "filters by array element count greater than zero in a having clause" do
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

    test "excludes records where the array element count equals zero in a having clause" do
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

    test "filters by array element count equal to zero in a having clause" do
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
