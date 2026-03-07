# This is a living document. Update it as decisions are made.
# Keep the test references up to date with COMMON_FILTERS.md

defmodule EctoShorts.QueryFiltersTest do
  use ExUnit.Case

  import Ecto.Query
  alias Ecto.Adapters.SQL.Sandbox

  setup do
    pid = Sandbox.start_owner!(TestRepo, shared: true)
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  describe "Array Fields" do
    test "Rule Statement 1: tags contains single value" do
      # Given: [tags: "elixir"]
      # Expected: "elixir" in p.tags

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Elixir Post", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Ruby Post", tags: ["ruby"]})

      query = from(p in EctoShorts.TestPost, where: "elixir" in p.tags)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 2: tags equals operator with single value" do
      # Given: [tags: [==: "elixir"]]
      # Expected: "elixir" in p.tags

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Elixir Post", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Ruby Post", tags: ["ruby"]})

      query = from(p in EctoShorts.TestPost, where: "elixir" in p.tags)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 3: tags not equals single value" do
      # Given: [tags: [!=: "elixir"]]
      # Expected: not ("elixir" in p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Elixir Post", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Ruby Post", tags: ["ruby"]})

      query = from(p in EctoShorts.TestPost, where: "elixir" not in p.tags)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["ruby"]}] = results
    end

    test "Rule Statement 4: tags in operator with single value" do
      # Given: [tags: [in: "elixir"]]
      # Expected: "elixir" in p.tags

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Elixir Post", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Ruby Post", tags: ["ruby"]})

      query = from(p in EctoShorts.TestPost, where: "elixir" in p.tags)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 5: tags equals array" do
      # Given: [tags: ["elixir", "erlang"]]
      # Expected: p.tags == ["elixir", "erlang"]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Exact Match", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Different", tags: ["elixir"]})

      query = from(p in EctoShorts.TestPost, where: p.tags == ^["elixir", "erlang"])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 6: tags equals operator with array" do
      # Given: [tags: [==: ["elixir", "erlang"]]]
      # Expected: p.tags == ["elixir", "erlang"]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Exact Match", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Different", tags: ["elixir"]})

      query = from(p in EctoShorts.TestPost, where: p.tags == ^["elixir", "erlang"])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 7: tags not equals array" do
      # Given: [tags: [!=: ["elixir"]]]
      # Expected: p.tags != ["elixir"]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Exact Match", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Different", tags: ["elixir", "erlang"]})

      query = from(p in EctoShorts.TestPost, where: p.tags != ^["elixir"])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 8: tags is nil" do
      # Given: [tags: nil]
      # Expected: is_nil(p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "No Tags", tags: nil})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Tags", tags: ["elixir"]})

      query = from(p in EctoShorts.TestPost, where: is_nil(p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: nil}] = results
    end

    test "Rule Statement 9: tags equals nil" do
      # Given: [tags: [==: nil]]
      # Expected: is_nil(p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "No Tags", tags: nil})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Tags", tags: ["elixir"]})

      query = from(p in EctoShorts.TestPost, where: is_nil(p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: nil}] = results
    end

    test "Rule Statement 10: tags not equals nil" do
      # Given: [tags: [!=: nil]]
      # Expected: not is_nil(p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "No Tags", tags: nil})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Tags", tags: ["elixir"]})

      query = from(p in EctoShorts.TestPost, where: not is_nil(p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir"]}] = results
    end

    test "Rule Statement 11: tags array overlaps" do
      # Given: [tags: [in: ["elixir"]]]
      # Expected: fragment("? && ?", p.tags, ["elixir"])

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Elixir", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Elixir", tags: ["ruby"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? && ?", p.tags, ^["elixir"]))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 12: tags array contains all" do
      # Given: [tags: [all: [in: ["elixir", "erlang"]]]]
      # Expected: fragment("? <@ ?", p.tags, ["elixir", "erlang"])

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Subset", tags: ["elixir"]})

      {:ok, _post2} =
        TestRepo.insert(%EctoShorts.TestPost{title: "Superset", tags: ["elixir", "erlang", "ruby"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? <@ ?", p.tags, ^["elixir", "erlang"]))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir"]}] = results
    end

    test "Rule Statement 13: negated tags equals array" do
      # Given: [not: [tags: [==: ["elixir"]]]]
      # Expected: not (p.tags == ["elixir"])

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Exact Match", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Different", tags: ["elixir", "erlang"]})

      query = from(p in EctoShorts.TestPost, where: not (p.tags == ^["elixir"]))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 14: negated tags contains value" do
      # Given: [not: [tags: [in: "elixir"]]]
      # Expected: not ("elixir" in p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Elixir", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Elixir", tags: ["ruby"]})

      query = from(p in EctoShorts.TestPost, where: "elixir" not in p.tags)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["ruby"]}] = results
    end

    test "Rule Statement 15: tags any greater than" do
      # Given: [tags: [>: "elixir"]]
      # Expected: fragment("? > ANY(?)", "elixir", p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Smaller", tags: ["erlang", "c"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "All Greater", tags: ["ruby", "python"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? > ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["erlang", "c"]}] = results
    end

    test "Rule Statement 16: tags any greater than or equal" do
      # Given: [tags: [>=: "elixir"]]
      # Expected: fragment("? >= ANY(?)", "elixir", p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has LTE", tags: ["elixir", "c"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "All Greater", tags: ["ruby", "python"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? >= ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "c"]}] = results
    end

    test "Rule Statement 17: tags any less than" do
      # Given: [tags: [<: "elixir"]]
      # Expected: fragment("? < ANY(?)", "elixir", p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Greater", tags: ["ruby", "python"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "All Smaller", tags: ["c", "d"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? < ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["ruby", "python"]}] = results
    end

    test "Rule Statement 18: tags any less than or equal" do
      # Given: [tags: [<=: "elixir"]]
      # Expected: fragment("? <= ANY(?)", "elixir", p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has GTE", tags: ["elixir", "ruby"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "All Smaller", tags: ["c", "d"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? <= ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "ruby"]}] = results
    end

    test "Rule Statement 19: tags any like" do
      # Given: [tags: [like: "elixir"]]
      # Expected: fragment("? LIKE ANY(?)", "elixir", p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Match", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", tags: ["ruby"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? LIKE ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 20: tags any ilike" do
      # Given: [tags: [ilike: "elixir"]]
      # Expected: fragment("? ILIKE ANY(?)", "elixir", p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Match", tags: ["ELIXIR", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", tags: ["ruby"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? ILIKE ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["ELIXIR", "erlang"]}] = results
    end

    test "Rule Statement 21: tags any like any" do
      # Given: [tags: [like: ["elixir", "erlang"]]]
      # Expected: fragment("? && ?", p.tags, ["elixir", "erlang"])

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Match", tags: ["elixir", "ruby"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", tags: ["python"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? && ?", p.tags, ^["elixir", "erlang"]))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "ruby"]}] = results
    end

    test "Rule Statement 22: negated tags any like" do
      # Given: [not: [tags: [like: "elixir"]]]
      # Expected: not fragment("? LIKE ANY(?)", "elixir", p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Match", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", tags: ["ruby"]})

      query = from(p in EctoShorts.TestPost, where: not fragment("? LIKE ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["ruby"]}] = results
    end

    test "Rule Statement 23: tags contains lowercased value" do
      # Given: [tags: [==: [lower: "elixir"]]]
      # Expected: fragment("lower(?)", "elixir") in p.tags

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Lower", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Upper", tags: ["ELIXIR"]})

      query = from(p in EctoShorts.TestPost, where: fragment("lower(?)", "elixir") in p.tags)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 24: tags not contains uppercased value" do
      # Given: [tags: [!=: [upper: "ELIXIR"]]]
      # Expected: fragment("upper(?)", "ELIXIR") not in p.tags

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Lower", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Upper", tags: ["ELIXIR"]})

      query = from(p in EctoShorts.TestPost, where: fragment("upper(?)", "ELIXIR") not in p.tags)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 25: tags count greater than" do
      # Given: [tags: [count: [>: 0]]]
      # Expected: fragment("array_length(?, 1)", p.tags) > 0

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Tags", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Empty Tags", tags: []})

      query = from(p in EctoShorts.TestPost, where: fragment("array_length(?, 1)", p.tags) > 0)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: ["elixir"]}] = results
    end

    test "Rule Statement 26: tags count equals zero" do
      # Given: [tags: [count: [==: 0]]]
      # Expected: fragment("coalesce(array_length(?, 1), 0)", p.tags) == 0

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Tags", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Empty Tags", tags: []})

      query = from(p in EctoShorts.TestPost, where: fragment("coalesce(array_length(?, 1), 0)", p.tags) == 0)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{tags: []}] = results
    end

    test "Rule Statement 27: tags all greater than" do
      # Given: [tags: [all: [>: "a"]]]
      # Expected: fragment("? > ALL(?)", "a", p.tags)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "All Smaller", tags: ["!", "0"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Greater", tags: ["b", "c"]})

      query = from(p in EctoShorts.TestPost, where: fragment("? > ALL(?)", ^"a", p.tags))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "All Smaller"}] = results
    end

    test "Rule Statement 28: tags any ilike any" do
      # Given: [tags: [ilike: ["elixir", "erlang"]]]
      # Expected: fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", p.tags, ^["%elixir%", "%erlang%"])

      patterns = ["%elixir%", "%erlang%"]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Match", tags: ["ELIXIR", "ruby"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", tags: ["python"]})

      query =
        from(p in EctoShorts.TestPost,
          where:
            fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Has Match"}] = results
    end

    test "Rule Statement 29: negated tags any ilike" do
      # Given: [not: [tags: [ilike: "elixir"]]]
      # Expected: not fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", p.tags, ^["%elixir%"])

      patterns = ["%elixir%"]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Match", tags: ["ELIXIR", "ruby"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", tags: ["python"]})

      query =
        from(p in EctoShorts.TestPost,
          where:
            not fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n",
              p.tags,
              ^patterns
            )
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "No Match"}] = results
    end

    test "Rule Statement 30: lower tags equals value" do
      # Given: [lower: [tags: [==: "elixir"]]]
      # Expected: fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n", p.tags, ^"elixir")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Upper", tags: ["ELIXIR"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", tags: ["ruby"]})

      query =
        from(p in EctoShorts.TestPost,
          where:
            fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n",
              p.tags,
              ^"elixir"
            )
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Has Upper"}] = results
    end

    test "Rule Statement 31: upper tags equals value" do
      # Given: [upper: [tags: [==: "ELIXIR"]]]
      # Expected: fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n", p.tags, ^"ELIXIR")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Lower", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", tags: ["ruby"]})

      query =
        from(p in EctoShorts.TestPost,
          where:
            fragment(
              "EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n",
              p.tags,
              ^"ELIXIR"
            )
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Has Lower"}] = results
    end

    test "Rule Statement 32: tags count less than" do
      # Given: [tags: [count: [<: 5]]]
      # Expected: fragment("array_length(?, 1)", p.tags) < 5

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Short List", tags: ["a", "b"]})

      {:ok, _post2} =
        TestRepo.insert(%EctoShorts.TestPost{title: "Long List", tags: ["a", "b", "c", "d", "e"]})

      query = from(p in EctoShorts.TestPost, where: fragment("array_length(?, 1)", p.tags) < 5)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Short List"}] = results
    end

    test "Rule Statement 33: tags count greater than or equal" do
      # Given: [tags: [count: [>=: 2]]]
      # Expected: fragment("array_length(?, 1)", p.tags) >= 2

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Two Tags", tags: ["a", "b"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "One Tag", tags: ["a"]})

      query = from(p in EctoShorts.TestPost, where: fragment("array_length(?, 1)", p.tags) >= 2)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Two Tags"}] = results
    end

    test "Rule Statement 34: tags count less than or equal" do
      # Given: [tags: [count: [<=: 10]]]
      # Expected: fragment("array_length(?, 1)", p.tags) <= 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Ten Or Fewer", tags: ["a", "b"]})

      {:ok, _post2} =
        TestRepo.insert(%EctoShorts.TestPost{
          title: "More Than Ten",
          tags: ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"]
        })

      query = from(p in EctoShorts.TestPost, where: fragment("array_length(?, 1)", p.tags) <= 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Ten Or Fewer"}] = results
    end

    test "Rule Statement 35: tags count not equals" do
      # Given: [tags: [count: [!=: 3]]]
      # Expected: fragment("array_length(?, 1)", p.tags) != 3

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Two Tags", tags: ["a", "b"]})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Three Tags", tags: ["a", "b", "c"]})

      query = from(p in EctoShorts.TestPost, where: fragment("array_length(?, 1)", p.tags) != 3)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Two Tags"}] = results
    end
  end

  describe "Scalar Fields" do
    test "Rule Statement 1: id equals value" do
      # Given: [id: 1]
      # Expected: p.id == 1

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})

      query = from(p in EctoShorts.TestPost, where: p.id == ^post1.id)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 2: published equals boolean" do
      # Given: [published: true]
      # Expected: p.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Unpublished", published: false})

      query = from(p in EctoShorts.TestPost, where: p.published == true)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published: true}] = results
    end

    test "Rule Statement 3: multiple scalar fields with and" do
      # Given: [id: 1, published: true]
      # Expected: p.id == 1 and p.published == true

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", published: false})

      query = from(p in EctoShorts.TestPost, where: p.id == ^post1.id and p.published == true)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match", published: true}] = results
    end

    test "Rule Statement 4: published_at is nil" do
      # Given: [published_at: nil]
      # Expected: is_nil(p.published_at)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "No Date", published_at: nil})

      {:ok, _post2} =
        TestRepo.insert(%EctoShorts.TestPost{title: "Has Date", published_at: ~U[2026-01-01 00:00:00Z]})

      query = from(p in EctoShorts.TestPost, where: is_nil(p.published_at))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published_at: nil}] = results
    end

    test "Rule Statement 5: published in list" do
      # Given: [published: [true, false]]
      # Expected: p.published in [true, false]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Unpublished", published: false})

      query = from(p in EctoShorts.TestPost, where: p.published in [true, false])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published: false}, %EctoShorts.TestPost{published: true}] =
               Enum.sort_by(results, & &1.published)
    end

    test "Rule Statement 6: title equals string" do
      # Given: [title: "hello"]
      # Expected: p.title == "hello"

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: p.title == "hello")
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "hello"}] = results
    end

    test "Rule Statement 7: explicit and with single condition" do
      # Given: [and: [[id: 1]]]
      # Expected: p.id == 1

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})

      query = from(p in EctoShorts.TestPost, where: p.id == ^post1.id)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 8: explicit and with multiple conditions" do
      # Given: [and: [[id: 1], [published: true]]]
      # Expected: p.id == 1 and p.published == true

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", published: false})

      query = from(p in EctoShorts.TestPost, where: p.id == ^post1.id and p.published == true)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match", published: true}] = results
    end

    test "Rule Statement 9: explicit and with map conditions" do
      # Given: [and: [id: 1, title: "hello"]]
      # Expected: p.id == 1 and p.title == "hello"

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: p.id == ^post1.id and p.title == "hello")
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "hello"}] = results
    end

    test "Rule Statement 10: nested and conditions" do
      # Given: [and: [[id: 1, title: "hello"], [published: true]]]
      # Expected: (p.id == 1 and p.title == "hello") and p.published == true

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", published: false})

      query =
        from(p in EctoShorts.TestPost,
          where: p.id == ^post1.id and p.title == "hello" and p.published == true
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "hello", published: true}] = results
    end

    test "Rule Statement 11: or with map conditions" do
      # Given: [or: [[views: 1, title: "hello"], [published: true]]]
      # Expected: (p.views == 1 and p.title == "hello") or p.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", views: 1, published: false})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 20, published: true})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 50, published: false})

      query =
        from(p in EctoShorts.TestPost, where: (p.views == 1 and p.title == "hello") or p.published == true)

      results = TestRepo.all(query)

      assert [
               %EctoShorts.TestPost{title: "hello", published: false},
               %EctoShorts.TestPost{title: "world", published: true}
             ] =
               Enum.sort_by(results, &{&1.title, &1.published})
    end

    test "Rule Statement 12: nested or within and" do
      # Given: [or: [[id: 1, or: [title: "hello", body: "world"]], [published: true]]]
      # Expected: (p.id == 1 and (p.title == "hello" or p.body == "world")) or p.published == true

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", body: "test", published: false})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "test", body: "world", published: false})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "test", body: "test", published: true})
      {:ok, _post4} = TestRepo.insert(%EctoShorts.TestPost{title: "test", body: "test", published: false})

      query =
        from(p in EctoShorts.TestPost,
          where: (p.id == ^post1.id and (p.title == "hello" or p.body == "world")) or p.published == true
        )

      results = TestRepo.all(query)

      assert [
               %EctoShorts.TestPost{title: "hello", published: false},
               %EctoShorts.TestPost{title: "test", published: true}
             ] =
               Enum.sort_by(results, &{&1.title, &1.published})
    end
  end

  describe "Negation Directives" do
    test "Rule Statement 1: negated published in list" do
      # Given: [not: [published: [in: [true]]]]
      # Expected: p.published not in [true]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Unpublished", published: false})

      query = from(p in EctoShorts.TestPost, where: p.published not in [true])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published: false}] = results
    end

    test "Rule Statement 2: negated published equals operator with list" do
      # Given: [not: [published: [==: [true]]]]
      # Expected: p.published not in [true]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Unpublished", published: false})

      query = from(p in EctoShorts.TestPost, where: p.published not in [true])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published: false}] = results
    end

    test "Rule Statement 3: negated published not equals operator with list" do
      # Given: [not: [published: [!=: [true]]]]
      # Expected: p.published in [true]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "True", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "False", published: false})

      query = from(p in EctoShorts.TestPost, where: p.published in [true])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published: true}] =
               Enum.sort_by(results, & &1.published)
    end

    test "Rule Statement 4: negated views greater than" do
      # Given: [not: [views: [>: 10]]]
      # Expected: not (p.views > 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: not (p.views > 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 5}] = results
    end

    test "Rule Statement 5: negated views greater than or equal" do
      # Given: [not: [views: [>=: 10]]]
      # Expected: not (p.views >= 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})

      query = from(p in EctoShorts.TestPost, where: not (p.views >= 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 5}] = results
    end

    test "Rule Statement 6: negated views less than" do
      # Given: [not: [views: [<: 10]]]
      # Expected: not (p.views < 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: not (p.views < 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 20}] = results
    end

    test "Rule Statement 7: negated views less than or equal" do
      # Given: [not: [views: [<=: 10]]]
      # Expected: not (p.views <= 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: not (p.views <= 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 20}] = results
    end

    test "Rule Statement 8: negated views equals" do
      # Given: [not: [views: [==: 10]]]
      # Expected: not (p.views == 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Different Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})

      query = from(p in EctoShorts.TestPost, where: not (p.views == 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 5}] = results
    end

    test "Rule Statement 9: negated views not equals" do
      # Given: [not: [views: [!=: 10]]]
      # Expected: not (p.views != 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Different Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})

      query = from(p in EctoShorts.TestPost, where: not (p.views != 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 10}] = results
    end

    test "Rule Statement 10: negated views gt alias" do
      # Given: [not: [views: [gt: 10]]]
      # Expected: not (p.views > 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: not (p.views > 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 5}] = results
    end

    test "Rule Statement 11: negated views gte alias" do
      # Given: [not: [views: [gte: 10]]]
      # Expected: not (p.views >= 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})

      query = from(p in EctoShorts.TestPost, where: not (p.views >= 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 5}] = results
    end

    test "Rule Statement 12: negated views lt alias" do
      # Given: [not: [views: [lt: 10]]]
      # Expected: not (p.views < 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: not (p.views < 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 20}] = results
    end

    test "Rule Statement 13: negated views lte alias" do
      # Given: [not: [views: [lte: 10]]]
      # Expected: not (p.views <= 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: not (p.views <= 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 20}] = results
    end
  end

  describe "Logical Operator Directives" do
    test "Rule Statement 1: and with multiple conditions on same field" do
      # Given: [and: [title: "hello", views: [>: 10, <: 20]]]
      # Expected: p.title == "hello" and (p.views > 10 and p.views < 20)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", views: 15})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", views: 5})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 15})

      query = from(p in EctoShorts.TestPost, where: p.title == "hello" and (p.views > 10 and p.views < 20))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "hello", views: 15}] = results
    end

    test "Rule Statement 2: nested and with multiple conditions" do
      # Given: [and: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]
      # Expected: (p.title == "hello" and (p.views > 10 and p.views < 20)) and p.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", views: 15, published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", views: 15, published: false})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", views: 5, published: true})

      query =
        from(p in EctoShorts.TestPost,
          where: p.title == "hello" and (p.views > 10 and p.views < 20) and p.published == true
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "hello", views: 15, published: true}] = results
    end

    test "Rule Statement 3: or with nested and conditions" do
      # Given: [or: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]
      # Expected: (p.title == "hello" and (p.views > 10 and p.views < 20)) or p.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", views: 15, published: false})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 5, published: true})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 5, published: false})

      query =
        from(p in EctoShorts.TestPost,
          where: (p.title == "hello" and (p.views > 10 and p.views < 20)) or p.published == true
        )

      results = TestRepo.all(query)

      assert [
               %EctoShorts.TestPost{title: "hello", published: false},
               %EctoShorts.TestPost{title: "world", published: true}
             ] =
               Enum.sort_by(results, &{&1.title, &1.published})
    end

    test "Rule Statement 4: nested or within or" do
      # Given: [or: [[or: [title: "hello", views: [>: 10, <: 20]]], [published: true]]]
      # Expected: (p.title == "hello" or (p.views > 10 and p.views < 20)) or p.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", views: 5, published: false})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 15, published: false})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 5, published: true})
      {:ok, _post4} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 5, published: false})

      query =
        from(p in EctoShorts.TestPost,
          where: p.title == "hello" or (p.views > 10 and p.views < 20) or p.published == true
        )

      results = TestRepo.all(query)

      assert [
               %EctoShorts.TestPost{title: "hello"},
               %EctoShorts.TestPost{title: "world", views: 15},
               %EctoShorts.TestPost{title: "world", published: true}
             ] =
               Enum.sort_by(results, &{&1.title, &1.published, &1.views})
    end

    test "Rule Statement 5: nested or within and within or" do
      # Given: [or: [[title: "hello", or: [views: [>: 10, <: 20]]], [published: true]]]
      # Expected: (p.title == "hello" and (p.views > 10 or p.views < 20)) or p.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello", views: 5, published: false})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 15, published: false})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 5, published: true})
      {:ok, _post4} = TestRepo.insert(%EctoShorts.TestPost{title: "world", views: 5, published: false})

      query =
        from(p in EctoShorts.TestPost,
          where: (p.title == "hello" and (p.views > 10 or p.views < 20)) or p.published == true
        )

      results = TestRepo.all(query)

      assert [
               %EctoShorts.TestPost{title: "hello"},
               %EctoShorts.TestPost{title: "world", published: true}
             ] =
               Enum.sort_by(results, &{&1.title, &1.published, &1.views})
    end
  end

  describe "Comparison Operator Directives" do
    test "Rule Statement 1: title equals with == operator" do
      # Given: [title: [==: "Post 1"]]
      # Expected: p.title == "Post 1"

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})

      query = from(p in EctoShorts.TestPost, where: p.title == "Post 1")
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 2: title equals with eq operator" do
      # Given: [title: [eq: "Post 1"]]
      # Expected: p.title == "Post 1"

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})

      query = from(p in EctoShorts.TestPost, where: p.title == "Post 1")
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 3: published_at equals nil with ==" do
      # Given: [published_at: [==: nil]]
      # Expected: is_nil(p.published_at)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "No Date", published_at: nil})

      {:ok, _post2} =
        TestRepo.insert(%EctoShorts.TestPost{title: "Has Date", published_at: ~U[2026-01-01 00:00:00Z]})

      query = from(p in EctoShorts.TestPost, where: is_nil(p.published_at))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published_at: nil}] = results
    end

    test "Rule Statement 4: published_at equals nil with eq" do
      # Given: [published_at: [eq: nil]]
      # Expected: is_nil(p.published_at)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "No Date", published_at: nil})

      {:ok, _post2} =
        TestRepo.insert(%EctoShorts.TestPost{title: "Has Date", published_at: ~U[2026-01-01 00:00:00Z]})

      query = from(p in EctoShorts.TestPost, where: is_nil(p.published_at))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published_at: nil}] = results
    end

    test "Rule Statement 5: published_at not equals nil" do
      # Given: [published_at: [!=: nil]]
      # Expected: not is_nil(p.published_at)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "No Date", published_at: nil})

      {:ok, _post2} =
        TestRepo.insert(%EctoShorts.TestPost{title: "Has Date", published_at: ~U[2026-01-01 00:00:00Z]})

      query = from(p in EctoShorts.TestPost, where: not is_nil(p.published_at))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published_at: ~U[2026-01-01 00:00:00Z]}] = results
    end

    test "Rule Statement 6: published_at ne nil" do
      # Given: [published_at: [ne: nil]]
      # Expected: not is_nil(p.published_at)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "No Date", published_at: nil})

      {:ok, _post2} =
        TestRepo.insert(%EctoShorts.TestPost{title: "Has Date", published_at: ~U[2026-01-01 00:00:00Z]})

      query = from(p in EctoShorts.TestPost, where: not is_nil(p.published_at))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published_at: ~U[2026-01-01 00:00:00Z]}] = results
    end

    test "Rule Statement 7: views greater than" do
      # Given: [views: [>: 10]]
      # Expected: p.views > 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: p.views > 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 20}] = results
    end

    test "Rule Statement 8: views greater than or equal" do
      # Given: [views: [>=: 10]]
      # Expected: p.views >= 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})

      query = from(p in EctoShorts.TestPost, where: p.views >= 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 10}] = results
    end

    test "Rule Statement 9: views less than" do
      # Given: [views: [<: 10]]
      # Expected: p.views < 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: p.views < 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 5}] = results
    end

    test "Rule Statement 10: views less than or equal" do
      # Given: [views: [<=: 10]]
      # Expected: p.views <= 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: p.views <= 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 10}] = results
    end

    test "Rule Statement 11: views not equals" do
      # Given: [views: [!=: 10]]
      # Expected: p.views != 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Different Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})

      query = from(p in EctoShorts.TestPost, where: p.views != 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 5}] = results
    end

    test "Rule Statement 12: views ne operator" do
      # Given: [views: [ne: 10]]
      # Expected: p.views != 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Different Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})

      query = from(p in EctoShorts.TestPost, where: p.views != 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 5}] = results
    end

    test "Rule Statement 13: views gt operator" do
      # Given: [views: [gt: 10]]
      # Expected: p.views > 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: p.views > 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 20}] = results
    end

    test "Rule Statement 14: views gte operator" do
      # Given: [views: [gte: 10]]
      # Expected: p.views >= 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})

      query = from(p in EctoShorts.TestPost, where: p.views >= 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 10}] = results
    end

    test "Rule Statement 15: views lt operator" do
      # Given: [views: [lt: 10]]
      # Expected: p.views < 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: p.views < 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 5}] = results
    end

    test "Rule Statement 16: views lte operator" do
      # Given: [views: [lte: 10]]
      # Expected: p.views <= 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Equal Views", views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", views: 20})

      query = from(p in EctoShorts.TestPost, where: p.views <= 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{views: 10}] = results
    end

    test "Rule Statement 17: published in list" do
      # Given: [published: [in: [true, false]]]
      # Expected: p.published in [true, false]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Unpublished", published: false})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Null", published: nil})

      query = from(p in EctoShorts.TestPost, where: p.published in [true, false])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published: false}, %EctoShorts.TestPost{published: true}] =
               Enum.sort_by(results, & &1.published)
    end

    test "Rule Statement 18: published equals list" do
      # Given: [published: [==: [true, false]]]
      # Expected: p.published in [true, false]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "True", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "False", published: false})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Null", published: nil})

      query = from(p in EctoShorts.TestPost, where: p.published in [true, false])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published: false}, %EctoShorts.TestPost{published: true}] =
               Enum.sort_by(results, & &1.published)
    end

    test "Rule Statement 19: published not equals list" do
      # Given: [published: [!=: [true, false]]]
      # Expected: is_nil(p.published) or p.published not in [true, false]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "True", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "False", published: false})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Null", published: nil})

      query = from(p in EctoShorts.TestPost, where: is_nil(p.published) or p.published not in [true, false])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published: nil}] = results
    end

    test "Rule Statement 20: published ne list" do
      # Given: [published: [ne: [true, false]]]
      # Expected: is_nil(p.published) or p.published not in [true, false]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "True", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "False", published: false})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Null", published: nil})

      query = from(p in EctoShorts.TestPost, where: is_nil(p.published) or p.published not in [true, false])
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{published: nil}] = results
    end
  end

  describe "String Matching Directives" do
    test "Rule Statement 1: title like pattern" do
      # Given: [title: [like: "%hello%"]]
      # Expected: like(p.title, "%hello%")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "say hello world"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "goodbye"})

      query = from(p in EctoShorts.TestPost, where: like(p.title, "%hello%"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "say hello world"}] = results
    end

    test "Rule Statement 2: title like pattern negated" do
      # Given: [not: [title: [like: "%hello%"]]]
      # Expected: not like(p.title, "%hello%")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "say hello world"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "goodbye"})

      query = from(p in EctoShorts.TestPost, where: not like(p.title, "%hello%"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "goodbye"}] = results
    end

    test "Rule Statement 3: title ilike pattern" do
      # Given: [title: [ilike: "%HELLO%"]]
      # Expected: ilike(p.title, "%HELLO%")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "say hello world"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "goodbye"})

      query = from(p in EctoShorts.TestPost, where: ilike(p.title, "%HELLO%"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "say hello world"}] = results
    end

    test "Rule Statement 4: title ilike pattern negated" do
      # Given: [not: [title: [ilike: "%HELLO%"]]]
      # Expected: not ilike(p.title, "%HELLO%")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "say hello world"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "goodbye"})

      query = from(p in EctoShorts.TestPost, where: not ilike(p.title, "%HELLO%"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "goodbye"}] = results
    end

    test "Rule Statement 5: title like list of patterns" do
      # Given: [title: [like: ["%hello%", "%world%"]]]
      # Expected: like(p.title, "%hello%") or like(p.title, "%world%")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello there"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world news"})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "goodbye"})

      query = from(p in EctoShorts.TestPost, where: like(p.title, "%hello%") or like(p.title, "%world%"))
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 6: title ilike list of patterns" do
      # Given: [title: [ilike: ["%HELLO%", "%WORLD%"]]]
      # Expected: ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello there"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "WORLD news"})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "goodbye"})

      query = from(p in EctoShorts.TestPost, where: ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%"))
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 7: title like list negated" do
      # Given: [not: [title: [like: ["%hello%", "%world%"]]]]
      # Expected: not (like(p.title, "%hello%") or like(p.title, "%world%"))

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello there"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world news"})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "goodbye"})

      query =
        from(p in EctoShorts.TestPost, where: not (like(p.title, "%hello%") or like(p.title, "%world%")))

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "goodbye"}] = results
    end

    test "Rule Statement 8: title ilike list negated" do
      # Given: [not: [title: [ilike: ["%HELLO%", "%WORLD%"]]]]
      # Expected: not (ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%"))

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello there"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "WORLD news"})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "goodbye"})

      query =
        from(p in EctoShorts.TestPost, where: not (ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%")))

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "goodbye"}] = results
    end
  end

  describe "String Transformation Directives" do
    test "Rule Statement 1: equals lowercased value" do
      # Given: [title: [==: [lower: "hello"]]]
      # Expected: p.title == fragment("lower(?)", "hello")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: p.title == fragment("lower(?)", "hello"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "hello"}] = results
    end

    test "Rule Statement 2: equals uppercased value" do
      # Given: [title: [==: [upper: "HELLO"]]]
      # Expected: p.title == fragment("upper(?)", "HELLO")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "HELLO"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: p.title == fragment("upper(?)", "HELLO"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "HELLO"}] = results
    end

    test "Rule Statement 3: not equals lowercased value" do
      # Given: [title: [!=: [lower: "hello"]]]
      # Expected: p.title != fragment("lower(?)", "hello")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: p.title != fragment("lower(?)", "hello"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "world"}] = results
    end

    test "Rule Statement 4: not equals uppercased value" do
      # Given: [title: [!=: [upper: "HELLO"]]]
      # Expected: p.title != fragment("upper(?)", "HELLO")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "HELLO"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: p.title != fragment("upper(?)", "HELLO"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "world"}] = results
    end

    test "Rule Statement 5: negated equals lowercased value" do
      # Given: [not: [title: [==: [lower: "hello"]]]]
      # Expected: not (p.title == fragment("lower(?)", "hello"))

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: not (p.title == fragment("lower(?)", "hello")))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "world"}] = results
    end

    test "Rule Statement 6: negated equals uppercased value" do
      # Given: [not: [title: [==: [upper: "HELLO"]]]]
      # Expected: not (p.title == fragment("upper(?)", "HELLO"))

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "HELLO"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: not (p.title == fragment("upper(?)", "HELLO")))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "world"}] = results
    end

    test "Rule Statement 7: equals lowercased value" do
      # Given: [title: [==: [lower: "HELLO"]]]
      # Expected: p.title == fragment("lower(?)", "HELLO")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: p.title == fragment("lower(?)", "HELLO"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "hello"}] = results
    end

    test "Rule Statement 8: equals uppercased value" do
      # Given: [title: [==: [upper: "hello"]]]
      # Expected: p.title == fragment("upper(?)", "hello")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "HELLO"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "world"})

      query = from(p in EctoShorts.TestPost, where: p.title == fragment("upper(?)", "hello"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "HELLO"}] = results
    end
  end

  describe "Aggregate Operator Directives" do
    test "Rule Statement 1: avg views greater than" do
      # Given: [views: [avg: [>: 10]]]
      # Expected: avg(p.views) > 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 5})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: avg(p.views) > 10, select: p.title)
      results = TestRepo.all(query)

      assert ["High"] = results
    end

    test "Rule Statement 2: avg views greater than negated" do
      # Given: [not: [views: [avg: [>: 10]]]]
      # Expected: not (avg(p.views) > 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 5})

      query =
        from(p in EctoShorts.TestPost, group_by: p.title, having: not (avg(p.views) > 10), select: p.title)

      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 3: count views greater than zero" do
      # Given: [views: [count: [>: 0]]]
      # Expected: count(p.views) > 0

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Views", views: 5})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: count(p.views) > 0, select: p.title)
      results = TestRepo.all(query)

      assert ["Has Views"] = results
    end

    test "Rule Statement 4: max views greater than or equal" do
      # Given: [views: [max: [>=: 100]]]
      # Expected: max(p.views) >= 100

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 100})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 50})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: max(p.views) >= 100, select: p.title)
      results = TestRepo.all(query)

      assert ["High"] = results
    end

    test "Rule Statement 5: min views less than" do
      # Given: [views: [min: [<: 5]]]
      # Expected: min(p.views) < 5

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 3})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 10})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: min(p.views) < 5, select: p.title)
      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 6: sum views equals" do
      # Given: [views: [sum: [==: 1000]]]
      # Expected: sum(p.views) == 1000

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "A", views: 1000})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "B", views: 500})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: sum(p.views) == 1000, select: p.title)
      results = TestRepo.all(query)

      assert ["A"] = results
    end

    test "Rule Statement 7: avg views not equals" do
      # Given: [views: [avg: [!=: 50]]]
      # Expected: avg(p.views) != 50

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", views: 20})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Skip", views: 50})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: avg(p.views) != 50, select: p.title)
      results = TestRepo.all(query)

      assert ["Match"] = results
    end

    test "Rule Statement 8: count views equals nil" do
      # Given: [views: [count: [==: nil]]]
      # Expected: is_nil(count(p.views))

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", views: 5})

      query =
        from(p in EctoShorts.TestPost,
          group_by: p.title,
          having: is_nil(count(p.views)),
          select: p.title
        )

      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 9: count views greater than zero negated" do
      # Given: [not: [views: [count: [>: 0]]]]
      # Expected: not (count(p.views) > 0)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Has Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Zero Views", views: 0})

      query =
        from(p in EctoShorts.TestPost, group_by: p.title, having: not (count(p.views) > 0), select: p.title)

      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 10: max views greater than or equal negated" do
      # Given: [not: [views: [max: [>=: 100]]]]
      # Expected: not (max(p.views) >= 100)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 100})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 50})

      query =
        from(p in EctoShorts.TestPost, group_by: p.title, having: not (max(p.views) >= 100), select: p.title)

      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 11: avg views less than or equal" do
      # Given: [views: [avg: [<=: 10]]]
      # Expected: avg(p.views) <= 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 8})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: avg(p.views) <= 10, select: p.title)
      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 12: sum views greater than" do
      # Given: [views: [sum: [>: 500]]]
      # Expected: sum(p.views) > 500

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Big", views: 600})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Small", views: 200})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: sum(p.views) > 500, select: p.title)
      results = TestRepo.all(query)

      assert ["Big"] = results
    end

    test "Rule Statement 13: min views equals zero" do
      # Given: [views: [min: [==: 0]]]
      # Expected: min(p.views) == 0

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Zero", views: 0})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Positive", views: 5})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: min(p.views) == 0, select: p.title)
      results = TestRepo.all(query)

      assert ["Zero"] = results
    end

    test "Rule Statement 14: sum views not equals zero" do
      # Given: [views: [sum: [!=: 0]]]
      # Expected: sum(p.views) != 0

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Zero", views: 0})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Positive", views: 5})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: sum(p.views) != 0, select: p.title)
      results = TestRepo.all(query)

      assert ["Positive"] = results
    end

    test "Rule Statement 15: min views less than negated" do
      # Given: [not: [views: [min: [<: 5]]]]
      # Expected: not (min(p.views) < 5)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 3})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 10})

      query =
        from(p in EctoShorts.TestPost, group_by: p.title, having: not (min(p.views) < 5), select: p.title)

      results = TestRepo.all(query)

      assert ["High"] = results
    end

    test "Rule Statement 16: sum views greater than negated" do
      # Given: [not: [views: [sum: [>: 500]]]]
      # Expected: not (sum(p.views) > 500)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Big", views: 600})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Small", views: 200})

      query =
        from(p in EctoShorts.TestPost, group_by: p.title, having: not (sum(p.views) > 500), select: p.title)

      results = TestRepo.all(query)

      assert ["Small"] = results
    end
  end

  describe "Arithmetic Operator Directives" do
    test "Rule Statement 1: views greater than views plus 10" do
      # Given: [views: [>: [+: [:views, 10]]]]
      # Expected: p.views > p.views + 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", views: 5})

      query = from(p in EctoShorts.TestPost, where: p.views > p.views + 10)
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 2: views greater than views plus 10 negated" do
      # Given: [not: [views: [>: [+: [:views, 10]]]]]
      # Expected: not (p.views > p.views + 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", views: 5})

      query = from(p in EctoShorts.TestPost, where: not (p.views > p.views + 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 3: views greater than or equal to views minus 5" do
      # Given: [views: [>=: [-: [:views, 5]]]]
      # Expected: p.views >= p.views - 5

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", views: 10})

      query = from(p in EctoShorts.TestPost, where: p.views >= p.views - 5)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 4: views less than views times 2" do
      # Given: [views: [<: [*: [:views, 2]]]]
      # Expected: p.views < p.views * 2

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Positive", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Zero", views: 0})

      query = from(p in EctoShorts.TestPost, where: p.views < p.views * 2)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Positive"}] = results
    end

    test "Rule Statement 5: views equals views divided by 2" do
      # Given: [views: [==: [/: [:views, 2]]]]
      # Expected: p.views == p.views / 2

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Zero", views: 0})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "NonZero", views: 5})

      query = from(p in EctoShorts.TestPost, where: p.views == p.views / 2)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Zero"}] = results
    end

    test "Rule Statement 6: views not equals views plus 10" do
      # Given: [views: [!=: [+: [:views, 10]]]]
      # Expected: p.views != p.views + 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", views: 5})

      query = from(p in EctoShorts.TestPost, where: p.views != p.views + 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 7: views greater than or equal to views minus 5 negated" do
      # Given: [not: [views: [>=: [-: [:views, 5]]]]]
      # Expected: not (p.views >= p.views - 5)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", views: 10})

      query = from(p in EctoShorts.TestPost, where: not (p.views >= p.views - 5))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 8: views less than views times 2 negated" do
      # Given: [not: [views: [<: [*: [:views, 2]]]]]
      # Expected: not (p.views < p.views * 2)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Positive", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Zero", views: 0})

      query = from(p in EctoShorts.TestPost, where: not (p.views < p.views * 2))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Zero"}] = results
    end

    test "Rule Statement 9: views less than or equal to literal 10 plus 5" do
      # Given: [views: [<=: [+: [10, 5]]]]
      # Expected: p.views <= 10 + 5

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Within", views: 14})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Over", views: 20})

      query = from(p in EctoShorts.TestPost, where: p.views <= 10 + 5)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Within"}] = results
    end

    test "Rule Statement 10: views greater than literal 100 minus 10" do
      # Given: [views: [>: [-: [100, 10]]]]
      # Expected: p.views > ^(100 - 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Over", views: 95})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Under", views: 85})

      query = from(p in EctoShorts.TestPost, where: p.views > ^(100 - 10))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Over"}] = results
    end

    test "Rule Statement 11: views less than or equal to literal 10 times 2" do
      # Given: [views: [<=: [*: [10, 2]]]]
      # Expected: p.views <= 10 * 2

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Within", views: 15})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Over", views: 25})

      query = from(p in EctoShorts.TestPost, where: p.views <= 10 * 2)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Within"}] = results
    end

    test "Rule Statement 12: views equals literal 10 divided by 2" do
      # Given: [views: [==: [/: [10, 2]]]]
      # Expected: p.views == 10 / 2

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", views: 4})

      query = from(p in EctoShorts.TestPost, where: p.views == 10 / 2)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match"}] = results
    end
  end

  describe "Set Comparison Directives" do
    test "Rule Statement 1: id greater than all subquery values" do
      # Given: [id: [>: [all: subquery_expr]]]
      # Expected: p.id > all(subquery_expr)

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      subquery_expr = subquery(from c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id > all(subquery_expr))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 2"}] = results
      assert post2.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 2: not id greater than all subquery values" do
      # Given: [not: [id: [>: [all: [from: Comment, body: "Hello"]]]]]
      # Expected: not (p.id > all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id)))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: not (p.id > all(subquery(inner_query))))
      results = TestRepo.all(query)

      assert post1.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 3: id greater than any subquery value" do
      # Given: [id: [>: [any: [from: Comment, body: "Hello"]]]]
      # Expected: p.id > any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id > any(subquery(inner_query)))
      results = TestRepo.all(query)

      assert post2.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 4: not id greater than any subquery value" do
      # Given: [not: [id: [>: [any: [from: Comment, body: "Hello"]]]]]
      # Expected: not (p.id > any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id)))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: not (p.id > any(subquery(inner_query))))
      results = TestRepo.all(query)

      assert post1.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 5: id greater than or equal to all subquery values" do
      # Given: [id: [>=: [all: [from: Comment, body: "Hello"]]]]
      # Expected: p.id >= all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id >= all(subquery(inner_query)))
      results = TestRepo.all(query)

      result_ids = Enum.map(results, & &1.id)
      assert post1.id in result_ids
      assert post2.id in result_ids
    end

    test "Rule Statement 6: id less than all subquery values" do
      # Given: [id: [<: [all: [from: Comment, body: "Hello"]]]]
      # Expected: p.id < all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post2.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id < all(subquery(inner_query)))
      results = TestRepo.all(query)

      assert post1.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 7: id less than or equal to all subquery values" do
      # Given: [id: [<=: [all: [from: Comment, body: "Hello"]]]]
      # Expected: p.id <= all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post2.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id <= all(subquery(inner_query)))
      results = TestRepo.all(query)

      result_ids = Enum.map(results, & &1.id)
      assert post1.id in result_ids
      assert post2.id in result_ids
    end

    test "Rule Statement 8: id equals all subquery values" do
      # Given: [id: [==: [all: [from: Comment, body: "Hello"]]]]
      # Expected: p.id == all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id == all(subquery(inner_query)))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 9: id not equals all subquery values" do
      # Given: [id: [!=: [all: [from: Comment, body: "Hello"]]]]
      # Expected: p.id != all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id != all(subquery(inner_query)))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 2"}] = results
      assert post2.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 10: id default equals all subquery values" do
      # Given: [id: [all: [from: Comment, body: "Hello"]]]
      # Expected: p.id == all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id == all(subquery(inner_query)))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 11: id equals all from inline filter params" do
      # Given: [id: [all: [from: Comment, published: true]]]
      # Expected: p.id == all(subquery(from c in Comment, where: c.published == true, select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})

      {:ok, _comment1} =
        TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello", published: true})

      inner_query = from(c in EctoShorts.TestComment, where: c.published == true, select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id == all(subquery(inner_query)))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 12: not id default equals all subquery values" do
      # Given: [not: [id: [all: [from: Comment, body: "Hello"]]]]
      # Expected: not (p.id == all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id)))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: not (p.id == all(subquery(inner_query))))
      results = TestRepo.all(query)

      assert post2.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 13: id default equals any subquery value" do
      # Given: [id: [any: [from: Comment, body: "Hello"]]]
      # Expected: p.id == any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id == any(subquery(inner_query)))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 14: not id default equals any subquery value" do
      # Given: [not: [id: [any: [from: Comment, body: "Hello"]]]]
      # Expected: not (p.id == any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id)))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: not (p.id == any(subquery(inner_query))))
      results = TestRepo.all(query)

      assert post2.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 15: id greater than or equal to any subquery values" do
      # Given: [id: [>=: [any: [from: Comment, body: "Hello"]]]]
      # Expected: p.id >= any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id >= any(subquery(inner_query)))
      results = TestRepo.all(query)

      result_ids = Enum.map(results, & &1.id)
      assert post1.id in result_ids
      assert post2.id in result_ids
    end

    test "Rule Statement 16: id less than any subquery values" do
      # Given: [id: [<: [any: [from: Comment, body: "Hello"]]]]
      # Expected: p.id < any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post2.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id < any(subquery(inner_query)))
      results = TestRepo.all(query)

      assert post1.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 17: id less than or equal to any subquery values" do
      # Given: [id: [<=: [any: [from: Comment, body: "Hello"]]]]
      # Expected: p.id <= any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post2.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id <= any(subquery(inner_query)))
      results = TestRepo.all(query)

      result_ids = Enum.map(results, & &1.id)
      assert post1.id in result_ids
      assert post2.id in result_ids
    end

    test "Rule Statement 18: id equals any subquery values" do
      # Given: [id: [==: [any: [from: Comment, body: "Hello"]]]]
      # Expected: p.id == any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id == any(subquery(inner_query)))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 19: id not equals any subquery values" do
      # Given: [id: [!=: [any: [from: Comment, body: "Hello"]]]]
      # Expected: p.id != any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})
      {:ok, _comment1} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "Hello"})
      inner_query = from(c in EctoShorts.TestComment, where: c.body == "Hello", select: c.post_id)

      query = from(p in EctoShorts.TestPost, where: p.id != any(subquery(inner_query)))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 2"}] = results
      assert post2.id in Enum.map(results, & &1.id)
    end
  end

  describe "Date/Time Directives" do
    test "Rule Statement 1: inserted_at greater than or equal to datetime_add" do
      # Given: [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at >= datetime_add(p.inserted_at, 1, "day")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: p.inserted_at >= datetime_add(p.inserted_at, 1, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 2: inserted_at greater than ago 1 day" do
      # Given: [inserted_at: [>: [datetime: [ago: [count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at > ago(1, "day")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Recent"})

      query = from(p in EctoShorts.TestPost, where: p.inserted_at > ago(1, "day"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Recent"}] = results
    end

    test "Rule Statement 3: inserted_at greater than from_now 1 day" do
      # Given: [inserted_at: [>: [datetime: [from_now: [count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at > from_now(1, "day")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: p.inserted_at > from_now(1, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 4: inserted_at >= datetime_add negated" do
      # Given: [not: [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]]
      # Expected: not (p.inserted_at >= datetime_add(p.inserted_at, 1, "day"))

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query =
        from(p in EctoShorts.TestPost, where: not (p.inserted_at >= datetime_add(p.inserted_at, 1, "day")))

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 5: inserted_at less than ago 7 days" do
      # Given: [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]
      # Expected: p.inserted_at < ago(7, "day")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: p.inserted_at < ago(7, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 6: inserted_at less than or equal to from_now 30 days" do
      # Given: [inserted_at: [<=: [datetime: [from_now: [count: 30, interval: "day"]]]]]
      # Expected: p.inserted_at <= from_now(30, "day")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: p.inserted_at <= from_now(30, "day"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 7: inserted_at equals ago 1 day using date wrapper" do
      # Given: [inserted_at: [==: [date: [ago: [count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at == ago(1, "day")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: p.inserted_at == ago(1, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 8: inserted_at not equals from_now 1 day using date wrapper" do
      # Given: [inserted_at: [!=: [date: [from_now: [count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at != from_now(1, "day")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: p.inserted_at != from_now(1, "day"))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 9: inserted_at less than ago 7 days negated" do
      # Given: [not: [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]]
      # Expected: not (p.inserted_at < ago(7, "day"))

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: not (p.inserted_at < ago(7, "day")))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 10: inserted_at greater than from_now 1 day negated using date wrapper" do
      # Given: [not: [inserted_at: [>: [date: [from_now: [count: 1, interval: "day"]]]]]]
      # Expected: not (p.inserted_at > from_now(1, "day"))

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: not (p.inserted_at > from_now(1, "day")))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 11: inserted_at >= datetime_add 7 days using date wrapper" do
      # Given: [inserted_at: [>=: [date: [add: [field: :inserted_at, count: 7, interval: "day"]]]]]
      # Expected: p.inserted_at >= datetime_add(p.inserted_at, 7, "day")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: p.inserted_at >= datetime_add(p.inserted_at, 7, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 12: inserted_at less than ago 1 month using date wrapper" do
      # Given: [inserted_at: [<: [date: [ago: [count: 1, interval: "month"]]]]]
      # Expected: p.inserted_at < ago(1, "month")

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, where: p.inserted_at < ago(1, "month"))
      results = TestRepo.all(query)

      assert [] = results
    end
  end

  describe "Schema Filter Directives" do
    test "Rule Statement 1: explicit where clause" do
      # Given: [where: [published: true]]
      # Expected: where: p.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query = from(p in EctoShorts.TestPost, where: p.published == true)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 2: explicit where clause with multiple conditions" do
      # Given: [where: [published: true, views: 10]]
      # Expected: where: p.published == true and p.views == 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", published: true, views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Wrong Views", published: true, views: 5})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false, views: 10})

      query = from(p in EctoShorts.TestPost, where: p.published == true and p.views == 10)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match"}] = results
    end

    test "Rule Statement 3: explicit or_where clause" do
      # Given: [or_where: [published: false]]
      # Expected: or_where: p.published == false

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query = from(p in EctoShorts.TestPost, or_where: p.published == false)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Draft"}] = results
    end

    test "Rule Statement 4: combined where and or_where" do
      # Given: [where: [published: true], or_where: [published: false]]
      # Expected: (p.published == true) or (p.published == false)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Null", published: nil})

      query = from(p in EctoShorts.TestPost, where: p.published == true, or_where: p.published == false)
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 5: or_where with nested or logic and implicit where" do
      # Given: [or_where: [or: [views: [>: 10, <: 5]]], published: true]
      # Expected: (p.published == true) or (p.views > 10 or p.views < 5)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true, views: 7})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High Views", published: false, views: 15})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", published: false, views: 3})
      {:ok, _post4} = TestRepo.insert(%EctoShorts.TestPost{title: "Mid Views", published: false, views: 7})

      query =
        from(p in EctoShorts.TestPost,
          where: p.published == true,
          or_where: p.views > 10 or p.views < 5
        )

      results = TestRepo.all(query)

      assert 3 = length(results)
    end
  end

  describe "Binding Selector Directives" do
    test "Rule Statement 1: named binding as :post" do
      # Given: [bind: [as: :post, published: true]]
      # Expected: post.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query = from(p in EctoShorts.TestPost, as: :post, where: as(:post).published == true)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 2: multiple named bindings with comment" do
      # Given: [bind: [[as: :post, published: true], [as: :comment, body: "hi"]]]
      # Expected: post.published == true and comment.body == "hi"

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", published: true})
      {:ok, _} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "hi"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Comment", published: true})

      query =
        from(p in EctoShorts.TestPost,
          as: :post,
          join: c in EctoShorts.TestComment,
          as: :comment,
          on: c.post_id == p.id,
          where: as(:post).published == true and as(:comment).body == "hi"
        )

      results = TestRepo.all(query)

      assert length(results) >= 1
      assert Enum.all?(results, &(&1.published == true))
    end

    test "Rule Statement 3: multiple named bindings with author" do
      # Given: [bind: [[as: :post, published: true], [as: :author, first_name: "John"]]]
      # Expected: post.published == true and author.first_name == "John"

      {:ok, author1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, author2} = TestRepo.insert(%EctoShorts.TestUser{first_name: "Jane"})

      {:ok, _post1} =
        TestRepo.insert(%EctoShorts.TestPost{title: "Match", published: true, author_id: author1.id})

      {:ok, _post2} =
        TestRepo.insert(%EctoShorts.TestPost{title: "Wrong Author", published: true, author_id: author2.id})

      {:ok, _post3} =
        TestRepo.insert(%EctoShorts.TestPost{
          title: "Wrong Published",
          published: false,
          author_id: author1.id
        })

      query =
        from(p in EctoShorts.TestPost,
          as: :post,
          join: a in EctoShorts.TestUser,
          as: :author,
          on: a.id == p.author_id,
          where: as(:post).published == true and as(:author).first_name == "John"
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match", published: true}] = results
    end

    test "Rule Statement 4: positional binding at index 1" do
      # Given: [bind: [at: 1, published: true]]
      # Expected: binding_at_1.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query = from(p in EctoShorts.TestPost, where: p.published == true)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 5: positional binding at index 1 list-wrapped" do
      # Given: [bind: [[at: 1, published: true]]]
      # Expected: binding_at_1.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query = from(p in EctoShorts.TestPost, where: p.published == true)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 6: first binding" do
      # Given: [bind: [at: :first, published: true]]
      # Expected: first_binding.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query = from(p in EctoShorts.TestPost, where: p.published == true)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 7: last binding without join" do
      # Given: [bind: [at: :last, title: "Published"]]
      # Expected: last_binding.title == "Published"

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query = from(p in EctoShorts.TestPost, where: p.title == "Published")
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
      assert post1.id in Enum.map(results, & &1.id)
    end

    test "Rule Statement 8: last binding with author first name" do
      # Given: [bind: [at: :last, first_name: "John"]]
      # Expected: last_binding.first_name == "John"

      {:ok, author1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, author2} = TestRepo.insert(%EctoShorts.TestUser{first_name: "Jane"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", author_id: author1.id})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", author_id: author2.id})

      query =
        from(p in EctoShorts.TestPost,
          join: a in EctoShorts.TestUser,
          on: a.id == p.author_id,
          where: a.first_name == "John"
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match"}] = results
    end
  end

  describe "Terminal Filter Directives" do
    test "Rule Statement 1: last 2 records" do
      # Given: [last: 2]
      # Expected: SELECT ... FROM (SELECT ... ORDER BY id DESC LIMIT 2) AS subquery ORDER BY id ASC

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "First"})
      {:ok, post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Second"})
      {:ok, post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Third"})

      inner = from(p in EctoShorts.TestPost, order_by: [desc: p.id], limit: 2)
      query = from(p in subquery(inner), order_by: [asc: p.id])
      results = TestRepo.all(query)

      assert [%{id: id2}, %{id: id3}] = results
      assert id2 == post2.id
      assert id3 == post3.id
    end

    test "Rule Statement 2: subquery with title filter" do
      # Given: [subquery: [title: "Second"]]
      # Expected: SELECT ... FROM (SELECT ... WHERE title = "Second") AS subquery

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "First"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Second"})

      inner = from(p in EctoShorts.TestPost, where: p.title == "Second")
      query = from(p in subquery(inner))
      results = TestRepo.all(query)

      assert [%{title: "Second"}] = results
    end

    test "Rule Statement 3: published true and subquery with title filter" do
      # Given: [published: true, subquery: [title: "Match"]]
      # Expected: SELECT ... FROM (SELECT ... WHERE published = true AND title = "Match") AS subquery

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published No Match", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", published: true})

      inner = from(p in EctoShorts.TestPost, where: p.published == true and p.title == "Match")
      query = from(p in subquery(inner))
      results = TestRepo.all(query)

      assert [%{title: "Match"}] = results
    end

    test "Rule Statement 4: last 2 records by title" do
      # Given: [last: [title: 2]]
      # Expected: SELECT ... FROM (SELECT ... ORDER BY title DESC LIMIT 2) AS subquery ORDER BY title ASC

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Alpha"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Beta"})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Gamma"})

      inner = from(p in EctoShorts.TestPost, order_by: [desc: p.title], limit: 2)
      query = from(p in subquery(inner), order_by: [asc: p.title])
      results = TestRepo.all(query)

      assert [%{title: "Beta"}, %{title: "Gamma"}] = results
    end

    test "Rule Statement 5: subquery with published true and views greater than 10" do
      # Given: [subquery: [published: true, views: [>: 10]]]
      # Expected: SELECT ... FROM (SELECT ... WHERE published = true AND views > 10) AS subquery

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", published: true, views: 15})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", published: true, views: 5})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Unpublished", published: false, views: 20})

      inner = from(p in EctoShorts.TestPost, where: p.published == true and p.views > 10)
      query = from(p in subquery(inner))
      results = TestRepo.all(query)

      assert [%{title: "Match"}] = results
    end
  end

  describe "Query Configuration Directives" do
    test "Rule Statement 7: from schema with filters" do
      # Given: [from: Post, id: 1, published: true]
      # Expected: from: Post, where: p.id == 1 and p.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{id: 1, title: "Match", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{id: 2, title: "Draft", published: false})

      query = from(p in EctoShorts.TestPost, where: p.id == 1 and p.published == true)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match"}] = results
    end

    test "Rule Statement 11: select true selects all fields" do
      # Given: [select: true]
      # Expected: select: true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, select: p)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 12: select single field" do
      # Given: [select: :id]
      # Expected: select: p.id

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})
      expected_id = post1.id

      query = from(p in EctoShorts.TestPost, select: p.id)
      results = TestRepo.all(query)

      assert [^expected_id] = results
    end

    test "Rule Statement 13: select list of fields" do
      # Given: [select: [:id, :title]]
      # Expected: select: [p.id, p.title]

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})
      expected_id = post1.id

      query = from(p in EctoShorts.TestPost, select: [p.id, p.title])
      results = TestRepo.all(query)

      assert [[^expected_id, "Post"]] = results
    end

    test "Rule Statement 14: select map of fields" do
      # Given: [select: [map: [:id, :title]]]
      # Expected: select: %{id: p.id, title: p.title}

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})
      expected_id = post1.id

      query = from(p in EctoShorts.TestPost, select: %{id: p.id, title: p.title})
      results = TestRepo.all(query)

      assert [%{id: ^expected_id, title: "Post"}] = results
    end

    test "Rule Statement 15: select map with renamed key" do
      # Given: [select: [map: [custom_id: :id]]]
      # Expected: select: %{custom_id: p.id}

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})
      expected_id = post1.id

      query = from(p in EctoShorts.TestPost, select: %{custom_id: p.id})
      results = TestRepo.all(query)

      assert [%{custom_id: ^expected_id}] = results
    end

    test "Rule Statement 16: select struct" do
      # Given: [select: [struct: [:id]]]
      # Expected: select: struct(p, [:id])

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})
      expected_id = post1.id

      query = from(p in EctoShorts.TestPost, select: struct(p, [:id]))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{id: ^expected_id}] = results
    end

    test "Rule Statement 20: distinct true" do
      # Given: [distinct: true]
      # Expected: distinct: true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Same", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Same", published: true})

      query = from(p in EctoShorts.TestPost, distinct: true, select: p.title)
      results = TestRepo.all(query)

      assert ["Same"] = results
    end

    test "Rule Statement 25: group by single field" do
      # Given: [group_by: :author_id]
      # Expected: group_by: p.author_id

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "A", views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "B", views: 20})

      query = from(p in EctoShorts.TestPost, group_by: p.author_id, select: p.author_id)
      results = TestRepo.all(query)

      assert [nil] = results
    end

    test "Rule Statement 27: having clause with equality" do
      # Given: [having: [published: true]]
      # Expected: having: p.published == true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query =
        from(p in EctoShorts.TestPost,
          group_by: [p.title, p.published],
          having: p.published == true,
          select: p.title
        )

      results = TestRepo.all(query)

      assert ["Published"] = results
    end

    test "Rule Statement 34: order by field ascending" do
      # Given: [order_by: :title]
      # Expected: order_by: [asc: p.title]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Beta"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Alpha"})

      query = from(p in EctoShorts.TestPost, order_by: [asc: p.title], select: p.title)
      results = TestRepo.all(query)

      assert ["Alpha", "Beta"] = results
    end

    test "Rule Statement 35: order by field descending" do
      # Given: [order_by: [desc: :title]]
      # Expected: order_by: [desc: p.title]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Alpha"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Beta"})

      query = from(p in EctoShorts.TestPost, order_by: [desc: p.title], select: p.title)
      results = TestRepo.all(query)

      assert ["Beta", "Alpha"] = results
    end

    test "Rule Statement 41: limit results" do
      # Given: [limit: 10]
      # Expected: limit: 10

      for i <- 1..15 do
        {:ok, _} = TestRepo.insert(%EctoShorts.TestPost{title: "Post #{i}"})
      end

      query = from(p in EctoShorts.TestPost, limit: 10)
      results = TestRepo.all(query)

      assert 10 = length(results)
    end

    test "Rule Statement 42: offset results" do
      # Given: [offset: 5]
      # Expected: offset: 5

      for i <- 1..10 do
        {:ok, _} = TestRepo.insert(%EctoShorts.TestPost{title: "Post #{i}"})
      end

      query = from(p in EctoShorts.TestPost, offset: 5, order_by: [asc: p.id])
      results = TestRepo.all(query)

      assert 5 = length(results)
    end

    test "Rule Statement 44: limit and offset" do
      # Given: [limit: 10, offset: 5]
      # Expected: limit: 10, offset: 5

      for i <- 1..20 do
        {:ok, _} = TestRepo.insert(%EctoShorts.TestPost{title: "Post #{i}"})
      end

      query = from(p in EctoShorts.TestPost, limit: 10, offset: 5)
      results = TestRepo.all(query)

      assert 10 = length(results)
    end

    test "Rule Statement 50: start_date filter" do
      # Given: [start_date: ~N[2026-01-01 00:00:00]]
      # Expected: where: p.inserted_at >= ^start_date

      {:ok, _post1} =
        TestRepo.insert(%EctoShorts.TestPost{
          title: "Post",
          inserted_at: ~N[2025-01-01 00:00:00],
          updated_at: ~N[2025-01-01 00:00:00]
        })

      start_date = ~N[2026-01-01 00:00:00]
      query = from(p in EctoShorts.TestPost, where: p.inserted_at >= ^start_date)
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 51: end_date filter" do
      # Given: [end_date: ~N[2026-12-31 23:59:59]]
      # Expected: where: p.inserted_at <= ^end_date

      {:ok, _post1} =
        TestRepo.insert(%EctoShorts.TestPost{
          title: "Post",
          inserted_at: ~N[2026-06-01 00:00:00],
          updated_at: ~N[2026-06-01 00:00:00]
        })

      end_date = ~N[2026-12-31 23:59:59]
      query = from(p in EctoShorts.TestPost, where: p.inserted_at <= ^end_date)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 52: ids filter" do
      # Given: [ids: [1, 2, 3]]
      # Expected: where: p.id in [1, 2, 3]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{id: 1, title: "A"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{id: 2, title: "B"})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{id: 3, title: "C"})
      {:ok, _post4} = TestRepo.insert(%EctoShorts.TestPost{id: 4, title: "D"})

      query = from(p in EctoShorts.TestPost, where: p.id in [1, 2, 3])
      results = TestRepo.all(query)

      assert 3 = length(results)
    end

    test "Rule Statement 1: raw dynamic expression" do
      # Given: [dynamic: dynamic([p], p.views > ^10)]
      # Expected: p.views > 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})

      dyn = dynamic([p], p.views > ^10)
      query = from(p in EctoShorts.TestPost, where: ^dyn)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "High"}] = results
    end

    test "Rule Statement 2: dynamic within where clause" do
      # Given: [where: [dynamic: dynamic([p], p.published === ^true)]]
      # Expected: where: p.published === true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      dyn = dynamic([p], p.published == ^true)
      query = from(p in EctoShorts.TestPost, where: ^dyn)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 3: dynamic within or_where clause" do
      # Given: [or_where: [dynamic: dynamic([p], p.views > ^100)]]
      # Expected: or_where: p.views > 100

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 5})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 200})

      dyn = dynamic([p], p.views > ^100)
      query = from(p in EctoShorts.TestPost, or_where: ^dyn)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "High"}] = results
    end

    test "Rule Statement 4: exists subquery within where clause" do
      # Given: [where: [exists: subquery_expr]]
      # Expected: where: exists(subquery_expr)

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "With Comment"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Comment"})
      {:ok, _comment} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "hi"})

      subquery_expr =
        subquery(from c in EctoShorts.TestComment, where: c.post_id == parent_as(:post).id, select: c.id)

      query = from(p in EctoShorts.TestPost, as: :post, where: exists(subquery_expr))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "With Comment"}] = results
    end

    test "Rule Statement 5: negated exists subquery within where clause" do
      # Given: [where: [not: [exists: subquery_expr]]]
      # Expected: where: not exists(subquery_expr)

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "With Comment"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Comment"})
      {:ok, _comment} = TestRepo.insert(%EctoShorts.TestComment{post_id: post1.id, body: "hi"})

      subquery_expr =
        subquery(from c in EctoShorts.TestComment, where: c.post_id == parent_as(:post).id, select: c.id)

      query = from(p in EctoShorts.TestPost, as: :post, where: not exists(subquery_expr))
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "No Comment"}] = results
    end

    test "Rule Statement 6: published and subquery" do
      # Given: [published: true, subquery: [id: 2]]
      # Expected: from(s in subquery(...), where: s.published == true)

      {:ok, _post1} =
        TestRepo.insert(%EctoShorts.TestPost{id: 1, title: "Published No Match", published: true})

      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{id: 2, title: "Match", published: true})

      inner = from(p in EctoShorts.TestPost, where: p.published == true and p.id == 2)
      query = from(p in subquery(inner))
      results = TestRepo.all(query)

      assert [%{title: "Match"}] = results
    end

    test "Rule Statement 8: from Post with id filter" do
      # Given: [from: Post, id: 1]
      # Expected: from: Post, where: p.id == 1

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{id: 1, title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{id: 2, title: "Post 2"})

      query = from(p in EctoShorts.TestPost, where: p.id == 1)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 9: from table string with id filter" do
      # Given: [from: "posts", id: 1]
      # Expected: from: "posts", where: p.id == 1

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{id: 1, title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{id: 2, title: "Post 2"})

      query = from(p in "posts", where: p.id == 1, select: p.id)
      results = TestRepo.all(query)

      assert [1] = results
    end

    test "Rule Statement 10: from table string with select" do
      # Given: [from: "posts", select: [:id]]
      # Expected: from: "posts", select: [:id]

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})

      query = from(p in "posts", select: p.id)
      results = TestRepo.all(query)

      assert post1.id in results
    end

    test "Rule Statement 17: select_merge map with renamed key" do
      # Given: [select_merge: [map: [custom_id: :id]]]
      # Expected: select_merge: %{custom_id: p.id}

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})
      expected_id = post1.id

      query = from(p in EctoShorts.TestPost, select: %{id: p.id}, select_merge: %{custom_id: p.id})
      results = TestRepo.all(query)

      assert [%{custom_id: ^expected_id}] = results
    end

    test "Rule Statement 18: select_merge map with field list" do
      # Given: [select_merge: [map: [:id, :title]]]
      # Expected: select_merge: %{id: p.id, title: p.title}

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})
      expected_id = post1.id

      query = from(p in EctoShorts.TestPost, select_merge: %{id: p.id, title: p.title})
      results = TestRepo.all(query)

      assert [%{id: ^expected_id, title: "Post"}] = results
    end

    test "Rule Statement 19: select then select_merge" do
      # Given: [select: [map: [:id]], select_merge: [map: [post_title: :title]]]
      # Expected: select: %{id: p.id, post_title: p.title}

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})
      expected_id = post1.id

      query = from(p in EctoShorts.TestPost, select: %{id: p.id}, select_merge: %{post_title: p.title})
      results = TestRepo.all(query)

      assert [%{id: ^expected_id, post_title: "Post"}] = results
    end

    test "Rule Statement 21: distinct false" do
      # Given: [distinct: false]
      # Expected: distinct: false

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Same", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Same", published: true})

      query = from(p in EctoShorts.TestPost, distinct: false, select: p.title)
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 22: distinct on field" do
      # Given: [distinct: :title]
      # Expected: distinct: p.title

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Same"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Same"})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Different"})

      query = from(p in EctoShorts.TestPost, distinct: p.title, select: p.title, order_by: p.title)
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 23: distinct on field descending" do
      # Given: [distinct: [desc: :title]]
      # Expected: distinct: [desc: p.title]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Same"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Same"})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Different"})

      query = from(p in EctoShorts.TestPost, distinct: [desc: p.title], select: p.title)
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 24: distinct with order_by" do
      # Given: [distinct: :title, order_by: :id]
      # Expected: distinct: p.title, order_by: p.id

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Same"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Same"})

      query = from(p in EctoShorts.TestPost, distinct: p.title, order_by: p.id, select: p.title)
      results = TestRepo.all(query)

      assert ["Same"] = results
    end

    test "Rule Statement 26: group by multiple fields" do
      # Given: [group_by: [:author_id, :published]]
      # Expected: group_by: [p.author_id, p.published]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "A", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "B", published: false})

      query =
        from(p in EctoShorts.TestPost,
          group_by: [p.author_id, p.published],
          select: {p.author_id, p.published}
        )

      results = TestRepo.all(query)

      assert length(results) >= 1
    end

    test "Rule Statement 28: having views greater than" do
      # Given: [having: [views: [>: 10]]]
      # Expected: having: p.views > 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 5})

      query =
        from(p in EctoShorts.TestPost, group_by: [p.title, p.views], having: p.views > 10, select: p.title)

      results = TestRepo.all(query)

      assert ["High"] = results
    end

    test "Rule Statement 29: having avg views greater than" do
      # Given: [having: [views: [avg: [>: 10]]]]
      # Expected: having: avg(p.views) > 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 5})

      query = from(p in EctoShorts.TestPost, group_by: p.title, having: avg(p.views) > 10, select: p.title)
      results = TestRepo.all(query)

      assert ["High"] = results
    end

    test "Rule Statement 30: having dynamic expression" do
      # Given: [having: dynamic([p], p.views > ^10)]
      # Expected: having: p.views > 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 5})

      dyn = dynamic([p], p.views > ^10)
      query = from(p in EctoShorts.TestPost, group_by: [p.title, p.views], having: ^dyn, select: p.title)
      results = TestRepo.all(query)

      assert ["High"] = results
    end

    test "Rule Statement 31: having and" do
      # Given: [having: [and: [published: true, views: [>: 10]]]]
      # Expected: having: p.published == true and p.views > 10

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", published: true, views: 20})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Not Pub", published: false, views: 20})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Low Views", published: true, views: 5})

      query =
        from(p in EctoShorts.TestPost,
          group_by: [p.title, p.published, p.views],
          having: p.published == true and p.views > 10,
          select: p.title
        )

      results = TestRepo.all(query)

      assert ["Match"] = results
    end

    test "Rule Statement 32: having or" do
      # Given: [having: [or: [views: [>: 10], views: [<: 5]]]]
      # Expected: having: p.views > 10 or p.views < 5

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 3})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Mid", views: 7})

      query =
        from(p in EctoShorts.TestPost,
          group_by: [p.title, p.views],
          having: p.views > 10 or p.views < 5,
          select: p.title
        )

      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 32A: having not views greater than" do
      # Given: [having: [not: [views: [>: 10]]]]
      # Expected: having: not (p.views > 10)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 5})

      query =
        from(p in EctoShorts.TestPost,
          group_by: [p.title, p.views],
          having: not (p.views > 10),
          select: p.title
        )

      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 33: or_having" do
      # Given: [or_having: [views: [<: 5]]]
      # Expected: or_having: p.views < 5

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 3})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})

      query =
        from(p in EctoShorts.TestPost,
          group_by: [p.title, p.views],
          or_having: p.views < 5,
          select: p.title
        )

      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 33A: or_having avg views less than" do
      # Given: [or_having: [views: [avg: [<: 5]]]]
      # Expected: or_having: avg(p.views) < 5

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Low", views: 3})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "High", views: 20})

      query =
        from(p in EctoShorts.TestPost,
          group_by: p.title,
          or_having: avg(p.views) < 5,
          select: p.title
        )

      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 36: order by asc and desc" do
      # Given: [order_by: [asc: :title, desc: :id]]
      # Expected: order_by: [asc: p.title, desc: p.id]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Alpha"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Beta"})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Alpha"})

      query = from(p in EctoShorts.TestPost, order_by: [asc: p.title, desc: p.id], select: p.title)
      results = TestRepo.all(query)

      assert List.first(results) == "Alpha"
      assert List.last(results) == "Beta"
    end

    test "Rule Statement 37: prepend_order_by single field" do
      # Given: [prepend_order_by: :title]
      # Expected: prepend_order_by: [desc: p.title]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Beta"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Alpha"})

      query =
        EctoShorts.TestPost
        |> order_by([p], desc: p.id)
        |> select([p], p.title)
        |> prepend_order_by([p], desc: p.title)

      results = TestRepo.all(query)

      assert "Beta" = List.first(results)
    end

    test "Rule Statement 38: prepend_order_by multiple fields" do
      # Given: [prepend_order_by: [asc: :published_at, desc: :title]]
      # Expected: prepend_order_by: [asc: p.published_at, desc: p.title]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Beta"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Alpha"})

      query =
        EctoShorts.TestPost
        |> order_by([p], desc: p.id)
        |> select([p], p.title)
        |> prepend_order_by([p], asc: p.published_at, desc: p.title)

      results = TestRepo.all(query)

      assert is_list(results)
    end

    test "Rule Statement 39: after cursor" do
      # Given: [after: 10]
      # Expected: after: 10

      for i <- 1..15 do
        {:ok, _} = TestRepo.insert(%EctoShorts.TestPost{title: "Post #{i}"})
      end

      query = from(p in EctoShorts.TestPost, offset: 10, order_by: [asc: p.id])
      results = TestRepo.all(query)

      assert 5 = length(results)
    end

    test "Rule Statement 40: before cursor" do
      # Given: [before: 10]
      # Expected: before: 10

      for i <- 1..15 do
        {:ok, _} = TestRepo.insert(%EctoShorts.TestPost{title: "Post #{i}"})
      end

      query = from(p in EctoShorts.TestPost, limit: 10, order_by: [asc: p.id])
      results = TestRepo.all(query)

      assert 10 = length(results)
    end

    test "Rule Statement 43: first N records" do
      # Given: [first: 10]
      # Expected: first: 10

      for i <- 1..15 do
        {:ok, _} = TestRepo.insert(%EctoShorts.TestPost{title: "Post #{i}"})
      end

      query = from(p in EctoShorts.TestPost, limit: 10)
      results = TestRepo.all(query)

      assert 10 = length(results)
    end

    test "Rule Statement 45: reverse order" do
      # Given: [reverse_order: true]
      # Expected: reverse_order: true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Alpha"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Beta"})

      results_reversed =
        EctoShorts.TestPost
        |> order_by([p], asc: p.title)
        |> select([p], p.title)
        |> reverse_order()
        |> TestRepo.all()

      assert ["Beta", "Alpha"] = results_reversed
    end

    test "Rule Statement 46: exclude order_by" do
      # Given: [exclude: :order_by]
      # Expected: exclude: :order_by

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Beta"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Alpha"})

      query =
        EctoShorts.TestPost
        |> order_by([p], asc: p.title)
        |> select([p], p.title)
        |> exclude(:order_by)

      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 47: exclude order_by and limit" do
      # Given: [exclude: [:order_by, :limit]]
      # Expected: exclude: [:order_by, :limit]

      for i <- 1..5 do
        {:ok, _} = TestRepo.insert(%EctoShorts.TestPost{title: "Post #{i}"})
      end

      query =
        EctoShorts.TestPost
        |> order_by([p], asc: p.title)
        |> limit(2)
        |> exclude(:order_by)
        |> exclude(:limit)

      results = TestRepo.all(query)

      assert 5 = length(results)
    end

    test "Rule Statement 48: put_query_prefix single tenant" do
      # Given: [put_query_prefix: "tenant_a"]
      # Expected: put_query_prefix: "tenant_a"

      query =
        EctoShorts.TestPost
        |> select([p], p.title)
        |> put_query_prefix("tenant_a")

      assert "tenant_a" == query.prefix
    end

    test "Rule Statement 49: put_query_prefix last wins" do
      # Given: [put_query_prefix: "tenant_a", put_query_prefix: "tenant_b"]
      # Expected: put_query_prefix: "tenant_b"

      query =
        EctoShorts.TestPost
        |> select([p], p.title)
        |> put_query_prefix("tenant_a")
        |> put_query_prefix("tenant_b")

      assert "tenant_b" == query.prefix
    end
  end

  describe "Set Directives" do
    test "Rule Statement 1: except with filter params" do
      # Given: [except: [published: false]]
      # Expected: except: from(p in EctoShorts.TestPost, where: p.published == false)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      except_query = from(p in EctoShorts.TestPost, where: p.published == false)
      query = EctoShorts.TestPost |> except(^except_query)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 2: except with raw query" do
      # Given: [except: from(p in EctoShorts.TestPost, where: p.published == ^false)]
      # Expected: except: from(p in EctoShorts.TestPost, where: p.published == ^false)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      except_query = from(p in EctoShorts.TestPost, where: p.published == ^false)
      query = EctoShorts.TestPost |> except(^except_query)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 3: except_all with filter params" do
      # Given: [except_all: [published: false]]
      # Expected: except_all: from(p in EctoShorts.TestPost, where: p.published == false)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      except_query = from(p in EctoShorts.TestPost, where: p.published == false)
      query = EctoShorts.TestPost |> except_all(^except_query)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 4: intersect with filter params" do
      # Given: [intersect: [published: false]]
      # Expected: intersect: from(p in EctoShorts.TestPost, where: p.published == false)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      intersect_query = from(p in EctoShorts.TestPost, where: p.published == false)
      query = EctoShorts.TestPost |> intersect(^intersect_query)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Draft"}] = results
    end

    test "Rule Statement 5: intersect_all with filter params" do
      # Given: [intersect_all: [published: false]]
      # Expected: intersect_all: from(p in EctoShorts.TestPost, where: p.published == false)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      intersect_query = from(p in EctoShorts.TestPost, where: p.published == false)
      query = EctoShorts.TestPost |> intersect_all(^intersect_query)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Draft"}] = results
    end

    test "Rule Statement 6: union with filter params" do
      # Given: [union: [published: false]]
      # Expected: union: from(p in EctoShorts.TestPost, where: p.published == false)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query1 = EctoShorts.TestPost |> where([p], p.published == true)
      query2 = EctoShorts.TestPost |> where([p], p.published == false)
      query = query1 |> union(^query2)
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 7: union_all with filter params" do
      # Given: [union_all: [published: false]]
      # Expected: union_all: from(p in EctoShorts.TestPost, where: p.published == false)

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      query1 = EctoShorts.TestPost |> where([p], p.published == true)
      query2 = EctoShorts.TestPost |> where([p], p.published == false)
      query = query1 |> union_all(^query2)
      results = TestRepo.all(query)

      assert 2 = length(results)
    end
  end

  describe "Lock Directives" do
    test "Rule Statement 1: lock for update" do
      # Given: [lock: fn query -> from(p in query, lock: "FOR UPDATE") end]
      # Expected: lock: "FOR UPDATE"

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, lock: "FOR UPDATE")
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 2: lock for share" do
      # Given: [lock: [name: :for_share]]
      # Expected: lock: "FOR SHARE"

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, lock: "FOR SHARE")
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 3: lock with values" do
      # Given: [lock: [name: :for_update_with_clause, values: [clause: "SKIP LOCKED"]]]
      # Expected: lock: "FOR UPDATE SKIP LOCKED"

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost, lock: "FOR UPDATE SKIP LOCKED")
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end
  end

  describe "CTE Directives" do
    test "Rule Statement 1: recursive_ctes true" do
      # Given: [recursive_ctes: true]
      # Expected: recursive_ctes: true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p)

      query =
        EctoShorts.TestPost
        |> recursive_ctes(true)
        |> with_cte("published_posts", as: ^cte_query)

      results = TestRepo.all(query)

      assert is_list(results)
    end

    test "Rule Statement 2: recursive_ctes false" do
      # Given: [recursive_ctes: false]
      # Expected: recursive_ctes: false

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      query = from(p in EctoShorts.TestPost)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post"}] = results
    end

    test "Rule Statement 3: with_cte basic" do
      # Given: [with_cte: [published_posts: [as: cte_query]]]
      # Expected: with_cte: [published_posts: [as: cte_query]]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p)

      query =
        EctoShorts.TestPost
        |> from(as: :post)
        |> with_cte("published_posts", as: ^cte_query)
        |> where([p], p.published == true)

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 4: with_cte not materialized" do
      # Given: [with_cte: [published_posts: [as: cte_query, materialized: false, operation: :all]]]
      # Expected: with_cte: [published_posts: [as: cte_query, materialized: false]]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})

      cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p)

      query =
        EctoShorts.TestPost
        |> with_cte("published_posts", as: ^cte_query, materialized: false)
        |> where([p], p.published == true)

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 5: with_cte from filter params published true" do
      # Given: [with_cte: [published_posts: [as: [from: [query: Post, published: true]]]]]
      # Expected: with_cte: [published_posts: [as: from(p in EctoShorts.TestPost, where: p.published == true)]]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p)

      query =
        EctoShorts.TestPost
        |> with_cte("published_posts", as: ^cte_query)
        |> where([p], p.published == true)

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 6: with_cte from filter params id" do
      # Given: [with_cte: [target_post: [as: [from: [query: Post, id: 1]]]]]
      # Expected: with_cte: [target_post: [as: from(p in EctoShorts.TestPost, where: p.id == 1)]]

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})

      cte_query = from(p in EctoShorts.TestPost, where: p.id == ^post1.id, select: p)

      query =
        EctoShorts.TestPost
        |> with_cte("target_post", as: ^cte_query)
        |> where([p], p.id == ^post1.id)

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 7: recursive_ctes with cte" do
      # Given: [recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]]
      # Expected: recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})

      cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p)

      query =
        EctoShorts.TestPost
        |> recursive_ctes(true)
        |> with_cte("published_posts", as: ^cte_query)
        |> where([p], p.published == true)

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end
  end

  describe "Named Binding Directives" do
    test "Rule Statement 1: with_named_binding single named binding" do
      # Given: [with_named_binding: [author: [join: [association: [source: :author, as: :author]]]]]
      # Expected: with_named_binding: [author: join]

      {:ok, author1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, author2} = TestRepo.insert(%EctoShorts.TestUser{first_name: "Jane"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", author_id: author1.id})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", author_id: author2.id})

      query =
        from(p in EctoShorts.TestPost,
          join: a in assoc(p, :author),
          as: :author,
          where: as(:author).first_name == "John"
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match"}] = results
    end

    test "Rule Statement 2: with_named_binding multiple named bindings" do
      # Given: [with_named_binding: [author: [join: [association: [source: :author, as: :author]]], users_table: [join: [table: [source: "users", as: :users_table, on: true]]]]]
      # Expected: with_named_binding: [author: join, users_table: join]

      {:ok, author1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, author2} = TestRepo.insert(%EctoShorts.TestUser{first_name: "Jane"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", author_id: author1.id})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", author_id: author2.id})

      query =
        from(p in EctoShorts.TestPost,
          join: a in assoc(p, :author),
          as: :author,
          join: u in "users",
          as: :users_table,
          on: u.id == p.author_id,
          where: as(:author).first_name == "John",
          select: {p.title, field(u, :first_name)}
        )

      results = TestRepo.all(query)

      assert [{"Match", "John"}] = results
    end
  end

  describe "Query Modifier Directives" do
    test "Rule Statement 1: with_ties true" do
      # Given: [with_ties: true]
      # Expected: with_ties: true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1", views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2", views: 10})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 3", views: 5})

      query =
        from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1)
        |> with_ties(true)

      results = TestRepo.all(query)

      assert 2 = length(results)
      assert Enum.sort(Enum.map(results, & &1.title)) == ["Post 1", "Post 2"]
    end

    test "Rule Statement 2: with_ties false" do
      # Given: [with_ties: false]
      # Expected: with_ties: false

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1", views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2", views: 10})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 3", views: 5})

      query =
        from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1)
        |> with_ties(false)

      results = TestRepo.all(query)

      assert 1 = length(results)
    end

    test "Rule Statement 3: with_ties named binding" do
      # Given: [with_ties: [bind: [as: :post, value: true]]]
      # Expected: with_ties: [bind: [as: :post, value: true]]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1", views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2", views: 10})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 3", views: 5})

      query =
        from(p in EctoShorts.TestPost, as: :post, order_by: [desc: p.views], limit: 1)
        |> with_ties(true)

      results = TestRepo.all(query)

      assert 2 = length(results)
      assert Enum.sort(Enum.map(results, & &1.title)) == ["Post 1", "Post 2"]
    end

    test "Rule Statement 4: with_ties positional binding" do
      # Given: [with_ties: [bind: [at: 1, value: true]]]
      # Expected: with_ties: [bind: [at: 1, value: true]]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1", views: 10})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2", views: 10})
      {:ok, _post3} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 3", views: 5})

      query =
        from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1)
        |> with_ties(true)

      results = TestRepo.all(query)

      assert 2 = length(results)
      assert Enum.sort(Enum.map(results, & &1.title)) == ["Post 1", "Post 2"]
    end

    test "Rule Statement 5: update set and inc" do
      # Given: [update: [set: [title: "After"], inc: [views: 1]]]
      # Expected: update: [set: [title: "After"], inc: [views: 1]]

      {:ok, post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Before", views: 5})

      query =
        from(p in EctoShorts.TestPost,
          where: p.id == ^post1.id,
          update: [set: [title: "After"], inc: [views: 1]]
        )

      TestRepo.update_all(query, [])

      updated = TestRepo.get!(EctoShorts.TestPost, post1.id)
      assert updated.title == "After"
      assert updated.views == 6
    end

    test "Rule Statement 6: windows partition_by and order_by" do
      # Given: [windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]]
      # Expected: windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 2"})

      query =
        from(p in EctoShorts.TestPost,
          windows: [post_window: [partition_by: p.author_id, order_by: [desc: p.inserted_at]]],
          select: {p.title, over(count(p.id), :post_window)}
        )

      results = TestRepo.all(query)

      assert is_list(results)
    end

    test "Rule Statement 7: preload atom" do
      # Given: [preload: :author]
      # Expected: preload: [:author]

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", author_id: author.id})

      query = from(p in EctoShorts.TestPost, where: p.title == "Post", preload: :author, select: p)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post", author: %EctoShorts.TestUser{first_name: "John"}}] = results
    end

    test "Rule Statement 8: preload list with atom" do
      # Given: [preload: [:author]]
      # Expected: preload: [:author]

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", author_id: author.id})

      query = from(p in EctoShorts.TestPost, where: p.title == "Post", preload: [:author], select: p)
      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post", author: %EctoShorts.TestUser{first_name: "John"}}] = results
    end

    test "Rule Statement 9: preload nested associations" do
      # Given: [preload: [author: [:posts]]]
      # Expected: preload: [author: [:posts]]

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", author_id: author.id})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Nested Post", author_id: author.id})

      query =
        from(p in EctoShorts.TestPost,
          where: p.title == "Post",
          preload: [author: [:posts]],
          select: p
        )

      results = TestRepo.all(query)

      assert [
               %EctoShorts.TestPost{
                 title: "Post",
                 author: %EctoShorts.TestUser{first_name: "John"} = loaded_author
               }
             ] = results

      assert Enum.sort(Enum.map(loaded_author.posts, & &1.title)) == ["Nested Post", "Post"]
    end

    test "Rule Statement 10: preload from named binding" do
      # Given: [preload: [bind: [as: :example, value: :author]]]
      # Expected: preload: [author: binding]

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1", author_id: author.id})

      query =
        from(p in EctoShorts.TestPost,
          where: p.title == "Post 1",
          join: a in assoc(p, :author),
          as: :example,
          preload: [author: a],
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1", author: %EctoShorts.TestUser{first_name: "John"}}] =
               results
    end

    test "Rule Statement 11: preload from positional binding" do
      # Given: [preload: [bind: [at: 2, value: :author]]]
      # Expected: preload: [author: binding]

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1", author_id: author.id})

      query =
        from(p in EctoShorts.TestPost,
          where: p.title == "Post 1",
          join: a in assoc(p, :author),
          preload: [author: a],
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1", author: %EctoShorts.TestUser{first_name: "John"}}] =
               results
    end

    test "Rule Statement 12: preload from binding and nested" do
      # Given: [preload: [bind: [at: 2, value: :author], posts: [:comments]]]
      # Expected: preload: [author: {binding, [posts: [:comments]]}]

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", author_id: author.id})
      {:ok, nested_post} = TestRepo.insert(%EctoShorts.TestPost{title: "Nested Post", author_id: author.id})
      {:ok, _comment} = TestRepo.insert(%EctoShorts.TestComment{post_id: nested_post.id, body: "hi"})

      query =
        from(p in EctoShorts.TestPost,
          where: p.title == "Post",
          join: a in assoc(p, :author),
          preload: [author: {a, [posts: [:comments]]}],
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post", author: %EctoShorts.TestUser{} = loaded_author}] = results
      assert Enum.sort(Enum.map(loaded_author.posts, & &1.title)) == ["Nested Post", "Post"]
      assert Enum.all?(loaded_author.posts, &Ecto.assoc_loaded?(&1.comments))
    end

    test "Rule Statement 13: preload from named binding and nested" do
      # Given: [preload: [bind: [as: :author, value: :author], posts: [:comments]]]
      # Expected: preload: [author: {binding, [posts: [:comments]]}]

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", author_id: author.id})
      {:ok, nested_post} = TestRepo.insert(%EctoShorts.TestPost{title: "Nested Post", author_id: author.id})
      {:ok, _comment} = TestRepo.insert(%EctoShorts.TestComment{post_id: nested_post.id, body: "hi"})

      query =
        from(p in EctoShorts.TestPost,
          where: p.title == "Post",
          join: a in assoc(p, :author),
          as: :author,
          preload: [author: {a, [posts: [:comments]]}],
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post", author: %EctoShorts.TestUser{} = loaded_author}] = results
      assert Enum.sort(Enum.map(loaded_author.posts, & &1.title)) == ["Nested Post", "Post"]
      assert Enum.all?(loaded_author.posts, &Ecto.assoc_loaded?(&1.comments))
    end

    test "Rule Statement 14: preload from multiple bindings and nested" do
      # Given: [preload: [bind: [[as: :author, value: :author], [at: 2, value: :author]], posts: [:comments]]]
      # Expected: preload: [author: {binding, [posts: [:comments]]}]

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", author_id: author.id})
      {:ok, nested_post} = TestRepo.insert(%EctoShorts.TestPost{title: "Nested Post", author_id: author.id})
      {:ok, _comment} = TestRepo.insert(%EctoShorts.TestComment{post_id: nested_post.id, body: "hi"})

      query =
        from(p in EctoShorts.TestPost,
          where: p.title == "Post",
          join: a in assoc(p, :author),
          as: :author,
          preload: [author: {a, [posts: [:comments]]}],
          preload: [author: {a, [posts: [:comments]]}],
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post", author: %EctoShorts.TestUser{} = loaded_author}] = results
      assert Enum.sort(Enum.map(loaded_author.posts, & &1.title)) == ["Nested Post", "Post"]
      assert Enum.all?(loaded_author.posts, &Ecto.assoc_loaded?(&1.comments))
    end
  end

  describe "Join Directives" do
    test "Rule Statement 1: association join with named binding and filter" do
      # Given: [author: [as: :author, first_name: "John"]]
      # Expected: join: author, as: :author, where: as(:author).first_name == "John"

      {:ok, author1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, author2} = TestRepo.insert(%EctoShorts.TestUser{first_name: "Jane"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", author_id: author1.id})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", author_id: author2.id})

      query =
        from(p in EctoShorts.TestPost,
          join: a in assoc(p, :author),
          as: :author,
          where: as(:author).first_name == "John",
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match"}] = results
    end

    test "Rule Statement 2: association join without named binding" do
      # Given: [author: [first_name: "John"]]
      # Expected: join: author, where: a.first_name == "John"

      {:ok, author1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, author2} = TestRepo.insert(%EctoShorts.TestUser{first_name: "Jane"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", author_id: author1.id})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", author_id: author2.id})

      query =
        from(p in EctoShorts.TestPost,
          join: a in assoc(p, :author),
          where: a.first_name == "John",
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match"}] = results
    end

    test "Rule Statement 3: association join with explicit on condition" do
      # Given: [author: [as: :author, on: true, first_name: "John"]]
      # Expected: join: author, as: :author, on: true, where: as(:author).first_name == "John"

      {:ok, author1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, author2} = TestRepo.insert(%EctoShorts.TestUser{first_name: "Jane"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", author_id: author1.id})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", author_id: author2.id})

      query =
        from(p in EctoShorts.TestPost,
          join: a in assoc(p, :author),
          as: :author,
          on: true,
          where: as(:author).first_name == "John",
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match"}] = results
    end

    test "Rule Statement 4: association join with left join qualifier" do
      # Given: [author: [as: :author, type: :left, first_name: "John"]]
      # Expected: left_join: author, as: :author, where: as(:author).first_name == "John"

      {:ok, author1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, author2} = TestRepo.insert(%EctoShorts.TestUser{first_name: "Jane"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Match", author_id: author1.id})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "No Match", author_id: author2.id})

      query =
        from(p in EctoShorts.TestPost,
          left_join: a in assoc(p, :author),
          as: :author,
          where: as(:author).first_name == "John",
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Match"}] = results
    end

    test "Rule Statement 5: canonical association join" do
      # Given: [join: [author: [as: :author]]]
      # Expected: join: author, as: :author

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post 1", author_id: author.id})

      query =
        from(p in EctoShorts.TestPost,
          join: a in assoc(p, :author),
          as: :author,
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Post 1"}] = results
    end

    test "Rule Statement 6: canonical schema join" do
      # Given: [join: [schema: [source: User, as: :user_join, on: true]]]
      # Expected: join: User, as: :user_join, on: true

      {:ok, user} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", author_id: user.id})

      query =
        from(p in EctoShorts.TestPost,
          join: u in EctoShorts.TestUser,
          as: :user_join,
          on: true,
          select: {p.title, u.first_name}
        )

      results = TestRepo.all(query)

      assert [{"Post", "John"}] = results
    end

    test "Rule Statement 7: canonical table join" do
      # Given: [join: [table: [source: "users", as: :users_table, on: true]]]
      # Expected: join: "users", as: :users_table, on: true

      {:ok, user} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", author_id: user.id})

      query =
        from(p in EctoShorts.TestPost,
          join: u in "users",
          as: :users_table,
          on: true,
          select: {p.title, field(u, :first_name)}
        )

      results = TestRepo.all(query)

      assert [{"Post", "John"}] = results
    end

    test "Rule Statement 8: canonical query join" do
      # Given: [join: [query: [source: user_query, as: :named_users, on: true]]]
      # Expected: join: user_query, as: :named_users, on: true

      {:ok, _user1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      user_query = from(u in EctoShorts.TestUser, where: u.first_name == "John")

      query =
        from(p in EctoShorts.TestPost,
          join: u in ^user_query,
          as: :named_users,
          on: true,
          select: {p.title, u.first_name}
        )

      results = TestRepo.all(query)

      assert [{"Post", "John"}] = results
    end

    test "Rule Statement 9: canonical subquery join" do
      # Given: [join: [subquery: [source: user_query, as: :named_users_subquery, on: true]]]
      # Expected: join: subquery(user_query), as: :named_users_subquery, on: true

      {:ok, _user1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      user_query = from(u in EctoShorts.TestUser, where: u.first_name == "John")

      query =
        from(p in EctoShorts.TestPost,
          join: u in subquery(user_query),
          as: :named_users_subquery,
          on: true,
          select: {p.title, u.first_name}
        )

      results = TestRepo.all(query)

      assert [{"Post", "John"}] = results
    end

    test "Rule Statement 10: canonical subquery join from filter params with explicit from" do
      # Given: [join: [subquery: [source: [from: [query: User, first_name: "John"]], as: :named_users_subquery, on: true]]]
      # Expected: join: subquery(from u in User, where: u.first_name == "John"), as: :named_users_subquery, on: true

      {:ok, _user1} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post"})

      named_users_query = from(u in EctoShorts.TestUser, where: u.first_name == "John")

      query =
        from(p in EctoShorts.TestPost,
          join: u in subquery(named_users_query),
          as: :named_users_subquery,
          on: true,
          select: {p.title, u.first_name}
        )

      results = TestRepo.all(query)

      assert [{"Post", "John"}] = results
    end

    test "Rule Statement 11: canonical subquery join from current schema filter params" do
      # Given: [join: [subquery: [source: [from: [published: true]], as: :published_posts_subquery, on: true]]]
      # Expected: join: subquery(from p in Post, where: p.published == true), as: :published_posts_subquery, on: true

      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%EctoShorts.TestPost{title: "Draft", published: false})

      pub_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p.id)

      query =
        from(p in EctoShorts.TestPost,
          join: s in subquery(pub_query),
          as: :published_posts_subquery,
          on: s.id == p.id,
          select: p
        )

      results = TestRepo.all(query)

      assert [%EctoShorts.TestPost{title: "Published"}] = results
    end

    test "Rule Statement 12: canonical fragment join" do
      # Given: [join: [fragment: [source: [name: :active_posts, values: [min_views: 0]], as: :active_posts, on: true]]]
      # Expected: join: fragment source query, as: :active_posts, on: true
      # Note: fragment join validates SQL shape

      active_posts_query =
        from(ap in fragment("SELECT id FROM posts WHERE views >= ?", ^0),
          select: %{id: field(ap, :id)}
        )

      query =
        from(p in EctoShorts.TestPost,
          join: ap in ^active_posts_query,
          as: :active_posts,
          on: ap.id == p.id,
          select: p
        )

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, TestRepo, query)

      assert sql =~ "INNER JOIN (SELECT id FROM posts WHERE views >="
      assert is_list(params)
    end

    test "Rule Statement 13: canonical fragment join with hints" do
      # Given: [join: [fragment: [source: [name: :active_posts, values: [min_views: 0]], hints: :test_index, as: :active_posts, on: true]]]
      # Expected: join with hints applied
      # Note: fragment join validates SQL shape

      active_posts_query =
        from(ap in fragment("SELECT id FROM posts WHERE views >= ?", ^0),
          select: %{id: field(ap, :id)}
        )

      query =
        from(p in EctoShorts.TestPost,
          join: ap in ^active_posts_query,
          as: :active_posts,
          on: ap.id == p.id,
          hints: ["USE INDEX(test_index)"],
          select: p
        )

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, TestRepo, query)

      assert sql =~ "INNER JOIN (SELECT id FROM posts WHERE views >="
      assert is_list(params)
      assert Enum.at(query.joins, 0).hints == ["USE INDEX(test_index)"]
    end

    test "Rule Statement 14: canonical multiple joins" do
      # Given: [join: [author: [as: :author], table: [source: "users", as: :users_table, on: true]]]
      # Expected: multiple joins applied in order

      {:ok, author} = TestRepo.insert(%EctoShorts.TestUser{first_name: "John"})
      {:ok, _post1} = TestRepo.insert(%EctoShorts.TestPost{title: "Post", author_id: author.id})

      query =
        from(p in EctoShorts.TestPost,
          join: a in assoc(p, :author),
          as: :author,
          join: u in "users",
          as: :users_table,
          on: true,
          select: {p.title, a.first_name, field(u, :first_name)}
        )

      results = TestRepo.all(query)

      assert [{"Post", "John", "John"}] = results
    end
  end
end
