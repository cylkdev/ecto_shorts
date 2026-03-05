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

      {:ok, _post1} = TestRepo.insert(%Post{title: "Elixir Post", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Ruby Post", tags: ["ruby"]})

      query = from(p in Post, where: "elixir" in p.tags)
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 2: tags equals operator with single value" do
      # Given: [tags: [==: "elixir"]]
      # Expected: "elixir" in p.tags

      {:ok, _post1} = TestRepo.insert(%Post{title: "Elixir Post", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Ruby Post", tags: ["ruby"]})

      query = from(p in Post, where: "elixir" in p.tags)
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 3: tags not equals single value" do
      # Given: [tags: [!=: "elixir"]]
      # Expected: not ("elixir" in p.tags)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Elixir Post", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Ruby Post", tags: ["ruby"]})

      query = from(p in Post, where: not ("elixir" in p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["ruby"]}] = results
    end

    test "Rule Statement 4: tags in operator with single value" do
      # Given: [tags: [in: "elixir"]]
      # Expected: "elixir" in p.tags

      {:ok, _post1} = TestRepo.insert(%Post{title: "Elixir Post", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Ruby Post", tags: ["ruby"]})

      query = from(p in Post, where: "elixir" in p.tags)
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 5: tags equals array" do
      # Given: [tags: ["elixir", "erlang"]]
      # Expected: p.tags == ["elixir", "erlang"]

      {:ok, _post1} = TestRepo.insert(%Post{title: "Exact Match", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Different", tags: ["elixir"]})

      query = from(p in Post, where: p.tags == ^["elixir", "erlang"])
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 6: tags equals operator with array" do
      # Given: [tags: [==: ["elixir", "erlang"]]]
      # Expected: p.tags == ["elixir", "erlang"]

      {:ok, _post1} = TestRepo.insert(%Post{title: "Exact Match", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Different", tags: ["elixir"]})

      query = from(p in Post, where: p.tags == ^["elixir", "erlang"])
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 7: tags not equals array" do
      # Given: [tags: [!=: ["elixir"]]]
      # Expected: p.tags != ["elixir"]

      {:ok, _post1} = TestRepo.insert(%Post{title: "Exact Match", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Different", tags: ["elixir", "erlang"]})

      query = from(p in Post, where: p.tags != ^["elixir"])
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 8: tags is nil" do
      # Given: [tags: nil]
      # Expected: p.tags == nil

      {:ok, _post1} = TestRepo.insert(%Post{title: "No Tags", tags: nil})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Has Tags", tags: ["elixir"]})

      query = from(p in Post, where: is_nil(p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: nil}] = results
    end

    test "Rule Statement 9: tags equals nil" do
      # Given: [tags: [==: nil]]
      # Expected: p.tags == nil

      {:ok, _post1} = TestRepo.insert(%Post{title: "No Tags", tags: nil})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Has Tags", tags: ["elixir"]})

      query = from(p in Post, where: is_nil(p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: nil}] = results
    end

    test "Rule Statement 10: tags not equals nil" do
      # Given: [tags: [!=: nil]]
      # Expected: p.tags != nil

      {:ok, _post1} = TestRepo.insert(%Post{title: "No Tags", tags: nil})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Has Tags", tags: ["elixir"]})

      query = from(p in Post, where: not is_nil(p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir"]}] = results
    end

    test "Rule Statement 11: tags array overlaps" do
      # Given: [tags: [in: ["elixir"]]]
      # Expected: fragment("? && ?", p.tags, ["elixir"])

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Elixir", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "No Elixir", tags: ["ruby"]})

      query = from(p in Post, where: fragment("? && ?", p.tags, ^["elixir"]))
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 12: tags array contains all" do
      # Given: [tags: [all: [in: ["elixir", "erlang"]]]]
      # Expected: fragment("? <@ ?", p.tags, ["elixir", "erlang"])

      {:ok, _post1} = TestRepo.insert(%Post{title: "Subset", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Superset", tags: ["elixir", "erlang", "ruby"]})

      query = from(p in Post, where: fragment("? <@ ?", p.tags, ^["elixir", "erlang"]))
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir"]}] = results
    end

    test "Rule Statement 13: negated tags equals array" do
      # Given: [not: [tags: [==: ["elixir"]]]]
      # Expected: not (p.tags == ["elixir"])

      {:ok, _post1} = TestRepo.insert(%Post{title: "Exact Match", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Different", tags: ["elixir", "erlang"]})

      query = from(p in Post, where: not (p.tags == ^["elixir"]))
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 14: negated tags contains value" do
      # Given: [not: [tags: [in: "elixir"]]]
      # Expected: not ("elixir" in p.tags)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Elixir", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "No Elixir", tags: ["ruby"]})

      query = from(p in Post, where: not ("elixir" in p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["ruby"]}] = results
    end

    test "Rule Statement 15: tags any greater than" do
      # Given: [tags: [>: "elixir"]]
      # Expected: "elixir" > ANY(p.tags) — any tag is less than "elixir"

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Smaller", tags: ["erlang", "c"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "All Greater", tags: ["ruby", "python"]})

      query = from(p in Post, where: fragment("? > ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["erlang", "c"]}] = results
    end

    test "Rule Statement 16: tags any greater than or equal" do
      # Given: [tags: [>=: "elixir"]]
      # Expected: "elixir" >= ANY(p.tags) — any tag is <= "elixir"

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has LTE", tags: ["elixir", "c"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "All Greater", tags: ["ruby", "python"]})

      query = from(p in Post, where: fragment("? >= ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "c"]}] = results
    end

    test "Rule Statement 17: tags any less than" do
      # Given: [tags: [<: "elixir"]]
      # Expected: "elixir" < ANY(p.tags) — any tag is greater than "elixir"

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Greater", tags: ["ruby", "python"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "All Smaller", tags: ["c", "d"]})

      query = from(p in Post, where: fragment("? < ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["ruby", "python"]}] = results
    end

    test "Rule Statement 18: tags any less than or equal" do
      # Given: [tags: [<=: "elixir"]]
      # Expected: "elixir" <= ANY(p.tags) — any tag is >= "elixir"

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has GTE", tags: ["elixir", "ruby"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "All Smaller", tags: ["c", "d"]})

      query = from(p in Post, where: fragment("? <= ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "ruby"]}] = results
    end

    test "Rule Statement 19: tags any like" do
      # Given: [tags: [like: "elixir"]]
      # Expected: fragment("? LIKE ?", any(p.tags), "elixir")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Match", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "No Match", tags: ["ruby"]})

      query = from(p in Post, where: fragment("? LIKE ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 20: tags any ilike" do
      # Given: [tags: [ilike: "elixir"]]
      # Expected: fragment("? ILIKE ?", any(p.tags), "elixir")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Match", tags: ["ELIXIR", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "No Match", tags: ["ruby"]})

      query = from(p in Post, where: fragment("? ILIKE ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["ELIXIR", "erlang"]}] = results
    end

    test "Rule Statement 21: tags any like any" do
      # Given: [tags: [like: ["elixir", "erlang"]]]
      # Expected: p.tags && ["elixir", "erlang"] (array overlap — tags contains any element from the list)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Match", tags: ["elixir", "ruby"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "No Match", tags: ["python"]})

      query = from(p in Post, where: fragment("? && ?", p.tags, ^["elixir", "erlang"]))
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "ruby"]}] = results
    end

    test "Rule Statement 22: negated tags any like" do
      # Given: [not: [tags: [like: "elixir"]]]
      # Expected: not (fragment("? LIKE ?", any(p.tags), "elixir"))

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Match", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "No Match", tags: ["ruby"]})

      query = from(p in Post, where: not fragment("? LIKE ANY(?)", ^"elixir", p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["ruby"]}] = results
    end

    test "Rule Statement 23: tags contains lowercased value" do
      # Given: [tags: [==: [lower: "elixir"]]]
      # Expected: lower("elixir") in p.tags

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Lower", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Has Upper", tags: ["ELIXIR"]})

      query = from(p in Post, where: fragment("lower(?)", "elixir") in p.tags)
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 24: tags not contains uppercased value" do
      # Given: [tags: [!=: [upper: "ELIXIR"]]]
      # Expected: not (upper("ELIXIR") in p.tags)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Lower", tags: ["elixir", "erlang"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Has Upper", tags: ["ELIXIR"]})

      query = from(p in Post, where: not (fragment("upper(?)", "ELIXIR") in p.tags))
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir", "erlang"]}] = results
    end

    test "Rule Statement 25: tags count greater than" do
      # Given: [tags: [count: [>: 0]]]
      # Expected: fragment("array_length(?, 1)", p.tags) > 0

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Tags", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Empty Tags", tags: []})

      query = from(p in Post, where: fragment("array_length(?, 1)", p.tags) > 0)
      results = TestRepo.all(query)

      assert [%Post{tags: ["elixir"]}] = results
    end

    test "Rule Statement 26: tags count equals zero" do
      # Given: [tags: [count: [==: 0]]]
      # Expected: fragment("array_length(?, 1)", p.tags) == 0

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Tags", tags: ["elixir"]})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Empty Tags", tags: []})

      query = from(p in Post, where: fragment("coalesce(array_length(?, 1), 0)", p.tags) == 0)
      results = TestRepo.all(query)

      assert [%Post{tags: []}] = results
    end
  end

  describe "Scalar Fields" do
    test "Rule Statement 1: id equals value" do
      # Given: [id: 1]
      # Expected: p.id == 1

      {:ok, post1} = TestRepo.insert(%Post{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Post 2"})

      query = from(p in Post, where: p.id == ^post1.id)
      results = TestRepo.all(query)

      assert [%Post{title: "Post 1"}] = results
    end

    test "Rule Statement 2: published equals boolean" do
      # Given: [published: true]
      # Expected: p.published == true

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Unpublished", published: false})

      query = from(p in Post, where: p.published == true)
      results = TestRepo.all(query)

      assert [%Post{published: true}] = results
    end

    test "Rule Statement 3: multiple scalar fields with and" do
      # Given: [id: 1, published: true]
      # Expected: p.id == 1 and p.published == true

      {:ok, post1} = TestRepo.insert(%Post{title: "Match", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "No Match", published: false})

      query = from(p in Post, where: p.id == ^post1.id and p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "Match", published: true}] = results
    end

    test "Rule Statement 4: published_at is nil" do
      # Given: [published_at: nil]
      # Expected: is_nil(p.published_at)

      {:ok, _post1} = TestRepo.insert(%Post{title: "No Date", published_at: nil})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Has Date", published_at: ~U[2026-01-01 00:00:00Z]})

      query = from(p in Post, where: is_nil(p.published_at))
      results = TestRepo.all(query)

      assert [%Post{published_at: nil}] = results
    end

    test "Rule Statement 5: published in list" do
      # Given: [published: [true, false]]
      # Expected: p.published in [true, false]

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Unpublished", published: false})

      query = from(p in Post, where: p.published in [true, false])
      results = TestRepo.all(query)

      assert [%Post{published: false}, %Post{published: true}] =
               Enum.sort_by(results, & &1.published)
    end

    test "Rule Statement 6: title equals string" do
      # Given: [title: "hello"]
      # Expected: p.title == "hello"

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: p.title == "hello")
      results = TestRepo.all(query)

      assert [%Post{title: "hello"}] = results
    end

    test "Rule Statement 7: explicit and with single condition" do
      # Given: [and: [[id: 1]]]
      # Expected: p.id == 1

      {:ok, post1} = TestRepo.insert(%Post{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Post 2"})

      query = from(p in Post, where: p.id == ^post1.id)
      results = TestRepo.all(query)

      assert [%Post{title: "Post 1"}] = results
    end

    test "Rule Statement 8: explicit and with multiple conditions" do
      # Given: [and: [[id: 1], [published: true]]]
      # Expected: p.id == 1 and p.published == true

      {:ok, post1} = TestRepo.insert(%Post{title: "Match", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "No Match", published: false})

      query = from(p in Post, where: p.id == ^post1.id and p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "Match", published: true}] = results
    end

    test "Rule Statement 9: explicit and with map conditions" do
      # Given: [and: [id: 1, title: "hello"]]
      # Expected: p.id == 1 and p.title == "hello"

      {:ok, post1} = TestRepo.insert(%Post{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: p.id == ^post1.id and p.title == "hello")
      results = TestRepo.all(query)

      assert [%Post{title: "hello"}] = results
    end

    test "Rule Statement 10: nested and conditions" do
      # Given: [and: [[id: 1, title: "hello"], [published: true]]]
      # Expected: (p.id == 1 and p.title == "hello") and p.published == true

      {:ok, post1} = TestRepo.insert(%Post{title: "hello", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "hello", published: false})

      query = from(p in Post, where: (p.id == ^post1.id and p.title == "hello") and p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "hello", published: true}] = results
    end

    test "Rule Statement 11: or with map conditions" do
      # Given: [or: [[id: 1, title: "hello"], [published: true]]]
      # Expected: (p.id == 1 and p.title == "hello") or p.published == true

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello", published: false})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world", published: true})
      {:ok, _post3} = TestRepo.insert(%Post{title: "world", published: false})

      query = from(p in Post, where: (p.title == "hello") or p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "hello", published: false}, %Post{title: "world", published: true}] =
               Enum.sort_by(results, & {&1.title, &1.published})
    end

    test "Rule Statement 12: nested or within and" do
      # Given: [or: [[id: 1, or: [title: "hello", body: "world"]], [published: true]]]
      # Expected: (p.id == 1 and (p.title == "hello" or p.body == "world")) or p.published == true

      {:ok, post1} = TestRepo.insert(%Post{title: "hello", body: "test", published: false})
      {:ok, _post2} = TestRepo.insert(%Post{title: "test", body: "world", published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "test", body: "test", published: true})
      {:ok, _post4} = TestRepo.insert(%Post{title: "test", body: "test", published: false})

      query = from(p in Post, where: (p.id == ^post1.id and (p.title == "hello" or p.body == "world")) or p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "hello", published: false}, %Post{title: "test", published: true}] =
               Enum.sort_by(results, & {&1.title, &1.published})
    end
  end

  describe "Negation Directives" do
    test "Rule Statement 1: not published in list" do
      # Given: [not: [published: [in: [true, false]]]]
      # Expected: is_nil(p.published) or not (p.published in [true, false])
      # Note: NULL NOT IN (...) is NULL/falsy in SQL, so must handle nil explicitly

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Unpublished", published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Null", published: nil})

      query = from(p in Post, where: is_nil(p.published) or p.published not in [true, false])
      results = TestRepo.all(query)

      assert [%Post{published: nil}] = results
    end

    test "Rule Statement 2: not published equals list" do
      # Given: [not: [published: [==: [true, false]]]]
      # Expected: not (p.published in [true, false])

      {:ok, _post1} = TestRepo.insert(%Post{title: "True", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "False", published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Null", published: nil})

      query = from(p in Post, where: is_nil(p.published) or p.published not in [true, false])
      results = TestRepo.all(query)

      assert [%Post{published: nil}] = results
    end

    test "Rule Statement 3: not published not equals list" do
      # Given: [not: [published: [!=: [true, false]]]]
      # Expected: p.published in [true, false]

      {:ok, _post1} = TestRepo.insert(%Post{title: "True", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "False", published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Null", published: nil})

      query = from(p in Post, where: p.published in [true, false])
      results = TestRepo.all(query)

      assert [%Post{published: false}, %Post{published: true}] =
               Enum.sort_by(results, & &1.published)
    end

    test "Rule Statement 4: not views greater than" do
      # Given: [not: [views: [>: 10]]]
      # Expected: not (p.views > 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: not (p.views > 10))
      results = TestRepo.all(query)

      assert [%Post{views: 5}] = results
    end

    test "Rule Statement 5: not views greater than or equal" do
      # Given: [not: [views: [>=: 10]]]
      # Expected: not (p.views >= 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Equal Views", views: 10})

      query = from(p in Post, where: not (p.views >= 10))
      results = TestRepo.all(query)

      assert [%Post{views: 5}] = results
    end

    test "Rule Statement 6: not views less than" do
      # Given: [not: [views: [<: 10]]]
      # Expected: not (p.views < 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: not (p.views < 10))
      results = TestRepo.all(query)

      assert [%Post{views: 20}] = results
    end

    test "Rule Statement 7: not views less than or equal" do
      # Given: [not: [views: [<=: 10]]]
      # Expected: not (p.views <= 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Equal Views", views: 10})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: not (p.views <= 10))
      results = TestRepo.all(query)

      assert [%Post{views: 20}] = results
    end

    test "Rule Statement 8: not views equals" do
      # Given: [not: [views: [==: 10]]]
      # Expected: not (p.views == 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Different Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Equal Views", views: 10})

      query = from(p in Post, where: not (p.views == 10))
      results = TestRepo.all(query)

      assert [%Post{views: 5}] = results
    end

    test "Rule Statement 9: not views not equals" do
      # Given: [not: [views: [!=: 10]]]
      # Expected: not (p.views != 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Different Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Equal Views", views: 10})

      query = from(p in Post, where: not (p.views != 10))
      results = TestRepo.all(query)

      assert [%Post{views: 10}] = results
    end

    test "Rule Statement 10: not views gt" do
      # Given: [not: [views: [gt: 10]]]
      # Expected: not (p.views > 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: not (p.views > 10))
      results = TestRepo.all(query)

      assert [%Post{views: 5}] = results
    end

    test "Rule Statement 11: not views gte" do
      # Given: [not: [views: [gte: 10]]]
      # Expected: not (p.views >= 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Equal Views", views: 10})

      query = from(p in Post, where: not (p.views >= 10))
      results = TestRepo.all(query)

      assert [%Post{views: 5}] = results
    end

    test "Rule Statement 12: not views lt" do
      # Given: [not: [views: [lt: 10]]]
      # Expected: not (p.views < 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: not (p.views < 10))
      results = TestRepo.all(query)

      assert [%Post{views: 20}] = results
    end

    test "Rule Statement 13: not views lte" do
      # Given: [not: [views: [lte: 10]]]
      # Expected: not (p.views <= 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Equal Views", views: 10})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: not (p.views <= 10))
      results = TestRepo.all(query)

      assert [%Post{views: 20}] = results
    end
  end

  describe "Logical Operator Directives" do
    test "Rule Statement 1: and with multiple conditions on same field" do
      # Given: [and: [title: "hello", views: [>: 10, <: 20]]]
      # Expected: p.title == "hello" and (p.views > 10 and p.views < 20)

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello", views: 15})
      {:ok, _post2} = TestRepo.insert(%Post{title: "hello", views: 5})
      {:ok, _post3} = TestRepo.insert(%Post{title: "world", views: 15})

      query = from(p in Post, where: p.title == "hello" and (p.views > 10 and p.views < 20))
      results = TestRepo.all(query)

      assert [%Post{title: "hello", views: 15}] = results
    end

    test "Rule Statement 2: nested and with multiple conditions" do
      # Given: [and: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]
      # Expected: (p.title == "hello" and (p.views > 10 and p.views < 20)) and p.published == true

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello", views: 15, published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "hello", views: 15, published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "hello", views: 5, published: true})

      query = from(p in Post, where: (p.title == "hello" and (p.views > 10 and p.views < 20)) and p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "hello", views: 15, published: true}] = results
    end

    test "Rule Statement 3: or with nested and conditions" do
      # Given: [or: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]
      # Expected: (p.title == "hello" and (p.views > 10 and p.views < 20)) or p.published == true

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello", views: 15, published: false})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world", views: 5, published: true})
      {:ok, _post3} = TestRepo.insert(%Post{title: "world", views: 5, published: false})

      query = from(p in Post, where: (p.title == "hello" and (p.views > 10 and p.views < 20)) or p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "hello", published: false}, %Post{title: "world", published: true}] =
               Enum.sort_by(results, & {&1.title, &1.published})
    end

    test "Rule Statement 4: nested or within or" do
      # Given: [or: [[or: [title: "hello", views: [>: 10, <: 20]]], [published: true]]]
      # Expected: (p.title == "hello" or (p.views > 10 and p.views < 20)) or p.published == true

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello", views: 5, published: false})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world", views: 15, published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "world", views: 5, published: true})
      {:ok, _post4} = TestRepo.insert(%Post{title: "world", views: 5, published: false})

      query = from(p in Post, where: (p.title == "hello" or (p.views > 10 and p.views < 20)) or p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "hello"}, %Post{title: "world", views: 15}, %Post{title: "world", published: true}] =
               Enum.sort_by(results, & {&1.title, &1.published, &1.views})
    end

    test "Rule Statement 5: nested or within and within or" do
      # Given: [or: [[title: "hello", or: [views: [>: 10, <: 20]]], [published: true]]]
      # Expected: (p.title == "hello" or (p.views > 10 and p.views < 20)) or p.published == true

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello", views: 5, published: false})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world", views: 15, published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "world", views: 5, published: true})
      {:ok, _post4} = TestRepo.insert(%Post{title: "world", views: 5, published: false})

      query = from(p in Post, where: (p.title == "hello" or (p.views > 10 and p.views < 20)) or p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "hello"}, %Post{title: "world", views: 15}, %Post{title: "world", published: true}] =
               Enum.sort_by(results, & {&1.title, &1.published, &1.views})
    end
  end

  describe "Comparison Operator Directives" do
    test "Rule Statement 1: id equals with == operator" do
      # Given: [id: [==: 1]]
      # Expected: p.id == 1

      {:ok, post1} = TestRepo.insert(%Post{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Post 2"})

      query = from(p in Post, where: p.id == ^post1.id)
      results = TestRepo.all(query)

      assert [%Post{title: "Post 1"}] = results
    end

    test "Rule Statement 2: id equals with eq operator" do
      # Given: [id: [eq: 1]]
      # Expected: p.id == 1

      {:ok, post1} = TestRepo.insert(%Post{title: "Post 1"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Post 2"})

      query = from(p in Post, where: p.id == ^post1.id)
      results = TestRepo.all(query)

      assert [%Post{title: "Post 1"}] = results
    end

    test "Rule Statement 3: published_at equals nil with ==" do
      # Given: [published_at: [==: nil]]
      # Expected: is_nil(p.published_at)

      {:ok, _post1} = TestRepo.insert(%Post{title: "No Date", published_at: nil})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Has Date", published_at: ~U[2026-01-01 00:00:00Z]})

      query = from(p in Post, where: is_nil(p.published_at))
      results = TestRepo.all(query)

      assert [%Post{published_at: nil}] = results
    end

    test "Rule Statement 4: published_at equals nil with eq" do
      # Given: [published_at: [eq: nil]]
      # Expected: is_nil(p.published_at)

      {:ok, _post1} = TestRepo.insert(%Post{title: "No Date", published_at: nil})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Has Date", published_at: ~U[2026-01-01 00:00:00Z]})

      query = from(p in Post, where: is_nil(p.published_at))
      results = TestRepo.all(query)

      assert [%Post{published_at: nil}] = results
    end

    test "Rule Statement 5: published_at not equals nil" do
      # Given: [published_at: [!=: nil]]
      # Expected: not is_nil(p.published_at)

      {:ok, _post1} = TestRepo.insert(%Post{title: "No Date", published_at: nil})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Has Date", published_at: ~U[2026-01-01 00:00:00Z]})

      query = from(p in Post, where: not is_nil(p.published_at))
      results = TestRepo.all(query)

      assert [%Post{published_at: ~U[2026-01-01 00:00:00Z]}] = results
    end

    test "Rule Statement 6: views greater than" do
      # Given: [views: [>: 10]]
      # Expected: p.views > 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: p.views > 10)
      results = TestRepo.all(query)

      assert [%Post{views: 20}] = results
    end

    test "Rule Statement 7: views greater than or equal" do
      # Given: [views: [>=: 10]]
      # Expected: p.views >= 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Equal Views", views: 10})

      query = from(p in Post, where: p.views >= 10)
      results = TestRepo.all(query)

      assert [%Post{views: 10}] = results
    end

    test "Rule Statement 8: views less than" do
      # Given: [views: [<: 10]]
      # Expected: p.views < 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: p.views < 10)
      results = TestRepo.all(query)

      assert [%Post{views: 5}] = results
    end

    test "Rule Statement 9: views less than or equal" do
      # Given: [views: [<=: 10]]
      # Expected: p.views <= 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Equal Views", views: 10})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: p.views <= 10)
      results = TestRepo.all(query)

      assert [%Post{views: 10}] = results
    end

    test "Rule Statement 10: views not equals" do
      # Given: [views: [!=: 10]]
      # Expected: p.views != 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Different Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Equal Views", views: 10})

      query = from(p in Post, where: p.views != 10)
      results = TestRepo.all(query)

      assert [%Post{views: 5}] = results
    end

    test "Rule Statement 11: views gt operator" do
      # Given: [views: [gt: 10]]
      # Expected: p.views > 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: p.views > 10)
      results = TestRepo.all(query)

      assert [%Post{views: 20}] = results
    end

    test "Rule Statement 12: views gte operator" do
      # Given: [views: [gte: 10]]
      # Expected: p.views >= 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Equal Views", views: 10})

      query = from(p in Post, where: p.views >= 10)
      results = TestRepo.all(query)

      assert [%Post{views: 10}] = results
    end

    test "Rule Statement 13: views lt operator" do
      # Given: [views: [lt: 10]]
      # Expected: p.views < 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: p.views < 10)
      results = TestRepo.all(query)

      assert [%Post{views: 5}] = results
    end

    test "Rule Statement 14: views lte operator" do
      # Given: [views: [lte: 10]]
      # Expected: p.views <= 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Equal Views", views: 10})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", views: 20})

      query = from(p in Post, where: p.views <= 10)
      results = TestRepo.all(query)

      assert [%Post{views: 10}] = results
    end

    test "Rule Statement 15: published in list" do
      # Given: [published: [in: [true, false]]]
      # Expected: p.published in [true, false]

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Unpublished", published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Null", published: nil})

      query = from(p in Post, where: p.published in [true, false])
      results = TestRepo.all(query)

      assert [%Post{published: false}, %Post{published: true}] =
               Enum.sort_by(results, & &1.published)
    end

    test "Rule Statement 16: published equals list" do
      # Given: [published: [==: [true, false]]]
      # Expected: p.published in [true, false]

      {:ok, _post1} = TestRepo.insert(%Post{title: "True", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "False", published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Null", published: nil})

      query = from(p in Post, where: p.published in [true, false])
      results = TestRepo.all(query)

      assert [%Post{published: false}, %Post{published: true}] =
               Enum.sort_by(results, & &1.published)
    end

    test "Rule Statement 17: published not equals list" do
      # Given: [published: [!=: [true, false]]]
      # Expected: is_nil(p.published) or p.published not in [true, false]

      {:ok, _post1} = TestRepo.insert(%Post{title: "True", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "False", published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Null", published: nil})

      query = from(p in Post, where: is_nil(p.published) or p.published not in [true, false])
      results = TestRepo.all(query)

      assert [%Post{published: nil}] = results
    end
  end

  describe "String Matching Directives" do
    test "Rule Statement 1: title like pattern" do
      # Given: [title: [like: "%hello%"]]
      # Expected: like(p.title, "%hello%")

      {:ok, _post1} = TestRepo.insert(%Post{title: "say hello world"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "goodbye"})

      query = from(p in Post, where: like(p.title, "%hello%"))
      results = TestRepo.all(query)

      assert [%Post{title: "say hello world"}] = results
    end

    test "Rule Statement 2: title like pattern negated" do
      # Given: [not: [title: [like: "%hello%"]]]
      # Expected: not like(p.title, "%hello%")

      {:ok, _post1} = TestRepo.insert(%Post{title: "say hello world"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "goodbye"})

      query = from(p in Post, where: not like(p.title, "%hello%"))
      results = TestRepo.all(query)

      assert [%Post{title: "goodbye"}] = results
    end

    test "Rule Statement 3: title ilike pattern" do
      # Given: [title: [ilike: "%HELLO%"]]
      # Expected: ilike(p.title, "%HELLO%")

      {:ok, _post1} = TestRepo.insert(%Post{title: "say hello world"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "goodbye"})

      query = from(p in Post, where: ilike(p.title, "%HELLO%"))
      results = TestRepo.all(query)

      assert [%Post{title: "say hello world"}] = results
    end

    test "Rule Statement 4: title ilike pattern negated" do
      # Given: [not: [title: [ilike: "%HELLO%"]]]
      # Expected: not ilike(p.title, "%HELLO%")

      {:ok, _post1} = TestRepo.insert(%Post{title: "say hello world"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "goodbye"})

      query = from(p in Post, where: not ilike(p.title, "%HELLO%"))
      results = TestRepo.all(query)

      assert [%Post{title: "goodbye"}] = results
    end

    test "Rule Statement 5: title like list of patterns" do
      # Given: [title: [like: ["%hello%", "%world%"]]]
      # Expected: like(p.title, "%hello%") or like(p.title, "%world%")

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello there"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world news"})
      {:ok, _post3} = TestRepo.insert(%Post{title: "goodbye"})

      query = from(p in Post, where: like(p.title, "%hello%") or like(p.title, "%world%"))
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 6: title ilike list of patterns" do
      # Given: [title: [ilike: ["%HELLO%", "%WORLD%"]]]
      # Expected: ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%")

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello there"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "WORLD news"})
      {:ok, _post3} = TestRepo.insert(%Post{title: "goodbye"})

      query = from(p in Post, where: ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%"))
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 7: title like list negated" do
      # Given: [not: [title: [like: ["%hello%", "%world%"]]]]
      # Expected: not (like(p.title, "%hello%") or like(p.title, "%world%"))

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello there"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world news"})
      {:ok, _post3} = TestRepo.insert(%Post{title: "goodbye"})

      query = from(p in Post, where: not (like(p.title, "%hello%") or like(p.title, "%world%")))
      results = TestRepo.all(query)

      assert [%Post{title: "goodbye"}] = results
    end

    test "Rule Statement 8: title ilike list negated" do
      # Given: [not: [title: [ilike: ["%HELLO%", "%WORLD%"]]]]
      # Expected: not (ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%"))

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello there"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "WORLD news"})
      {:ok, _post3} = TestRepo.insert(%Post{title: "goodbye"})

      query = from(p in Post, where: not (ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%")))
      results = TestRepo.all(query)

      assert [%Post{title: "goodbye"}] = results
    end
  end

  describe "String Transformation Directives" do
    test "Rule Statement 1: lower field equals value" do
      # Given: [title: [lower: [==: "hello"]]]
      # Expected: fragment("lower(?)", p.title) == "hello"

      {:ok, _post1} = TestRepo.insert(%Post{title: "HELLO"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: fragment("lower(?)", p.title) == "hello")
      results = TestRepo.all(query)

      assert [%Post{title: "HELLO"}] = results
    end

    test "Rule Statement 2: upper field equals value" do
      # Given: [title: [upper: [==: "HELLO"]]]
      # Expected: fragment("upper(?)", p.title) == "HELLO"

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: fragment("upper(?)", p.title) == "HELLO")
      results = TestRepo.all(query)

      assert [%Post{title: "hello"}] = results
    end

    test "Rule Statement 3: lower field not equals value" do
      # Given: [title: [lower: [!=: "hello"]]]
      # Expected: fragment("lower(?)", p.title) != "hello"

      {:ok, _post1} = TestRepo.insert(%Post{title: "HELLO"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: fragment("lower(?)", p.title) != "hello")
      results = TestRepo.all(query)

      assert [%Post{title: "world"}] = results
    end

    test "Rule Statement 4: upper field not equals value" do
      # Given: [title: [upper: [!=: "HELLO"]]]
      # Expected: fragment("upper(?)", p.title) != "HELLO"

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: fragment("upper(?)", p.title) != "HELLO")
      results = TestRepo.all(query)

      assert [%Post{title: "world"}] = results
    end

    test "Rule Statement 5: lower field negated equals value" do
      # Given: [not: [title: [lower: [==: "hello"]]]]
      # Expected: not (fragment("lower(?)", p.title) == "hello")

      {:ok, _post1} = TestRepo.insert(%Post{title: "HELLO"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: not (fragment("lower(?)", p.title) == "hello"))
      results = TestRepo.all(query)

      assert [%Post{title: "world"}] = results
    end

    test "Rule Statement 6: upper field negated equals value" do
      # Given: [not: [title: [upper: [==: "HELLO"]]]]
      # Expected: not (fragment("upper(?)", p.title) == "HELLO")

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: not (fragment("upper(?)", p.title) == "HELLO"))
      results = TestRepo.all(query)

      assert [%Post{title: "world"}] = results
    end

    test "Rule Statement 7: equals lowercased value" do
      # Given: [title: [==: [lower: "HELLO"]]]
      # Expected: p.title == fragment("lower(?)", "HELLO")

      {:ok, _post1} = TestRepo.insert(%Post{title: "hello"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: p.title == fragment("lower(?)", "HELLO"))
      results = TestRepo.all(query)

      assert [%Post{title: "hello"}] = results
    end

    test "Rule Statement 8: equals uppercased value" do
      # Given: [title: [==: [upper: "hello"]]]
      # Expected: p.title == fragment("upper(?)", "hello")

      {:ok, _post1} = TestRepo.insert(%Post{title: "HELLO"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "world"})

      query = from(p in Post, where: p.title == fragment("upper(?)", "hello"))
      results = TestRepo.all(query)

      assert [%Post{title: "HELLO"}] = results
    end
  end

  describe "Aggregate Operator Directives" do
    test "Rule Statement 1: avg views greater than" do
      # Given: [views: [avg: [>: 10]]]
      # Expected: avg(p.views) > 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "High", views: 20})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Low", views: 5})

      query = from(p in Post, group_by: p.title, having: avg(p.views) > 10, select: p.title)
      results = TestRepo.all(query)

      assert ["High"] = results
    end

    test "Rule Statement 2: avg views greater than negated" do
      # Given: [not: [views: [avg: [>: 10]]]]
      # Expected: not (avg(p.views) > 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "High", views: 20})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Low", views: 5})

      query = from(p in Post, group_by: p.title, having: not (avg(p.views) > 10), select: p.title)
      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 3: count views greater than zero" do
      # Given: [views: [count: [>: 0]]]
      # Expected: count(p.views) > 0

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Views", views: 5})

      query = from(p in Post, group_by: p.title, having: count(p.views) > 0, select: p.title)
      results = TestRepo.all(query)

      assert ["Has Views"] = results
    end

    test "Rule Statement 4: max views greater than or equal" do
      # Given: [views: [max: [>=: 100]]]
      # Expected: max(p.views) >= 100

      {:ok, _post1} = TestRepo.insert(%Post{title: "High", views: 100})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Low", views: 50})

      query = from(p in Post, group_by: p.title, having: max(p.views) >= 100, select: p.title)
      results = TestRepo.all(query)

      assert ["High"] = results
    end

    test "Rule Statement 5: min views less than" do
      # Given: [views: [min: [<: 5]]]
      # Expected: min(p.views) < 5

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low", views: 3})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High", views: 10})

      query = from(p in Post, group_by: p.title, having: min(p.views) < 5, select: p.title)
      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 6: sum views equals" do
      # Given: [views: [sum: [==: 1000]]]
      # Expected: sum(p.views) == 1000

      {:ok, _post1} = TestRepo.insert(%Post{title: "A", views: 1000})
      {:ok, _post2} = TestRepo.insert(%Post{title: "B", views: 500})

      query = from(p in Post, group_by: p.title, having: sum(p.views) == 1000, select: p.title)
      results = TestRepo.all(query)

      assert ["A"] = results
    end

    test "Rule Statement 7: avg views not equals" do
      # Given: [views: [avg: [!=: 50]]]
      # Expected: avg(p.views) != 50

      {:ok, _post1} = TestRepo.insert(%Post{title: "Match", views: 20})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Skip", views: 50})

      query = from(p in Post, group_by: p.title, having: avg(p.views) != 50, select: p.title)
      results = TestRepo.all(query)

      assert ["Match"] = results
    end

    test "Rule Statement 8: count views equals nil" do
      # Given: [views: [count: [==: nil]]]
      # Expected: count(p.views) == nil

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post", views: 5})

      query = from(p in Post, group_by: p.title, having: is_nil(count(p.views)), select: p.title)
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 9: count views greater than zero negated" do
      # Given: [not: [views: [count: [>: 0]]]]
      # Expected: not (count(p.views) > 0)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Has Views", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Zero Views", views: 0})

      query =
        from(p in Post, group_by: p.title, having: not (count(p.views) > 0), select: p.title)

      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 10: max views greater than or equal negated" do
      # Given: [not: [views: [max: [>=: 100]]]]
      # Expected: not (max(p.views) >= 100)

      {:ok, _post1} = TestRepo.insert(%Post{title: "High", views: 100})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Low", views: 50})

      query =
        from(p in Post, group_by: p.title, having: not (max(p.views) >= 100), select: p.title)

      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 11: avg views less than or equal" do
      # Given: [views: [avg: [<=: 10]]]
      # Expected: avg(p.views) <= 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Low", views: 8})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High", views: 20})

      query = from(p in Post, group_by: p.title, having: avg(p.views) <= 10, select: p.title)
      results = TestRepo.all(query)

      assert ["Low"] = results
    end

    test "Rule Statement 12: sum views greater than" do
      # Given: [views: [sum: [>: 500]]]
      # Expected: sum(p.views) > 500

      {:ok, _post1} = TestRepo.insert(%Post{title: "Big", views: 600})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Small", views: 200})

      query = from(p in Post, group_by: p.title, having: sum(p.views) > 500, select: p.title)
      results = TestRepo.all(query)

      assert ["Big"] = results
    end
  end

  describe "Arithmetic Operator Directives" do
    test "Rule Statement 1: views greater than views plus 10" do
      # Given: [views: [>: [+: [:views, 10]]]]
      # Expected: p.views > p.views + 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post", views: 5})

      query = from(p in Post, where: p.views > p.views + 10)
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 2: views greater than views plus 10 negated" do
      # Given: [not: [views: [>: [+: [:views, 10]]]]]
      # Expected: not (p.views > p.views + 10)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post", views: 5})

      query = from(p in Post, where: not (p.views > p.views + 10))
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 3: views greater than or equal to views minus 5" do
      # Given: [views: [>=: [-: [:views, 5]]]]
      # Expected: p.views >= p.views - 5

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post", views: 10})

      query = from(p in Post, where: p.views >= p.views - 5)
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 4: views less than views times 2" do
      # Given: [views: [<: [*: [:views, 2]]]]
      # Expected: p.views < p.views * 2

      {:ok, _post1} = TestRepo.insert(%Post{title: "Positive", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Zero", views: 0})

      query = from(p in Post, where: p.views < p.views * 2)
      results = TestRepo.all(query)

      assert [%Post{title: "Positive"}] = results
    end

    test "Rule Statement 5: views equals views divided by 2" do
      # Given: [views: [==: [/: [:views, 2]]]]
      # Expected: p.views == p.views / 2

      {:ok, _post1} = TestRepo.insert(%Post{title: "Zero", views: 0})
      {:ok, _post2} = TestRepo.insert(%Post{title: "NonZero", views: 5})

      query = from(p in Post, where: p.views == p.views / 2)
      results = TestRepo.all(query)

      assert [%Post{title: "Zero"}] = results
    end

    test "Rule Statement 6: views not equals views plus 10" do
      # Given: [views: [!=: [+: [:views, 10]]]]
      # Expected: p.views != p.views + 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post", views: 5})

      query = from(p in Post, where: p.views != p.views + 10)
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 7: views greater than or equal to views minus 5 negated" do
      # Given: [not: [views: [>=: [-: [:views, 5]]]]]
      # Expected: not (p.views >= p.views - 5)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post", views: 10})

      query = from(p in Post, where: not (p.views >= p.views - 5))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 8: views less than views times 2 negated" do
      # Given: [not: [views: [<: [*: [:views, 2]]]]]
      # Expected: not (p.views < p.views * 2)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Positive", views: 5})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Zero", views: 0})

      query = from(p in Post, where: not (p.views < p.views * 2))
      results = TestRepo.all(query)

      assert [%Post{title: "Zero"}] = results
    end

    test "Rule Statement 9: views less than or equal to literal 10 plus 5" do
      # Given: [views: [<=: [+: [10, 5]]]]
      # Expected: p.views <= 10 + 5

      {:ok, _post1} = TestRepo.insert(%Post{title: "Within", views: 14})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Over", views: 20})

      query = from(p in Post, where: p.views <= 10 + 5)
      results = TestRepo.all(query)

      assert [%Post{title: "Within"}] = results
    end

    test "Rule Statement 10: views greater than literal 100 minus 10" do
      # Given: [views: [>: [-: [100, 10]]]]
      # Expected: p.views > 100 - 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Over", views: 95})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Under", views: 85})

      threshold = 100 - 10
      query = from(p in Post, where: p.views > ^threshold)
      results = TestRepo.all(query)

      assert [%Post{title: "Over"}] = results
    end
  end

  describe "Date/Time Directives" do
    test "Rule Statement 1: inserted_at greater than or equal to datetime_add" do
      # Given: [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at >= datetime_add(p.inserted_at, 1, "day")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: p.inserted_at >= datetime_add(p.inserted_at, 1, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 2: inserted_at greater than ago 1 day" do
      # Given: [inserted_at: [>: [datetime: [ago: [count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at > ago(1, "day")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Recent"})

      query = from(p in Post, where: p.inserted_at > ago(1, "day"))
      results = TestRepo.all(query)

      assert [%Post{title: "Recent"}] = results
    end

    test "Rule Statement 3: inserted_at greater than from_now 1 day" do
      # Given: [inserted_at: [>: [datetime: [from_now: [count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at > from_now(1, "day")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: p.inserted_at > from_now(1, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 4: inserted_at >= datetime_add negated" do
      # Given: [not: [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]]
      # Expected: not (p.inserted_at >= datetime_add(p.inserted_at, 1, "day"))

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: not (p.inserted_at >= datetime_add(p.inserted_at, 1, "day")))
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 5: inserted_at less than ago 7 days" do
      # Given: [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]
      # Expected: p.inserted_at < ago(7, "day")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: p.inserted_at < ago(7, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 6: inserted_at less than or equal to from_now 30 days" do
      # Given: [inserted_at: [<=: [datetime: [from_now: [count: 30, interval: "day"]]]]]
      # Expected: p.inserted_at <= from_now(30, "day")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: p.inserted_at <= from_now(30, "day"))
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 7: inserted_at equals ago 1 day using date wrapper" do
      # Given: [inserted_at: [==: [date: [ago: [count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at == ago(1, "day")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: p.inserted_at == ago(1, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 8: inserted_at not equals from_now 1 day using date wrapper" do
      # Given: [inserted_at: [!=: [date: [from_now: [count: 1, interval: "day"]]]]]
      # Expected: p.inserted_at != from_now(1, "day")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: p.inserted_at != from_now(1, "day"))
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 9: inserted_at less than ago 7 days negated" do
      # Given: [not: [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]]
      # Expected: not (p.inserted_at < ago(7, "day"))

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: not (p.inserted_at < ago(7, "day")))
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 10: inserted_at greater than from_now 1 day negated using date wrapper" do
      # Given: [not: [inserted_at: [>: [date: [from_now: [count: 1, interval: "day"]]]]]]
      # Expected: not (p.inserted_at > from_now(1, "day"))

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: not (p.inserted_at > from_now(1, "day")))
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 11: inserted_at >= datetime_add 7 days using date wrapper" do
      # Given: [inserted_at: [>=: [date: [add: [field: :inserted_at, count: 7, interval: "day"]]]]]
      # Expected: p.inserted_at >= datetime_add(p.inserted_at, 7, "day")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: p.inserted_at >= datetime_add(p.inserted_at, 7, "day"))
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 12: inserted_at less than ago 1 month using date wrapper" do
      # Given: [inserted_at: [<: [date: [ago: [count: 1, interval: "month"]]]]]
      # Expected: p.inserted_at < ago(1, "month")

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, where: p.inserted_at < ago(1, "month"))
      results = TestRepo.all(query)

      assert [] = results
    end
  end

  describe "Schema Filter Directives" do
    test "Rule Statement 1: explicit where clause" do
      # Given: [where: [published: true]]
      # Expected: where: p.published == true

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Draft", published: false})

      query = from(p in Post, where: p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "Published"}] = results
    end

    test "Rule Statement 2: explicit where clause with multiple conditions" do
      # Given: [where: [published: true, views: 10]]
      # Expected: where: p.published == true and p.views == 10

      {:ok, _post1} = TestRepo.insert(%Post{title: "Match", published: true, views: 10})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Wrong Views", published: true, views: 5})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Draft", published: false, views: 10})

      query = from(p in Post, where: p.published == true and p.views == 10)
      results = TestRepo.all(query)

      assert [%Post{title: "Match"}] = results
    end

    test "Rule Statement 3: explicit or_where clause" do
      # Given: [or_where: [published: false]]
      # Expected: or_where: p.published == false

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Draft", published: false})

      query = from(p in Post, or_where: p.published == false)
      results = TestRepo.all(query)

      assert [%Post{title: "Draft"}] = results
    end

    test "Rule Statement 4: combined where and or_where" do
      # Given: [where: [published: true], or_where: [published: false]]
      # Expected: (p.published == true) or (p.published == false)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Draft", published: false})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Null", published: nil})

      query = from(p in Post, where: p.published == true, or_where: p.published == false)
      results = TestRepo.all(query)

      assert 2 = length(results)
    end

    test "Rule Statement 5: or_where with nested or logic and implicit where" do
      # Given: [or_where: [or: [views: [>: 10, <: 5]]], published: true]
      # Expected: (p.published == true) or (p.views > 10 or p.views < 5)

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published", published: true, views: 7})
      {:ok, _post2} = TestRepo.insert(%Post{title: "High Views", published: false, views: 15})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Low Views", published: false, views: 3})
      {:ok, _post4} = TestRepo.insert(%Post{title: "Mid Views", published: false, views: 7})

      query =
        from(p in Post,
          where: p.published == true,
          or_where: p.views > 10 or p.views < 5
        )

      results = TestRepo.all(query)

      assert 3 = length(results)
    end
  end

  describe "Terminal Filter Directives" do
    test "Rule Statement 1: last 2 records" do
      # Given: [last: 2]
      # Expected: SELECT ... FROM (SELECT ... ORDER BY id DESC LIMIT 2) AS subquery ORDER BY id ASC

      {:ok, post1} = TestRepo.insert(%Post{title: "First"})
      {:ok, post2} = TestRepo.insert(%Post{title: "Second"})
      {:ok, post3} = TestRepo.insert(%Post{title: "Third"})

      inner = from(p in Post, order_by: [desc: p.id], limit: 2)
      query = from(p in subquery(inner), order_by: [asc: p.id])
      results = TestRepo.all(query)

      assert [%{id: id2}, %{id: id3}] = results
      assert id2 == post2.id
      assert id3 == post3.id
    end

    test "Rule Statement 2: subquery with id filter" do
      # Given: [subquery: [id: 2]]
      # Expected: SELECT ... FROM (SELECT ... WHERE id = 2) AS subquery

      {:ok, _post1} = TestRepo.insert(%Post{title: "First"})
      {:ok, post2} = TestRepo.insert(%Post{title: "Second"})

      inner = from(p in Post, where: p.id == ^post2.id)
      query = from(p in subquery(inner))
      results = TestRepo.all(query)

      assert [%{title: "Second"}] = results
    end

    test "Rule Statement 3: published true and subquery with id filter" do
      # Given: [published: true, subquery: [id: 2]]
      # Expected: SELECT ... FROM (SELECT ... WHERE published = true AND id = 2) AS subquery

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published No Match", published: true})
      {:ok, post2} = TestRepo.insert(%Post{title: "Match", published: true})

      inner = from(p in Post, where: p.published == true and p.id == ^post2.id)
      query = from(p in subquery(inner))
      results = TestRepo.all(query)

      assert [%{title: "Match"}] = results
    end

    test "Rule Statement 4: last 2 records by title" do
      # Given: [last: [title: 2]]
      # Expected: SELECT ... FROM (SELECT ... ORDER BY title DESC LIMIT 2) AS subquery ORDER BY title ASC

      {:ok, _post1} = TestRepo.insert(%Post{title: "Alpha"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Beta"})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Gamma"})

      inner = from(p in Post, order_by: [desc: p.title], limit: 2)
      query = from(p in subquery(inner), order_by: [asc: p.title])
      results = TestRepo.all(query)

      assert [%{title: "Beta"}, %{title: "Gamma"}] = results
    end

    test "Rule Statement 5: subquery with published true and views greater than 10" do
      # Given: [subquery: [published: true, views: [>: 10]]]
      # Expected: SELECT ... FROM (SELECT ... WHERE published = true AND views > 10) AS subquery

      {:ok, _post1} = TestRepo.insert(%Post{title: "Match", published: true, views: 15})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Low Views", published: true, views: 5})
      {:ok, _post3} = TestRepo.insert(%Post{title: "Unpublished", published: false, views: 20})

      inner = from(p in Post, where: p.published == true and p.views > 10)
      query = from(p in subquery(inner))
      results = TestRepo.all(query)

      assert [%{title: "Match"}] = results
    end
  end

  describe "Query Configuration Directives" do
    test "Rule Statement 7: from schema with filters" do
      # Given: [from: Post, id: 1, published: true]
      # Expected: from: Post, where: p.id == 1 and p.published == true

      {:ok, post1} = TestRepo.insert(%Post{title: "Match", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Draft", published: false})

      query = from(p in Post, where: p.id == ^post1.id and p.published == true)
      results = TestRepo.all(query)

      assert [%Post{title: "Match"}] = results
    end

    test "Rule Statement 11: select true selects all fields" do
      # Given: [select: true]
      # Expected: select: true

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      query = from(p in Post, select: p)
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 12: select single field" do
      # Given: [select: :id]
      # Expected: select: p.id

      {:ok, post1} = TestRepo.insert(%Post{title: "Post"})
      expected_id = post1.id

      query = from(p in Post, select: p.id)
      results = TestRepo.all(query)

      assert [^expected_id] = results
    end

    test "Rule Statement 13: select list of fields" do
      # Given: [select: [:id, :title]]
      # Expected: select: [p.id, p.title]

      {:ok, post1} = TestRepo.insert(%Post{title: "Post"})
      expected_id = post1.id

      query = from(p in Post, select: [p.id, p.title])
      results = TestRepo.all(query)

      assert [[^expected_id, "Post"]] = results
    end

    test "Rule Statement 14: select map of fields" do
      # Given: [select: [map: [:id, :title]]]
      # Expected: select: %{id: p.id, title: p.title}

      {:ok, post1} = TestRepo.insert(%Post{title: "Post"})
      expected_id = post1.id

      query = from(p in Post, select: %{id: p.id, title: p.title})
      results = TestRepo.all(query)

      assert [%{id: ^expected_id, title: "Post"}] = results
    end

    test "Rule Statement 15: select map with renamed key" do
      # Given: [select: [map: [custom_id: :id]]]
      # Expected: select: %{custom_id: p.id}

      {:ok, post1} = TestRepo.insert(%Post{title: "Post"})
      expected_id = post1.id

      query = from(p in Post, select: %{custom_id: p.id})
      results = TestRepo.all(query)

      assert [%{custom_id: ^expected_id}] = results
    end

    test "Rule Statement 16: select struct" do
      # Given: [select: [struct: [:id]]]
      # Expected: select: struct(p, [:id])

      {:ok, post1} = TestRepo.insert(%Post{title: "Post"})
      expected_id = post1.id

      query = from(p in Post, select: struct(p, [:id]))
      results = TestRepo.all(query)

      assert [%Post{id: ^expected_id}] = results
    end

    test "Rule Statement 20: distinct true" do
      # Given: [distinct: true]
      # Expected: distinct: true

      {:ok, _post1} = TestRepo.insert(%Post{title: "Same", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Same", published: true})

      query = from(p in Post, distinct: true, select: p.title)
      results = TestRepo.all(query)

      assert ["Same"] = results
    end

    test "Rule Statement 25: group by single field" do
      # Given: [group_by: :author_id]
      # Expected: group_by: p.author_id

      {:ok, _post1} = TestRepo.insert(%Post{title: "A", views: 10})
      {:ok, _post2} = TestRepo.insert(%Post{title: "B", views: 20})

      query = from(p in Post, group_by: p.author_id, select: p.author_id)
      results = TestRepo.all(query)

      assert [nil] = results
    end

    test "Rule Statement 27: having clause with equality" do
      # Given: [having: [published: true]]
      # Expected: having: p.published == true

      {:ok, _post1} = TestRepo.insert(%Post{title: "Published", published: true})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Draft", published: false})

      query =
        from(p in Post, group_by: [p.title, p.published], having: p.published == true,
          select: p.title)

      results = TestRepo.all(query)

      assert ["Published"] = results
    end

    test "Rule Statement 34: order by field ascending" do
      # Given: [order_by: :title]
      # Expected: order_by: [asc: p.title]

      {:ok, _post1} = TestRepo.insert(%Post{title: "Beta"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Alpha"})

      query = from(p in Post, order_by: [asc: p.title], select: p.title)
      results = TestRepo.all(query)

      assert ["Alpha", "Beta"] = results
    end

    test "Rule Statement 35: order by field descending" do
      # Given: [order_by: [desc: :title]]
      # Expected: order_by: [desc: p.title]

      {:ok, _post1} = TestRepo.insert(%Post{title: "Alpha"})
      {:ok, _post2} = TestRepo.insert(%Post{title: "Beta"})

      query = from(p in Post, order_by: [desc: p.title], select: p.title)
      results = TestRepo.all(query)

      assert ["Beta", "Alpha"] = results
    end

    test "Rule Statement 41: limit results" do
      # Given: [limit: 10]
      # Expected: limit: 10

      for i <- 1..15 do
        {:ok, _} = TestRepo.insert(%Post{title: "Post #{i}"})
      end

      query = from(p in Post, limit: 10)
      results = TestRepo.all(query)

      assert 10 = length(results)
    end

    test "Rule Statement 42: offset results" do
      # Given: [offset: 5]
      # Expected: offset: 5

      for i <- 1..10 do
        {:ok, _} = TestRepo.insert(%Post{title: "Post #{i}"})
      end

      query = from(p in Post, offset: 5, order_by: [asc: p.id])
      results = TestRepo.all(query)

      assert 5 = length(results)
    end

    test "Rule Statement 44: limit and offset" do
      # Given: [limit: 10, offset: 5]
      # Expected: limit: 10, offset: 5

      for i <- 1..20 do
        {:ok, _} = TestRepo.insert(%Post{title: "Post #{i}"})
      end

      query = from(p in Post, limit: 10, offset: 5)
      results = TestRepo.all(query)

      assert 10 = length(results)
    end

    test "Rule Statement 50: start_date filter" do
      # Given: [start_date: ~U[2026-01-01 00:00:00Z]]
      # Expected: where: p.inserted_at >= ~U[2026-01-01 00:00:00Z]

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      start_date = ~U[2030-01-01 00:00:00Z]
      query = from(p in Post, where: p.inserted_at >= ^start_date)
      results = TestRepo.all(query)

      assert [] = results
    end

    test "Rule Statement 51: end_date filter" do
      # Given: [end_date: ~U[2026-12-31 23:59:59Z]]
      # Expected: where: p.inserted_at <= ~U[2026-12-31 23:59:59Z]

      {:ok, _post1} = TestRepo.insert(%Post{title: "Post"})

      end_date = ~U[2030-12-31 23:59:59Z]
      query = from(p in Post, where: p.inserted_at <= ^end_date)
      results = TestRepo.all(query)

      assert [%Post{title: "Post"}] = results
    end

    test "Rule Statement 52: ids filter" do
      # Given: [ids: [1, 2, 3]]
      # Expected: where: p.id in [1, 2, 3]

      {:ok, post1} = TestRepo.insert(%Post{title: "A"})
      {:ok, post2} = TestRepo.insert(%Post{title: "B"})
      {:ok, _post3} = TestRepo.insert(%Post{title: "C"})

      ids = [post1.id, post2.id]
      query = from(p in Post, where: p.id in ^ids)
      results = TestRepo.all(query)

      assert 2 = length(results)
    end
  end
end
