defmodule EctoShorts.QueryBuilder.ParamPreprocessorTest do
  use ExUnit.Case
  alias EctoShorts.QueryBuilder.ParamPreprocessor

  describe "normalize_params/1" do
    test "flattens flat maps into tuples" do
      actual = ParamPreprocessor.normalize_params(%{title: "A", published: true})

      expected = [
        {:title, "A"},
        {:published, true}
      ]

      assert Enum.sort(actual) === Enum.sort(expected)
    end

    test "flattens nested maps into path tuples" do
      actual = ParamPreprocessor.normalize_params(%{post: %{title: "A", published: true}})

      expected = [
        {:post, {:title, "A"}},
        {:post, {:published, true}}
      ]

      assert Enum.sort(actual) === Enum.sort(expected)
    end

    test "keeps scalar lists as single values" do
      actual = ParamPreprocessor.normalize_params(%{tags: ["A", "B", "C"], likes: [1, 2, 3]})

      expected = [
        {:tags, ["A", "B", "C"]},
        {:likes, [1, 2, 3]}
      ]

      assert Enum.sort(actual) === Enum.sort(expected)
    end

    test "expands lists of maps into keyed tuples" do
      actual =
        ParamPreprocessor.normalize_params(%{
          users: [%{email: "a@b.com", display_name: "ab"}]
        })

      expected = [
        {:users, {:email, "a@b.com"}},
        {:users, {:display_name, "ab"}}
      ]

      assert Enum.sort(actual) === Enum.sort(expected)
    end

    test "retains keyword lists and tuple leaves" do
      actual =
        ParamPreprocessor.normalize_params(%{
          filters: [status: "active", role: "admin"],
          meta: {:custom, "value"}
        })

      expected = [
        {:filters, {:status, "active"}},
        {:filters, {:role, "admin"}},
        {:meta, {:custom, "value"}}
      ]

      assert Enum.sort(actual) === Enum.sort(expected)
    end

    test "ignores empty maps and lists" do
      assert ParamPreprocessor.normalize_params(%{empty_map: %{}, empty_list: []}) === []
    end

    test "handles deeply nested map and list combinations" do
      input =
        %{
          post: %{
            title: "Deep Dive",
            metadata: %{
              stats: [total: 5],
              rating: {:custom, 10}
            },
            tags: ["elixir"],
            authors: [%{id: %{>=: 5}, name: "Dev"}]
          },
          extras: [version: "1.0"]
        }

      expected = [
        post: {:title, "Deep Dive"},
        post: {:metadata, {:stats, {:total, 5}}},
        post: {:metadata, {:rating, {:custom, 10}}},
        post: {:tags, ["elixir"]},
        post: {:authors, {:id, {:>=, 5}}},
        post: {:authors, {:name, "Dev"}},
        extras: {:version, "1.0"}
      ]

      assert Enum.sort(ParamPreprocessor.normalize_params(input)) === Enum.sort(expected)
    end

    test "preserves scalar lists inside maps" do
      actual = ParamPreprocessor.normalize_params(%{title: %{in: ["a"]}})

      expected = [
        {:title, {:in, ["a"]}}
      ]

      assert actual === expected
    end
  end
end
