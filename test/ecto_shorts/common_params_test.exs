defmodule EctoShorts.CommonParamsTest do
  use ExUnit.Case, async: true

  alias EctoShorts.CommonParams
  alias EctoShorts.Schema.Post

  describe "convert_to_update_params/3 with schema module source" do
    test "happy path: groups update operators and adds updated_at by default" do
      updates =
        CommonParams.convert_to_update_params(Post, %{
          title: "Hello",
          views: {:inc, 1},
          tags: {:push, "elixir"}
        })

      assert [{:inc, inc_ops}, {:push, push_ops}, {:set, set_ops}] = updates

      assert inc_ops === [views: 1]
      assert push_ops === [tags: "elixir"]

      assert Keyword.fetch!(set_ops, :title) === "Hello"
      assert %NaiveDateTime{} = Keyword.fetch!(set_ops, :updated_at)
    end

    test "failure path: raises when using :push on a non-array field" do
      assert_raise ArgumentError, fn ->
        CommonParams.convert_to_update_params(Post, %{views: {:push, "oops"}})
      end
    end

    test "does not add updated_at when updated_at is false" do
      updates =
        CommonParams.convert_to_update_params(
          Post,
          %{title: "Hello"},
          updated_at: false
        )

      assert [{:set, set_ops}] = updates
      assert set_ops === [title: "Hello"]
    end

    test "does not add updated_at when updated_at_source is false" do
      updates =
        CommonParams.convert_to_update_params(
          Post,
          %{title: "Hello"},
          updated_at_source: false
        )

      assert [{:set, set_ops}] = updates
      assert set_ops === [title: "Hello"]
    end
  end

  describe "convert_to_update_params/3 update operation variants" do
    test "explicit {:set, value} tuple" do
      updates = CommonParams.convert_to_update_params(Post, %{title: {:set, "Explicit"}})

      assert [{:set, set_ops}] = updates
      assert Keyword.fetch!(set_ops, :title) === "Explicit"
    end

    test "list of update ops on a single field" do
      updates =
        CommonParams.convert_to_update_params(Post, %{
          tags: [{:push, "new_tag"}, {:pull, "old_tag"}]
        })

      assert Keyword.has_key?(updates, :pull)
      assert Keyword.has_key?(updates, :push)
    end

    test ":inc with non-integer value raises ArgumentError" do
      assert_raise ArgumentError, ~r/Expected value for key .* to be an integer/, fn ->
        CommonParams.convert_to_update_params(Post, %{views: {:inc, "bad"}})
      end
    end

    test ":inc with non-integer field raises ArgumentError" do
      assert_raise ArgumentError, ~r/is not a type of `:integer`/, fn ->
        CommonParams.convert_to_update_params(Post, %{title: {:inc, 1}})
      end
    end
  end

  describe "build_on_conflict_options/3" do
    test "on_conflict_replace: :none returns conflict_target without on_conflict" do
      inserts = [%{id: 1, title: "Hello"}]

      opts =
        CommonParams.build_on_conflict_options(Post, inserts, on_conflict_replace: :none)

      assert Keyword.fetch!(opts, :conflict_target) === [:id]
      refute Keyword.has_key?(opts, :on_conflict)
    end

    test "on_conflict_replace: invalid raises ArgumentError" do
      inserts = [%{id: 1, title: "Hello"}]

      assert_raise ArgumentError, ~r/Expected :on_conflict_replace/, fn ->
        CommonParams.build_on_conflict_options(Post, inserts, on_conflict_replace: :bad)
      end
    end
  end

  describe "convert_to_update_params/3 with schemaless sources" do
    test "source nil: allows all keys and defaults updated_at to DateTime" do
      updates =
        CommonParams.convert_to_update_params(nil, %{
          made_up_field: "value",
          updated_at: ~U[2026-01-01 00:00:00Z]
        })

      assert [{:set, set_ops}] = updates

      assert Keyword.fetch!(set_ops, :made_up_field) === "value"
      assert %DateTime{} = Keyword.fetch!(set_ops, :updated_at)
    end

    test "source binary: treated as schemaless" do
      updates =
        CommonParams.convert_to_update_params("posts", %{
          made_up_field: "value",
          updated_at: ~U[2026-01-01 00:00:00Z]
        })

      assert [{:set, set_ops}] = updates

      assert Keyword.fetch!(set_ops, :made_up_field) === "value"
      assert %DateTime{} = Keyword.fetch!(set_ops, :updated_at)
    end

    test "{binary, nil}: treated as schemaless" do
      updates =
        CommonParams.convert_to_update_params({"posts", nil}, %{
          made_up_field: "value",
          updated_at: ~U[2026-01-01 00:00:00Z]
        })

      assert [{:set, set_ops}] = updates

      assert Keyword.fetch!(set_ops, :made_up_field) === "value"
      assert %DateTime{} = Keyword.fetch!(set_ops, :updated_at)
    end

    test "{binary, module}: treated like schema module (filters unknown keys)" do
      updates =
        CommonParams.convert_to_update_params({"posts", Post}, %{
          title: "Hello",
          made_up_field: "value"
        })

      assert [{:set, set_ops}] = updates

      assert Keyword.fetch!(set_ops, :title) === "Hello"
      refute Keyword.has_key?(set_ops, :made_up_field)
    end
  end

  describe "convert_to_insert_params/3" do
    test "happy path: builds insert maps and adds timestamps by default" do
      assert {:ok, insert_maps} =
               CommonParams.convert_to_insert_params(Post, [%{title: "Hello"}], validate: false)

      assert [insert_map] = insert_maps

      assert insert_map.title === "Hello"
      assert %NaiveDateTime{} = insert_map.inserted_at
      assert %NaiveDateTime{} = insert_map.updated_at
    end

    test "happy path: replaces placeholder values with {:placeholder, field}" do
      placeholders = %{permalink: "__PLACEHOLDER__"}

      assert {:ok, insert_maps} =
               CommonParams.convert_to_insert_params(
                 Post,
                 [%{title: "Hello", permalink: "__PLACEHOLDER__"}],
                 validate: false,
                 placeholders: placeholders
               )

      assert [%{permalink: {:placeholder, :permalink}}] = insert_maps
    end

    test "failure path: returns {:error, [changeset]} when validation fails" do
      assert {:error, [changeset]} =
               CommonParams.convert_to_insert_params(Post, [%{views: "oops"}])

      assert %Ecto.Changeset{valid?: false} = changeset
      assert Keyword.has_key?(changeset.errors, :views)
    end
  end

  describe "convert_to_insert_params/3 with struct entry" do
    test "accepts a bare schema struct as insert entry" do
      struct = %Post{title: "From Struct", published: true}

      assert {:ok, [insert_map]} =
               CommonParams.convert_to_insert_params(Post, [struct], validate: false)

      assert insert_map.title === "From Struct"
      assert insert_map.published === true
    end

    test "accepts a {struct, params} tuple as insert entry" do
      struct = %Post{title: "Original"}
      params = %{title: "Overridden"}

      assert {:ok, [insert_map]} =
               CommonParams.convert_to_insert_params(Post, [{struct, params}], validate: false)

      assert insert_map.title === "Overridden"
    end

    test "accepts a changeset as insert entry" do
      changeset = Post.changeset(%Post{}, %{title: "From Changeset"})

      assert {:ok, [insert_map]} =
               CommonParams.convert_to_insert_params(Post, [changeset])

      assert insert_map.title === "From Changeset"
    end

    test "accepts keyword list params as insert entry" do
      assert {:ok, [insert_map]} =
               CommonParams.convert_to_insert_params(Post, [[title: "KW"]], validate: false)

      assert insert_map.title === "KW"
    end
  end

  describe "convert_to_insert_params/3 with schemaless sources" do
    test "source nil: allows arbitrary keys (including string keys)" do
      assert {:ok, [insert_map]} =
               CommonParams.convert_to_insert_params(nil, [%{"made_up_field" => "value"}])

      assert insert_map["made_up_field"] === "value"
      assert %DateTime{} = insert_map.updated_at
    end

    test "source binary: treated as schemaless" do
      assert {:ok, [insert_map]} =
               CommonParams.convert_to_insert_params("posts", [%{"made_up_field" => "value"}])

      assert insert_map["made_up_field"] === "value"
      assert %DateTime{} = insert_map.updated_at
    end

    test "{binary, nil}: treated as schemaless" do
      assert {:ok, [insert_map]} =
               CommonParams.convert_to_insert_params({"posts", nil}, [
                 %{"made_up_field" => "value"}
               ])

      assert insert_map["made_up_field"] === "value"
      assert %DateTime{} = insert_map.updated_at
    end

    test "{binary, module}: treated like schema module (filters unknown keys)" do
      assert {:ok, [insert_map]} =
               CommonParams.convert_to_insert_params(
                 {"posts", Post},
                 [%{title: "Hello", made_up_field: "value"}],
                 validate: false
               )

      assert insert_map.title === "Hello"
      refute Map.has_key?(insert_map, :made_up_field)
    end
  end
end
