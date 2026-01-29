defmodule EctoShorts.CommonSchemaTest do
  use EctoShorts.DataCase

  alias EctoShorts.CommonSchema
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.PostAbstract
  alias EctoShorts.Schema.PostAbstractHasSchemaPrefix
  alias EctoShorts.Schema.PostHasSchemaPrefix
  alias EctoShorts.Schema.User

  describe "get_schema_reflection/2" do
    test "delegates to schema module" do
      assert CommonSchema.get_schema_reflection(Post, :primary_key) == [:id]
    end

    test "delegates to tuple schema module" do
      assert CommonSchema.get_schema_reflection({"posts", PostAbstract}, :primary_key) == [
               :id
             ]
    end
  end

  describe "get_schema_reflection/3" do
    test "delegates to schema module" do
      assert CommonSchema.get_schema_reflection(Post, :type, :id) == :id
    end

    test "delegates to tuple schema module" do
      assert CommonSchema.get_schema_reflection({"posts", PostAbstract}, :type, :id) == :id
    end
  end

  describe "get_schema_prefix/1" do
    test "returns nil for schemas without @schema_prefix" do
      assert CommonSchema.get_schema_prefix(Post) == nil
    end

    test "returns prefix from schema module" do
      assert CommonSchema.get_schema_prefix(PostHasSchemaPrefix) == "custom_schema_prefix"
    end

    test "returns prefix from source tuple" do
      assert CommonSchema.get_schema_prefix({"posts", PostAbstractHasSchemaPrefix}) ==
               "custom_schema_prefix"
    end

    test "returns prefix from schema struct" do
      assert CommonSchema.get_schema_prefix(%PostHasSchemaPrefix{}) == "custom_schema_prefix"
    end
  end

  describe "get_schema_source/1" do
    import Ecto.Query

    test "returns source tuple from schema struct" do
      assert CommonSchema.get_schema_source(%Post{}) == {"posts", Post}
    end

    test "returns source tuple from schema module" do
      assert CommonSchema.get_schema_source(Post) == {"posts", Post}
    end

    test "returns source tuple from explicit source tuple" do
      assert CommonSchema.get_schema_source({"custom_posts", PostAbstract}) ==
               {"custom_posts", PostAbstract}
    end

    test "returns source tuple from query" do
      q = from(u in "users")
      assert CommonSchema.get_schema_source(q) == {"users", nil}
    end
  end

  describe "get_schema_metadata/1" do
    test "returns metadata from schema struct" do
      meta = CommonSchema.get_schema_metadata(%Post{})
      assert %Ecto.Schema.Metadata{} = meta
      assert meta.schema == Post
      assert meta.source == "posts"
    end

    test "returns metadata from changeset" do
      meta =
        %Post{}
        |> Post.changeset(%{title: "Hello"})
        |> CommonSchema.get_schema_metadata()

      assert %Ecto.Schema.Metadata{} = meta
      assert meta.schema == Post
      assert meta.source == "posts"
    end

    test "returns metadata for schema module" do
      meta = CommonSchema.get_schema_metadata(Post)
      assert %Ecto.Schema.Metadata{} = meta
      assert meta.schema == Post
      assert meta.source == "posts"
    end
  end

  describe "put_schema_metadata/2" do
    test "updates metadata on schema struct" do
      struct =
        CommonSchema.put_schema_metadata(%Post{}, state: :loaded, source: "custom_posts")

      meta = CommonSchema.get_schema_metadata(struct)

      assert meta.state == :loaded
      assert meta.source == "custom_posts"
      assert meta.schema == Post
    end

    test "builds struct for schema module and updates metadata" do
      struct = CommonSchema.put_schema_metadata(Post, state: :loaded, source: "custom_posts")
      meta = CommonSchema.get_schema_metadata(struct)

      assert meta.state == :loaded
      assert meta.source == "custom_posts"
      assert meta.schema == Post
    end

    test "builds struct for source tuple and applies tuple source" do
      struct =
        CommonSchema.put_schema_metadata({"custom_posts", PostAbstract}, state: :loaded)

      meta = CommonSchema.get_schema_metadata(struct)

      assert meta.state == :loaded
      assert meta.source == "custom_posts"
      assert meta.schema == PostAbstract
    end
  end

  describe "create_schema_struct/1" do
    test "builds a struct for a schema module" do
      assert %User{} = CommonSchema.create_schema_struct(User)
    end

    test "builds a struct for a source tuple and sets metadata" do
      struct = CommonSchema.create_schema_struct({"custom_posts", PostAbstract})
      meta = CommonSchema.get_schema_metadata(struct)

      assert meta.schema == PostAbstract
      assert meta.source == "custom_posts"
    end
  end

  describe "create_changeset/3" do
    test "creates changeset from schema module + params" do
      changeset = CommonSchema.create_changeset(Post, %{title: "Hello"}, [])

      assert %Ecto.Changeset{} = changeset
      assert changeset.changes.title == "Hello"
    end

    test "creates changeset from schema struct + params" do
      changeset = CommonSchema.create_changeset(%Post{}, %{title: "Hello"}, [])

      assert %Ecto.Changeset{} = changeset
      assert changeset.changes.title == "Hello"
    end

    test "creates changeset from changeset + params" do
      base = Post.changeset(%Post{}, %{title: "Base"})

      changeset = CommonSchema.create_changeset(base, %{title: "Override"}, [])

      assert %Ecto.Changeset{} = changeset
      assert changeset.changes.title == "Override"
    end

    test "creates changeset from {source, schema} + params and applies source" do
      assert %Ecto.Changeset{data: %{__meta__: %{source: "custom_posts"}}} =
               CommonSchema.create_changeset(
                 {"custom_posts", PostAbstract},
                 %{title: "Hello"},
                 []
               )
    end

    test "creates changeset from {source, schema} + schema struct and applies source" do
      assert %Ecto.Changeset{data: %{__meta__: %{source: "custom_posts"}}} =
               CommonSchema.create_changeset(
                 {"custom_posts", PostAbstract},
                 %PostAbstract{},
                 []
               )
    end

    test "supports :changeset callback 3-arity" do
      assert %Ecto.Changeset{data: %Post{}, changes: %{title: "Custom"}} =
               CommonSchema.create_changeset(Post, %Post{}, %{title: "Hello"},
                 changeset: fn _schema, schema_data_or_changeset, params ->
                   schema_data_or_changeset
                   |> Ecto.Changeset.change(params)
                   |> Ecto.Changeset.put_change(:title, "Custom")
                 end
               )
    end

    test "supports :changeset callback 2-arity" do
      assert %Ecto.Changeset{data: %Post{}, changes: %{title: "Custom"}} =
               CommonSchema.create_changeset(Post, %Post{}, %{title: "Hello"},
                 changeset: fn schema_data_or_changeset, params ->
                   schema_data_or_changeset
                   |> Ecto.Changeset.change(params)
                   |> Ecto.Changeset.put_change(:title, "Custom")
                 end
               )
    end

    test "supports :changeset callback 1-arity (receives a changeset)" do
      assert %Ecto.Changeset{data: %Post{}, changes: %{title: "Custom"}} =
               CommonSchema.create_changeset(Post, %Post{}, %{title: "Hello"},
                 changeset: fn changeset ->
                   Ecto.Changeset.put_change(changeset, :title, "Custom")
                 end
               )
    end

    test "raises ArgumentError for invalid :changeset callback" do
      assert_raise ArgumentError,
                   "Expected the value for option :changeset to be a 1-arity, 2-arity, or 3-arity function, got: :invalid",
                   fn ->
                     CommonSchema.create_changeset(Post, %{}, changeset: :invalid)
                   end
    end

    test "raises if callback does not return an Ecto.Changeset" do
      assert_raise RuntimeError,
                   "Expected an Ecto.Changeset, got: :not_a_changeset",
                   fn ->
                     CommonSchema.create_changeset(Post, %{},
                       changeset: fn _changeset -> :not_a_changeset end
                     )
                   end
    end
  end
end
