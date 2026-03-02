defmodule EctoShorts.CommonSchemaTest do
  use EctoShorts.DataCase, async: true

  alias Ecto.Changeset
  alias EctoShorts.CommonSchema
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.PostAbstract
  alias EctoShorts.Schema.PostAbstractHasSchemaPrefix
  alias EctoShorts.Schema.PostHasSchemaPrefix
  alias EctoShorts.Schema.User

  describe "get_schema_reflection/2" do
    test "returns schema information for a schema module" do
      assert CommonSchema.get_schema_reflection(Post, :primary_key) === [:id]
    end

    test "returns schema information for a source tuple" do
      assert CommonSchema.get_schema_reflection({"posts", PostAbstract}, :primary_key) === [
               :id
             ]
    end
  end

  describe "get_schema_reflection/3" do
    test "returns the field type for a schema module" do
      assert CommonSchema.get_schema_reflection(Post, :type, :id) === :id
    end

    test "returns the field type for a source tuple" do
      assert CommonSchema.get_schema_reflection({"posts", PostAbstract}, :type, :id) === :id
    end
  end

  describe "get_schema_prefix/1" do
    test "returns nil when the schema has no prefix" do
      assert CommonSchema.get_schema_prefix(Post) === nil
    end

    test "returns the prefix defined on the schema module" do
      assert CommonSchema.get_schema_prefix(PostHasSchemaPrefix) === "custom_schema_prefix"
    end

    test "returns the prefix from a source tuple" do
      assert CommonSchema.get_schema_prefix({"posts", PostAbstractHasSchemaPrefix}) ===
               "custom_schema_prefix"
    end

    test "returns the prefix from a schema struct" do
      assert CommonSchema.get_schema_prefix(%PostHasSchemaPrefix{}) === "custom_schema_prefix"
    end
  end

  describe "get_schema_source/1" do
    import Ecto.Query

    test "returns source tuple from schema struct" do
      assert CommonSchema.get_schema_source(%Post{}) === {"posts", Post}
    end

    test "returns source tuple from schema module" do
      assert CommonSchema.get_schema_source(Post) === {"posts", Post}
    end

    test "returns source tuple from explicit source tuple" do
      assert CommonSchema.get_schema_source({"custom_posts", PostAbstract}) ===
               {"custom_posts", PostAbstract}
    end

    test "returns source tuple from query" do
      q = from(u in "users")
      assert CommonSchema.get_schema_source(q) === {"users", nil}
    end
  end

  describe "get_schema_metadata/1" do
    test "returns metadata from schema struct" do
      meta = CommonSchema.get_schema_metadata(%Post{})
      assert %Ecto.Schema.Metadata{} = meta
      assert meta.schema === Post
      assert meta.source === "posts"
    end

    test "returns metadata from changeset" do
      meta =
        %Post{}
        |> Post.changeset(%{title: "Hello"})
        |> CommonSchema.get_schema_metadata()

      assert %Ecto.Schema.Metadata{} = meta
      assert meta.schema === Post
      assert meta.source === "posts"
    end

    test "returns metadata for schema module" do
      meta = CommonSchema.get_schema_metadata(Post)
      assert %Ecto.Schema.Metadata{} = meta
      assert meta.schema === Post
      assert meta.source === "posts"
    end
  end

  describe "put_schema_metadata/2" do
    test "updates metadata on schema struct" do
      struct =
        CommonSchema.put_schema_metadata(%Post{}, state: :loaded, source: "custom_posts")

      meta = CommonSchema.get_schema_metadata(struct)

      assert meta.state === :loaded
      assert meta.source === "custom_posts"
      assert meta.schema === Post
    end

    test "builds struct for schema module and updates metadata" do
      struct = CommonSchema.put_schema_metadata(Post, state: :loaded, source: "custom_posts")
      meta = CommonSchema.get_schema_metadata(struct)

      assert meta.state === :loaded
      assert meta.source === "custom_posts"
      assert meta.schema === Post
    end

    test "builds struct for source tuple and applies tuple source" do
      struct =
        CommonSchema.put_schema_metadata({"custom_posts", PostAbstract}, state: :loaded)

      meta = CommonSchema.get_schema_metadata(struct)

      assert meta.state === :loaded
      assert meta.source === "custom_posts"
      assert meta.schema === PostAbstract
    end
  end

  describe "build_struct/1" do
    test "builds a struct for a schema module" do
      assert %User{} = CommonSchema.build_struct(User)
    end

    test "builds a struct for a source tuple and sets metadata" do
      struct = CommonSchema.build_struct({"custom_posts", PostAbstract})
      meta = CommonSchema.get_schema_metadata(struct)

      assert meta.schema === PostAbstract
      assert meta.source === "custom_posts"
    end
  end

  describe "create_changeset/3" do
    test "builds a changeset from a schema module and params" do
      changeset = CommonSchema.create_changeset(Post, %{title: "Hello"}, [])

      assert %Changeset{} = changeset
      assert changeset.changes.title === "Hello"
    end

    test "builds a changeset from a schema struct and params" do
      changeset = CommonSchema.create_changeset(%Post{}, %{title: "Hello"}, [])

      assert %Changeset{} = changeset
      assert changeset.changes.title === "Hello"
    end

    test "builds a changeset from an existing changeset and new params" do
      base = Post.changeset(%Post{}, %{title: "Base"})

      changeset = CommonSchema.create_changeset(base, %{title: "Override"}, [])

      assert %Changeset{} = changeset
      assert changeset.changes.title === "Override"
    end

    test "builds a changeset from a source tuple and params" do
      assert %Changeset{data: %{__meta__: %{source: "custom_posts"}}} =
               CommonSchema.create_changeset(
                 {"custom_posts", PostAbstract},
                 %{title: "Hello"},
                 []
               )
    end

    test "builds a changeset from a source tuple and a schema struct" do
      assert %Changeset{data: %{__meta__: %{source: "custom_posts"}}} =
               CommonSchema.create_changeset(
                 {"custom_posts", PostAbstract},
                 %PostAbstract{},
                 []
               )
    end

    test "uses a 3-argument changeset callback from the options" do
      assert %Changeset{data: %Post{}, changes: %{title: "Custom"}} =
               CommonSchema.create_changeset(Post, %Post{}, %{title: "Hello"},
                 changeset: fn _schema, schema_data_or_changeset, params ->
                   schema_data_or_changeset
                   |> Changeset.change(params)
                   |> Changeset.put_change(:title, "Custom")
                 end
               )
    end

    test "uses a 2-argument changeset callback from the options" do
      assert %Changeset{data: %Post{}, changes: %{title: "Custom"}} =
               CommonSchema.create_changeset(Post, %Post{}, %{title: "Hello"},
                 changeset: fn schema_data_or_changeset, params ->
                   schema_data_or_changeset
                   |> Changeset.change(params)
                   |> Changeset.put_change(:title, "Custom")
                 end
               )
    end

    test "uses a 1-argument changeset callback from the options" do
      assert %Changeset{data: %Post{}, changes: %{title: "Custom"}} =
               CommonSchema.create_changeset(Post, %Post{}, %{title: "Hello"},
                 changeset: fn changeset ->
                   Changeset.put_change(changeset, :title, "Custom")
                 end
               )
    end

    test "raises when the changeset option is not a function" do
      assert_raise ArgumentError,
                   "Expected the value for option :changeset to be a 1-arity, 2-arity, or 3-arity function, got: :invalid",
                   fn ->
                     CommonSchema.create_changeset(Post, %{}, changeset: :invalid)
                   end
    end

    test "raises when the changeset callback does not return a changeset" do
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
