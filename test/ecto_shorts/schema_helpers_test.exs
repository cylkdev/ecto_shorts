defmodule EctoShorts.SchemaHelpersTest do
  use ExUnit.Case, async: true

  alias EctoShorts.SchemaHelpers
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.Comment
  alias EctoShorts.Schema.User

  describe "get_related_schema/2" do
    test "returns nil for nil schema" do
      assert SchemaHelpers.get_related_schema(nil, :comments) === nil
    end

    test "returns related schema for through association" do
      assert SchemaHelpers.get_related_schema(Post, :comments_authors) === User
    end

    test "returns nil for non-existent association" do
      assert SchemaHelpers.get_related_schema(Post, :does_not_exist) === nil
    end
  end

  describe "schema_field_type/2" do
    test "returns the Ecto type for a field" do
      assert SchemaHelpers.schema_field_type(Post, :title) === :string
    end

    test "returns array type for array fields" do
      assert SchemaHelpers.schema_field_type(Post, :tags) === {:array, :string}
    end
  end

  describe "association_not_loaded?/2" do
    test "returns true when association is not loaded" do
      post = %Post{}

      assert SchemaHelpers.association_not_loaded?(post, :comments) === true
    end

    test "returns false when association is loaded" do
      post = %Post{comments: []}

      assert SchemaHelpers.association_not_loaded?(post, :comments) === false
    end
  end

  describe "all_schema_struct?/1" do
    test "returns false for empty list" do
      assert SchemaHelpers.all_schema_struct?([]) === false
    end

    test "returns false for empty map" do
      assert SchemaHelpers.all_schema_struct?(%{}) === false
    end

    test "returns true when all items are schema structs" do
      assert SchemaHelpers.all_schema_struct?([%Post{}, %Comment{}]) === true
    end

    test "returns false when mixed with plain maps" do
      assert SchemaHelpers.all_schema_struct?([%Post{}, %{id: 1}]) === false
    end
  end

  describe "any_schema_struct?/1" do
    test "returns true when any item is a schema struct" do
      assert SchemaHelpers.any_schema_struct?([%Post{}, %{id: 1}]) === true
    end

    test "returns false when no items are schema structs" do
      assert SchemaHelpers.any_schema_struct?([%{id: 1}]) === false
    end
  end

  describe "schema_module?/1" do
    test "returns false for non-atom values" do
      assert SchemaHelpers.schema_module?("not_a_module") === false
      assert SchemaHelpers.schema_module?(123) === false
    end
  end

  describe "any_created?/1" do
    test "returns true for map with non-nil atom :id key" do
      assert SchemaHelpers.any_created?(%{id: 42}) === true
    end

    test "returns false for map with nil atom :id key" do
      assert SchemaHelpers.any_created?(%{id: nil}) === false
    end

    test "returns true for map with non-nil string id key" do
      assert SchemaHelpers.any_created?(%{"id" => 99}) === true
    end

    test "returns false for map with nil string id key" do
      assert SchemaHelpers.any_created?(%{"id" => nil}) === false
    end
  end
end
