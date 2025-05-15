defmodule EctoShorts.CommonSchemasTest do
  use ExUnit.Case, async: true
  doctest EctoShorts.CommonSchemas

  alias EctoShorts.CommonSchemas

  alias EctoShorts.Schema.{
    AbstractPost,
    Post
  }

  import Ecto.Query, only: [from: 2]

  describe "get_reflection/2" do
    test "when given a schema module, returns the expected value" do
      assert [:id] = CommonSchemas.get_reflection(Post, :primary_key)
    end

    test "when given an abstract source tuple, returns the expected value" do
      assert [:id] = CommonSchemas.get_reflection({"posts", AbstractPost}, :primary_key)
    end
  end

  describe "get_reflection/3" do
    test "when given a schema module, returns the expected type" do
      assert :id = CommonSchemas.get_reflection(Post, :type, :id)
    end

    test "when given an abstract source tuple, returns the expected type" do
      assert :id = CommonSchemas.get_reflection({"posts", AbstractPost}, :type, :id)
    end
  end

  describe "get_schema_prefix/1" do
    test "when given a schema module, returns the @schema_prefix value" do
      assert "custom_prefix" = CommonSchemas.get_schema_prefix(Post)
    end

    test "when given an abstract source tuple, returns the @schema_prefix value" do
      assert "custom_prefix" = CommonSchemas.get_schema_prefix({"posts", AbstractPost})
    end
  end

  describe "get_source_and_schema/1" do
    test "when given a schema module, returns database table name" do
      assert {"posts", Post} = CommonSchemas.get_source_and_schema(Post)
    end

    test "when given an abstract source tuple, returns database table name" do
      assert {"posts", AbstractPost} =
               CommonSchemas.get_source_and_schema({"posts", AbstractPost})
    end

    test "when given an ecto query, returns {binary, queryable}" do
      query =
        from p in Post,
          as: :posts,
          join: c in Comment,
          as: :comments,
          on: c.post_id == p.id

      assert {"posts", Post} = CommonSchemas.get_source_and_schema(query)
    end
  end

  describe "get_schema_module/1" do
    test "when given a schema module, returns expected schema module" do
      assert Post = CommonSchemas.get_schema_module(Post)
    end

    test "when given an abstract source tuple, returns expected schema module" do
      assert AbstractPost = CommonSchemas.get_schema_module({"posts", AbstractPost})
    end

    test "when given an ecto query, returns expected schema module" do
      query =
        from p in Post,
          as: :posts,
          join: c in Comment,
          as: :comments,
          on: c.post_id == p.id

      assert Post = CommonSchemas.get_schema_module(query)
    end
  end
end
