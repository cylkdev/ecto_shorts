defmodule EctoShorts.CommonBatchesTest do
  @moduledoc false
  use EctoShorts.DataCase

  alias EctoShorts.{
    Actions,
    CommonBatches,
    Support.Schemas.Comment,
    Support.Schemas.Post
  }

  describe "batch_all/5 : " do
    test "queryable - returns results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        CommonBatches.batch_all(Post, :id, [post_id], %{title: "post_created_title"}, :set)
    end

    test "{source, queryable} - returns results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        CommonBatches.batch_all({"posts", Post}, :id, [post_id], %{title: "post_created_title"}, :set)
    end

    test "query - returns results matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        CommonBatches.batch_all(query, :id, [post_id], %{title: "post_created_title"}, :set)
    end

    test "queryable - returns a map with values as a single result" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        CommonBatches.batch_all(Post, :id, [post_id], %{}, :set)
    end

    test "{source, queryable} - returns a map with values as a single result" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        CommonBatches.batch_all({"posts", Post}, :id, [post_id], %{}, :set)
    end

    test "query - returns a map with values as a single result" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        CommonBatches.batch_all(query, :id, [post_id], %{}, :set)
    end

    test "queryable - returns a map with values as lists of results" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        CommonBatches.batch_all(Post, :id, [post_id], %{}, :bag)
    end

    test "{source, queryable} - returns a map with values as lists of results" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        CommonBatches.batch_all({"posts", Post}, :id, [post_id], %{}, :bag)
    end

    test "query - returns a map with values as lists of results" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        CommonBatches.batch_all(query, :id, [post_id], %{}, :bag)
    end
  end

  describe "batch_all/6 : " do
    test "queryable - returns results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        CommonBatches.batch_all(Post, :id, [post_id], %{title: "post_created_title"}, :set, [])
    end

    test "{source, queryable} - returns results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        CommonBatches.batch_all({"posts", Post}, :id, [post_id], %{title: "post_created_title"}, :set, [])
    end

    test "query - returns results matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        CommonBatches.batch_all(query, :id, [post_id], %{title: "post_created_title"}, :set, [])
    end

    test "queryable - returns a map with values as a single result" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        CommonBatches.batch_all(Post, :id, [post_id], %{}, :set, [])
    end

    test "{source, queryable} - returns a map with values as a single result" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        CommonBatches.batch_all({"posts", Post}, :id, [post_id], %{}, :set, [])
    end

    test "query - returns a map with values as a single result" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        CommonBatches.batch_all(query, :id, [post_id], %{}, :set, [])
    end

    test "queryable - returns a map with values as lists of results" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        CommonBatches.batch_all(Post, :id, [post_id], %{}, :bag, [])
    end

    test "{source, queryable} - returns a map with values as lists of results" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        CommonBatches.batch_all({"posts", Post}, :id, [post_id], %{}, :bag, [])
    end

    test "query - returns a map with values as lists of results" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        CommonBatches.batch_all(query, :id, [post_id], %{}, :bag, [])
    end
  end
end
