defmodule EctoShorts.ActionsTest do
  @moduledoc false
  use EctoShorts.DataCase

  alias EctoShorts.Actions

  alias EctoShorts.Repo

  alias EctoShorts.Schemas.Post

  def insert!(repo, schema_module, params) do
    schema_module
    |> struct!()
    |> schema_module.changeset(params)
    |> repo.insert!()
  end

  describe "&delete_many/3" do
    test "can delete structs" do
      post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {:ok, [%Post{title: "post_title_1"}, %Post{title: "post_title_2"}]} =
               Actions.delete_many([post_1, post_2])
    end

    test "can delete changesets" do
      post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      post_1_changeset = Post.changeset(post_1)

      post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      post_2_changeset = Post.changeset(post_2)

      assert {:ok, [%Post{title: "post_title_1"}, %Post{title: "post_title_2"}]} =
               Actions.delete_many([post_1_changeset, post_2_changeset])
    end
  end

  describe "&find_and_create/3" do
    test "returns existing record" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "existing_post_title"}} =
               Actions.find_and_create(Post, %{title: "existing_post_title"}, %{
                 title: "created_post_title"
               })
    end

    test "creates record if not found" do
      assert {:ok, %Post{title: "created_post_title"}} =
               Actions.find_and_create(Post, %{title: "existing_post_title"}, %{
                 title: "created_post_title"
               })
    end
  end

  describe "&find_and_update/2" do
    test "can find and update record matching params" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "updated_post_title"}} =
               Actions.find_and_update(
                 Post,
                 %{title: "existing_post_title"},
                 %{title: "updated_post_title"}
               )
    end

    test "returns error if record not found" do
      assert {:error, %{code: :not_found}} =
               Actions.find_and_update(Post, %{title: "does_not_exist"}, %{})
    end
  end

  describe "&find_and_upsert/2" do
    test "creates record if one does not exist" do
      assert {:ok, %Post{title: "existing_post_title"}} =
               Actions.find_and_upsert(
                 Post,
                 %{title: "existing_post_title"},
                 %{title: "existing_post_title"}
               )
    end

    test "can find existing record matching params and update it if it exists or creates record if one does not exist" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "updated_post_title"}} =
               Actions.find_and_upsert(
                 Post,
                 %{title: "existing_post_title"},
                 %{title: "updated_post_title"}
               )
    end
  end

  describe "&find_and_delete/2" do
    test "can find existing record matching params and delete it" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, %Post{title: "post_title"}} =
               Actions.find_and_delete(Post, %{title: "post_title"})
    end

    test "returns error if record not found" do
      assert {:error, %{code: :not_found}} =
               Actions.find_and_delete(Post, %{title: "does_not_exist"})
    end
  end

  describe "&find_or_create/3" do
    test "returns existing record" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "post_title"}} =
               Actions.find_or_create(Post, %{title: "post_title"})
    end

    test "creates record if not found" do
      assert {:ok, %Post{title: "post_title"}} =
               Actions.find_or_create(Post, %{title: "post_title"})
    end
  end

  describe "&get/2" do
    test "returns record matching id" do
      post = insert!(Repo, Post, %{title: "post_title"})

      assert %Post{title: "post_title"} = Actions.get(Post, post.id)
    end
  end

  describe "&all/1" do
    test "returns all records" do
      post =
        insert!(Repo, Post, %{
          title: "post_title",
          unique_identifier: "post_unique_identifier",
          tags: ["post_tag"],
          views: 1
        })

      assert %Post{
               title: "post_title",
               unique_identifier: "post_unique_identifier",
               tags: ["post_tag"],
               views: 1
             } = post

      assert [^post] = Actions.all(Post)
    end
  end

  describe "&all/2" do
    test "returns records matching params" do
      _post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert [%Post{title: "post_title_1"}] = Actions.all(Post, %{title: "post_title_1"})
    end
  end

  describe "&create/2" do
    test "inserts record with expected params" do
      assert {:ok, %Post{title: "post_title"}} = Actions.create(Post, %{title: "post_title"})
    end

    test "returns changeset error when constraint violation occurs" do
      assert {:ok, %Post{unique_identifier: "post_unique_identifier"}} =
               Actions.create(Post, %{unique_identifier: "post_unique_identifier"})

      assert {:error, changeset} =
               Actions.create(Post, %{unique_identifier: "post_unique_identifier"})

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)
    end
  end

  describe "&find/2" do
    test "returns record matching parameters" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, %Post{title: "post_title"}} = Actions.find(Post, %{title: "post_title"})
    end

    test "returns error when record not found" do
      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "Record not found.",
                details: %{
                  params: %{title: "post_title"},
                  query: EctoShorts.Schemas.Post
                }
              }} = Actions.find(Post, %{title: "post_title"})
    end
  end

  describe "&update/3" do
    test "can update record by id" do
      post = insert!(Repo, Post, %{title: "created_title"})

      post_id = post.id

      assert {:ok, %Post{id: ^post_id, title: "updated_title"}} =
               Actions.update(Post, post_id, %{title: "updated_title"})
    end

    test "can update record by struct" do
      post = insert!(Repo, Post, %{title: "created_title"})

      assert {:ok, %Post{title: "updated_title"}} =
               Actions.update(Post, post, %{title: "updated_title"})
    end
  end

  describe "delete/1" do
    test "can delete record by struct" do
      post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, %Post{title: "post_title"}} = Actions.delete(post)
    end

    test "can delete record by changeset" do
      post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, %Post{title: "post_title"}} =
               post
               |> Post.changeset(%{})
               |> Actions.delete()
    end

    test "can delete many structs" do
      post_1 = insert!(Repo, Post, %{title: "post_title_1"})
      post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {:ok,
              [
                %Post{title: "post_title_1"},
                %Post{title: "post_title_2"}
              ]} = Actions.delete([post_1, post_2])
    end

    test "can delete many changesets" do
      post_1 = insert!(Repo, Post, %{title: "post_title_1"})
      post_1_changeset = Post.changeset(post_1, %{})

      post_2 = insert!(Repo, Post, %{title: "post_title_2"})
      post_2_changeset = Post.changeset(post_2, %{})

      assert {:ok,
              [
                %Post{title: "post_title_1"},
                %Post{title: "post_title_2"}
              ]} = Actions.delete([post_1_changeset, post_2_changeset])
    end
  end

  describe "delete/2" do
    test "can delete record by id" do
      post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, %Post{title: "post_title"}} = Actions.delete(Post, post.id)
    end
  end

  describe "stream/2" do
    test "returns all records matching params" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, [%Post{title: "post_title"}]} =
               Repo.transaction(fn ->
                 Post
                 |> Actions.stream(%{})
                 |> Enum.to_list()
               end)
    end
  end

  describe "aggregate/4" do
    test "returns expected value for count" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert 1 = Actions.aggregate(Post, %{}, :count, :id)
    end
  end

  describe "transaction/2" do
    test "handles non status tuple response" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, [%Post{title: "post_title"}]} =
               Actions.transaction(fn ->
                 Actions.all(Post, %{})
               end)
    end

    test "handles ok status tuple response" do
      assert {:ok, %Post{title: "post_title"}} =
               Actions.transaction(fn ->
                 Actions.create(Post, %{title: "post_title"})
               end)
    end

    test "does not commit change when function returns :error atom" do
      assert :error =
               Actions.transaction(fn ->
                 with {:ok, _} <- Actions.create(Post, %{title: "post_title"}) do
                   :error
                 end
               end)

      assert {:error, %{code: :not_found}} = Actions.find(Post, %{title: "post_title"})
    end

    test "does not commit change when function returns error status tuple" do
      assert {:error, "message"} =
               Actions.transaction(fn ->
                 with {:ok, _} <-
                        Actions.create(Post, %{unique_identifier: "post_unique_identifier"}) do
                   {:error, "message"}
                 end
               end)

      assert {:error, %{code: :not_found}} =
               Actions.find(Post, %{unique_identifier: "post_unique_identifier"})
    end

    test "transaction is rolled back when a constraint violation occurs" do
      assert {:error, %Ecto.Changeset{}} =
               Actions.transaction(fn ->
                 with {:ok, _} <-
                        Actions.create(Post, %{unique_identifier: "post_unique_identifier"}) do
                   Actions.create(Post, %{unique_identifier: "post_unique_identifier"})
                 end
               end)

      assert {:error, %{code: :not_found}} =
               Actions.find(Post, %{unique_identifier: "post_unique_identifier"})
    end
  end
end
