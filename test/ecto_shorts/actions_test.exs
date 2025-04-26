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

  describe "delete_all/2" do
    test "deletes all records matching params" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {1, nil} = Actions.delete_all(Post, %{})
    end

    test "deletes all records matching params and returns records with select" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {1, [%Post{title: "post_title"}]} = Actions.delete_all(Post, %{select: true})
    end
  end

  describe "find_or_create_many/3" do
    test "creates records" do
      assert {:ok,
              [
                %Post{title: "post_title_1"},
                %Post{title: "post_title_2"}
              ]} =
               Actions.find_or_create_many(Post, [
                 %{title: "post_title_1"},
                 %{title: "post_title_2"}
               ])
    end

    test "returns existing records" do
      _post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {:ok,
              [
                %Post{title: "post_title_1"},
                %Post{title: "post_title_2"}
              ]} =
               Actions.find_or_create_many(Post, [
                 %{title: "post_title_1"},
                 %{title: "post_title_2"}
               ])
    end

    test "returns error when constraint violation occurs" do
      assert {:error,
              %ErrorMessage{
                code: :conflict,
                message: "Failed to create record.",
                details: %{
                  changes_so_far: [%Post{title: "post_title_1"}],
                  changeset: changeset,
                  position: 1,
                  params: [
                    %{title: "post_title_1", unique_identifier: "post_unique_identifier"},
                    %{title: "post_title_2", unique_identifier: "post_unique_identifier"}
                  ]
                }
              }} =
               Actions.find_or_create_many(Post, [
                 %{title: "post_title_1", unique_identifier: "post_unique_identifier"},
                 %{title: "post_title_2", unique_identifier: "post_unique_identifier"}
               ])

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)
    end
  end

  describe "find_and_update_many/3" do
    test "updates existing records" do
      _post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {:ok,
              [
                %Post{title: "updated_post_title_1"},
                %Post{title: "updated_post_title_2"}
              ]} =
               Actions.find_and_update_many(Post, [
                 {%{title: "post_title_1"}, %{title: "updated_post_title_1"}},
                 {%{title: "post_title_2"}, %{title: "updated_post_title_2"}}
               ])
    end
  end

  describe "find_and_upsert_many/3" do
    test "creates records" do
      assert {:ok,
              [
                %Post{title: "created_post_title_1"},
                %Post{title: "created_post_title_2"}
              ]} =
               Actions.find_and_upsert_many(Post, [
                 {%{title: "post_1_does_not_exist"}, %{title: "created_post_title_1"}},
                 {%{title: "post_2_does_not_exist"}, %{title: "created_post_title_2"}}
               ])
    end

    test "updates existing records" do
      _post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {:ok,
              [
                %Post{title: "updated_post_title_1"},
                %Post{title: "updated_post_title_2"}
              ]} =
               Actions.find_and_upsert_many(Post, [
                 {%{title: "post_title_1"}, %{title: "updated_post_title_1"}},
                 {%{title: "post_title_2"}, %{title: "updated_post_title_2"}}
               ])
    end
  end

  describe "create_many/3" do
    test "creates multiple records" do
      assert {:ok,
              [
                %Post{title: "post_title_1"},
                %Post{title: "post_title_2"}
              ]} =
               Actions.create_many(Post, [
                 %{title: "post_title_1"},
                 %{title: "post_title_2"}
               ])
    end
  end

  describe "find_many/3" do
    test "returns all records matching params" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, [%Post{title: "post_title"}]} =
               Actions.find_many(Post, [%{title: "post_title"}])
    end

    test "returns error when no record matches params" do
      assert {
               :error,
               %ErrorMessage{
                 code: :not_found,
                 message: "Record not found.",
                 details: %{
                   changes_so_far: %{},
                   params: [%{title: "does_not_exist"}],
                   failing_value: %{title: "does_not_exist"},
                   position: 0,
                   query: EctoShorts.Schemas.Post
                 }
               }
             } = Actions.find_many(Post, [%{title: "does_not_exist"}])
    end
  end

  describe "delete_many/3" do
    test "successfully deletes multiple struct records" do
      post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {:ok, [%Post{title: "post_title_1"}, %Post{title: "post_title_2"}]} =
               Actions.delete_many([post_1, post_2])
    end

    test "successfully deletes multiple records given changesets" do
      post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      post_1_changeset = Post.changeset(post_1)

      post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      post_2_changeset = Post.changeset(post_2)

      assert {:ok, [%Post{title: "post_title_1"}, %Post{title: "post_title_2"}]} =
               Actions.delete_many([post_1_changeset, post_2_changeset])
    end
  end

  describe "find_and_create/3" do
    test "returns existing record when one matches the params" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "existing_post_title"}} =
               Actions.find_and_create(Post, %{title: "existing_post_title"}, %{
                 title: "created_post_title"
               })
    end

    test "creates a new record with params when no match is found" do
      assert {:ok, %Post{title: "created_post_title"}} =
               Actions.find_and_create(Post, %{title: "existing_post_title"}, %{
                 title: "created_post_title"
               })
    end
  end

  describe "find_and_update/2" do
    test "updates an existing record when found with the params" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "updated_post_title"}} =
               Actions.find_and_update(
                 Post,
                 %{title: "existing_post_title"},
                 %{title: "updated_post_title"}
               )
    end

    test "returns not_found error when no record matches the params" do
      assert {:error, %{code: :not_found}} =
               Actions.find_and_update(Post, %{title: "does_not_exist"}, %{})
    end
  end

  describe "find_and_upsert/2" do
    test "creates a new record when no matching record exists" do
      assert {:ok, %Post{title: "existing_post_title"}} =
               Actions.find_and_upsert(
                 Post,
                 %{title: "existing_post_title"},
                 %{title: "existing_post_title"}
               )
    end

    test "updates existing record when found, otherwise creates a new one" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "updated_post_title"}} =
               Actions.find_and_upsert(
                 Post,
                 %{title: "existing_post_title"},
                 %{title: "updated_post_title"}
               )
    end
  end

  describe "find_and_delete/2" do
    test "successfully deletes an existing record that matches the params" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, %Post{title: "post_title"}} =
               Actions.find_and_delete(Post, %{title: "post_title"})
    end

    test "returns not_found error when no matching record exists" do
      assert {:error, %{code: :not_found}} =
               Actions.find_and_delete(Post, %{title: "does_not_exist"})
    end
  end

  describe "find_or_create/3" do
    test "returns existing record when one matches the search parameters" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "post_title"}} =
               Actions.find_or_create(Post, %{title: "post_title"})
    end

    test "creates a new record with given parameters when no match is found" do
      assert {:ok, %Post{title: "post_title"}} =
               Actions.find_or_create(Post, %{title: "post_title"})
    end
  end

  describe "get/2" do
    test "retrieves a record by its primary key (id)" do
      post = insert!(Repo, Post, %{title: "post_title"})

      assert %Post{title: "post_title"} = Actions.get(Post, post.id)
    end
  end

  describe "all/1" do
    test "returns all records from the schema with all attributes properly set" do
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

  describe "all/2" do
    test "returns only records that match the given filter parameters" do
      _post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert [%Post{title: "post_title_1"}] = Actions.all(Post, %{title: "post_title_1"})
    end
  end

  describe "create/2" do
    test "successfully creates a new record with the given parameters" do
      assert {:ok, %Post{title: "post_title"}} = Actions.create(Post, %{title: "post_title"})
    end

    test "returns changeset errors when unique constraint is violated" do
      assert {:ok, %Post{unique_identifier: "post_unique_identifier"}} =
               Actions.create(Post, %{unique_identifier: "post_unique_identifier"})

      assert {:error, changeset} =
               Actions.create(Post, %{unique_identifier: "post_unique_identifier"})

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)
    end
  end

  describe "find/2" do
    test "successfully retrieves a record matching the search parameters" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, %Post{title: "post_title"}} = Actions.find(Post, %{title: "post_title"})
    end

    test "returns detailed not_found error when no record matches parameters" do
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

  describe "update/3" do
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
