defmodule EctoShorts.ActionsTest do
  @moduledoc false
  use EctoShorts.DataCase

  alias EctoShorts.Actions

  alias EctoShorts.Repo

  alias EctoShorts.Schemas.{
    Post,
    PostNoPrimaryKeySchema
  }

  def insert!(repo, schema_module, params) do
    schema_module
    |> struct!()
    |> schema_module.changeset(params)
    |> repo.insert!()
  end

  describe "batch/3" do
    test "converts query results to a map with batch keys as map keys and records as values" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert %{%{title: "post_title"} => %Post{title: "post_title"}} =
               Actions.batch(Post, [:title], [%{title: "post_title"}])
    end

    test "raises if schema has no primary key and batch key not provided" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert_raise KeyError, ~r|Batch key required for schema|, fn ->
        Actions.batch(PostNoPrimaryKeySchema, [%{title: "post_title"}])
      end
    end

    test "returns stream when option :stream is true" do
      assert %Stream{} = Actions.batch(Post, :primary_key, [%{title: "post_title"}], stream: true)
    end

    test "returns records with option :stream" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, [%{%{title: "post_title"} => %Post{title: "post_title"}}]} =
               Repo.transaction(fn ->
                 Post
                 |> Actions.batch([:title], [%{title: "post_title"}], stream: true)
                 |> Enum.to_list()
               end)
    end
  end

  describe "find_all/2" do
    test "retrieves records matching the given params including primary key" do
      post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      post_1_id = post_1.id

      assert {:ok, [%Post{id: ^post_1_id, title: "post_title_1"}]} =
               Actions.find_all(Post, [%{id: post_1_id, title: "post_title_1"}])
    end
  end

  describe "find_all/3" do
    test "retrieves records using specified batch keys" do
      _post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {:ok, [%Post{title: "post_title_1"}]} =
               Actions.find_all(Post, [:title], [%{title: "post_title_1"}])
    end

    test "returns error when no matching records exist" do
      assert {:error,
              [
                %ErrorMessage{
                  code: :not_found,
                  message: "Record not found.",
                  details: %{
                    failed_value: %{title: "does_not_exist"},
                    key: [:title],
                    params: [%{title: "does_not_exist"}],
                    position: 0,
                    query: EctoShorts.Schemas.Post
                  }
                }
              ]} = Actions.find_all(Post, [:title], [%{title: "does_not_exist"}])
    end
  end

  describe "insert_all/2" do
    test "creates new records and returns them with returning: true option" do
      assert {:ok, {1, [%Post{title: "post_title"}]}} =
               Actions.insert_all(Post, [%{title: "post_title"}], returning: true)
    end

    test "updates existing record when primary key is provided" do
      post = insert!(Repo, Post, %{title: "post_title"})

      post_id = post.id

      assert {:ok, {1, [%Post{id: ^post_id, title: "post_title"}]}} =
               Actions.insert_all(
                 Post,
                 [%{id: post_id, title: "post_title"}],
                 returning: true
               )
    end

    test "updates existing record using struct and params tuple" do
      post = insert!(Repo, Post, %{title: "post_title"})

      post_id = post.id

      assert {:ok, {1, [%Post{id: ^post_id, title: "post_title"}]}} =
               Actions.insert_all(Post, [{post, %{title: "post_title"}}], returning: true)
    end

    test "updates existing record using changeset and params tuple" do
      post = insert!(Repo, Post, %{title: "post_title"})

      post_id = post.id

      post_changeset = Post.changeset(post)

      assert {:ok, {1, [%Post{id: ^post_id, title: "post_title"}]}} =
               Actions.insert_all(Post, [{post_changeset, %{title: "post_title"}}],
                 returning: true
               )
    end
  end

  describe "update_all/2" do
    test "updates records matching the filter params" do
      _post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {1, nil} =
               Actions.update_all(
                 Post,
                 %{title: "post_title_1"},
                 %{title: "updated_post_title_1"}
               )
    end

    test "updates and returns matching records when select: true is specified" do
      _post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {1, [%Post{title: "updated_post_title_1"}]} =
               Actions.update_all(
                 Post,
                 %{title: "post_title_1", select: true},
                 %{title: "updated_post_title_1"}
               )
    end
  end

  describe "delete_all/2" do
    test "deletes records matching the filter params" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {1, nil} = Actions.delete_all(Post, %{})
    end

    test "deletes and returns matching records when select: true is specified" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {1, [%Post{title: "post_title"}]} = Actions.delete_all(Post, %{select: true})
    end
  end

  describe "find_or_create_many/3" do
    test "creates multiple records when none exist" do
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

    test "returns existing records when matches are found" do
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

    test "returns error on unique constraint violation" do
      assert {:error,
              %ErrorMessage{
                code: :conflict,
                message: "Failed to create record.",
                details: %{
                  query: EctoShorts.Schemas.Post,
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
    test "updates multiple records that match search criteria" do
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
    test "creates multiple records when no matches exist" do
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

    test "updates multiple existing records when matches are found" do
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
    test "creates multiple records in a single operation" do
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
    test "retrieves all records matching params" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, [%Post{title: "post_title"}]} =
               Actions.find_many(Post, [%{title: "post_title"}])
    end

    test "returns error when no matches exist" do
      assert {
               :error,
               %ErrorMessage{
                 code: :not_found,
                 message: "Record not found.",
                 details: %{
                   query: EctoShorts.Schemas.Post,
                   params: [%{title: "does_not_exist"}],
                   failing_value: %{title: "does_not_exist"},
                   position: 0,
                   changes_so_far: []
                 }
               }
             } = Actions.find_many(Post, [%{title: "does_not_exist"}])
    end
  end

  describe "delete_many/3" do
    test "deletes multiple records using structs" do
      post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert {:ok, [%Post{title: "post_title_1"}, %Post{title: "post_title_2"}]} =
               Actions.delete_many([post_1, post_2])
    end

    test "deletes multiple records using changesets" do
      post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      post_1_changeset = Post.changeset(post_1)

      post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      post_2_changeset = Post.changeset(post_2)

      assert {:ok, [%Post{title: "post_title_1"}, %Post{title: "post_title_2"}]} =
               Actions.delete_many([post_1_changeset, post_2_changeset])
    end
  end

  describe "find_and_create/3" do
    test "returns existing record when match is found" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "existing_post_title"}} =
               Actions.find_and_create(Post, %{title: "existing_post_title"}, %{
                 title: "created_post_title"
               })
    end

    test "creates new record when no match exists" do
      assert {:ok, %Post{title: "created_post_title"}} =
               Actions.find_and_create(Post, %{title: "existing_post_title"}, %{
                 title: "created_post_title"
               })
    end
  end

  describe "find_and_update/2" do
    test "updates record when match is found" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "updated_post_title"}} =
               Actions.find_and_update(
                 Post,
                 %{title: "existing_post_title"},
                 %{title: "updated_post_title"}
               )
    end

    test "returns error when no match exists" do
      assert {:error, %{code: :not_found}} =
               Actions.find_and_update(Post, %{title: "does_not_exist"}, %{})
    end
  end

  describe "find_and_upsert/2" do
    test "creates new record when no match exists" do
      assert {:ok, %Post{title: "existing_post_title"}} =
               Actions.find_and_upsert(
                 Post,
                 %{title: "existing_post_title"},
                 %{title: "existing_post_title"}
               )
    end

    test "updates existing record when match is found" do
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
    test "deletes record matching params" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, %Post{title: "post_title"}} =
               Actions.find_and_delete(Post, %{title: "post_title"})
    end

    test "returns error when no matching record exists" do
      assert {:error, %{code: :not_found}} =
               Actions.find_and_delete(Post, %{title: "does_not_exist"})
    end
  end

  describe "find_or_create/3" do
    test "returns existing record when match is found" do
      _post = insert!(Repo, Post, %{title: "existing_post_title"})

      assert {:ok, %Post{title: "post_title"}} =
               Actions.find_or_create(Post, %{title: "post_title"})
    end

    test "creates a new record with given params when no match is found" do
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
    test "returns records matching params" do
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
    test "returns only records that match the given filter params" do
      _post_1 = insert!(Repo, Post, %{title: "post_title_1"})

      _post_2 = insert!(Repo, Post, %{title: "post_title_2"})

      assert [%Post{title: "post_title_1"}] = Actions.all(Post, %{title: "post_title_1"})
    end
  end

  describe "create/2" do
    test "creates a new record with the given params" do
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
    test "retrieves a record matching the params" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, %Post{title: "post_title"}} = Actions.find(Post, %{title: "post_title"})
    end

    test "returns detailed not_found error when no record matches params" do
      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "Record not found.",
                details: %{
                  query: EctoShorts.Schemas.Post,
                  params: %{title: "post_title"}
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
    test "returns records matching params" do
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
    test "performs count aggregation on matching records" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert 1 = Actions.aggregate(Post, %{}, :count, :id)
    end
  end

  describe "transaction/2" do
    test "executes operations within a transaction" do
      _post = insert!(Repo, Post, %{title: "post_title"})

      assert {:ok, [%Post{title: "post_title"}]} =
               Actions.transaction(fn ->
                 Actions.all(Post, %{})
               end)
    end

    test "handles successful operation responses" do
      assert {:ok, %Post{title: "post_title"}} =
               Actions.transaction(fn ->
                 Actions.create(Post, %{title: "post_title"})
               end)
    end

    test "rolls back on error atom response" do
      assert :error =
               Actions.transaction(fn ->
                 with {:ok, _} <- Actions.create(Post, %{title: "post_title"}) do
                   :error
                 end
               end)

      assert {:error, %{code: :not_found}} = Actions.find(Post, %{title: "post_title"})
    end

    test "rolls back on error tuple response" do
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

    test "rolls back on constraint violations" do
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
