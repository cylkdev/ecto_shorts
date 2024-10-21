defmodule EctoShorts.ActionsTest do
  @moduledoc false
  use EctoShorts.DataCase

  alias Ecto.Adapters.SQL.Sandbox
  alias Ecto.{Changeset, Multi}
  alias EctoShorts.{
    Actions,
    Support.Repo,
    Support.Repo2,
    Support.Schemas.Comment,
    Support.Schemas.Post,
    Support.Schemas.PostNoConstraint
  }

  describe "find_or_create_many/2: " do
    test "arg queryable - fetches many if results matching all params found" do
      assert {:ok, %{id: id, title: "created_title"}} = Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{0 => %{id: ^id, title: "created_title"}}} =
        Actions.find_or_create_many(Post, [%{title: "created_title"}])
    end

    test "arg {source, queryable} - fetches many if results matching all params found" do
      assert {:ok, %{id: id, title: "created_title"}} = Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{0 => %{id: ^id, title: "created_title"}}} =
        Actions.find_or_create_many({"posts", Post}, [%{title: "created_title"}])
    end

    test "arg queryable - creates many if results matching all params not found" do
      assert {:ok, %{id: id, title: "created_title"} = schema_data} = Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{id: ^id}} = Actions.delete(schema_data)

      assert {:ok, %{0 => %{id: returned_id, title: "created_title"}}} = Actions.find_or_create_many(Post, [%{title: "created_title"}])

      assert returned_id !== id
    end

    test "arg {source, queryable} - creates many if results matching all params not found" do
      assert {:ok, %{id: id, title: "created_title"} = schema_data} = Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{id: ^id}} = Actions.delete(schema_data)

      assert {:ok, %{0 => %{id: returned_id, title: "created_title"}}} = Actions.find_or_create_many({"posts", Post}, [%{title: "created_title"}])

      assert returned_id !== id
    end

    test "arg queryable - return ecto multi error on create error" do
      assert {:error,
        1,
        %Ecto.Changeset{} = changeset,
        %{0 => %Post{unique_identifier: "unique_identifier_a", title: "title_a"} = post} = changes
      } =
        Actions.find_or_create_many(
          Post,
          [
            %{unique_identifier: "unique_identifier_a", title: "title_a"},
            %{unique_identifier: "unique_identifier_a", title: "title_b"},
            %{unique_identifier: "unique_identifier_b"}
          ]
        )

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert %{0 => post} === changes
    end

    test "arg {source, queryable} - return ecto multi error on create error" do
      assert {:error,
        1,
        %Ecto.Changeset{} = changeset,
        %{0 => %Post{unique_identifier: "unique_identifier_a", title: "title_a"} = post} = changes
      } =
        Actions.find_or_create_many(
          {"posts", Post},
          [
            %{unique_identifier: "unique_identifier_a", title: "title_a"},
            %{unique_identifier: "unique_identifier_a", title: "title_b"},
            %{unique_identifier: "unique_identifier_b"}
          ]
        )

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert %{0 => post} === changes
    end
  end

  describe "find_and_update_many/2: " do
    test "arg queryable - fetch result matching params and update" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{0 => %Post{id: ^id, title: "updated_post_title"} }} =
        Actions.find_and_update_many(
          Post,
          [{%{id: id}, %{title: "updated_post_title"}}]
        )
    end

    test "arg {source, queryable} - fetch result matching params and update" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{0 => %Post{id: ^id, title: "updated_post_title"} }} =
        Actions.find_and_update_many(
          {"posts", Post},
          [{%{id: id}, %{title: "updated_post_title"}}]
        )
    end

    test "arg queryable - return ecto multi error when not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, 0, error, changes} =
        Actions.find_and_update_many(
          Post,
          [{%{id: id}, %{title: "updated_post_title"}}]
        )

      assert  %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{id: ^id},
          query: EctoShorts.Support.Schemas.Post
        },
        message: "no records found"
      } = error

      assert %{} === changes
    end

    test "arg {source, queryable} - return ecto multi error when not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, 0, error, changes} =
        Actions.find_and_update_many(
          {"posts", Post},
          [{%{id: id}, %{title: "updated_post_title"}}]
        )

      assert  %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{id: ^id},
          query: {"posts", Post}
        },
        message: "no records found"
      } = error

      assert %{} === changes
    end
  end

  describe "find_and_upsert_many/2: " do
    test "arg queryable - fetches many results and create if not found or update if found" do
      assert {:ok, %{id: id, unique_identifier: "existing_identifier"}} =
        Actions.create(Post, %{unique_identifier: "existing_identifier"})

      assert {:ok, %{
        0 => %{id: ^id, unique_identifier: "updated_identifier"},
        1 => %{unique_identifier: "new_identifier"}
      }} =
        Actions.find_and_upsert_many(
          Post,
          [
            {%{unique_identifier: "existing_identifier"}, %{unique_identifier: "updated_identifier"}},
            {%{unique_identifier: "non_existent_identifier"}, %{unique_identifier: "new_identifier"}}
          ]
        )
    end

    test "arg {source, queryable} - fetches many results and create if not found or update if found" do
      assert {:ok, %{id: id, unique_identifier: "existing_identifier"}} =
        Actions.create(Post, %{unique_identifier: "existing_identifier"})

      assert {:ok, %{
        0 => %{id: ^id, unique_identifier: "updated_identifier"},
        1 => %{unique_identifier: "new_identifier"}
      }} =
        Actions.find_and_upsert_many(
          {"posts", Post},
          [
            {%{unique_identifier: "existing_identifier"}, %{unique_identifier: "updated_identifier"}},
            {%{unique_identifier: "non_existent_identifier"}, %{unique_identifier: "new_identifier"}}
          ]
        )
    end
  end

  describe "find_or_create/2: " do
    test "arg queryable - fetch result matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.find_or_create(Post, %{id: id})
    end

    test "arg {source, queryable} - fetch result matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.find_or_create({"posts", Post}, %{id: id})
    end

    test "arg queryable - creates result matching params if not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{id: returned_id, title: "created_post_title"}} =
        Actions.find_or_create(Post, %{
          id: id,
          title: "created_post_title"
        })

      assert id !== returned_id
    end

    test "arg {source, queryable} - creates result matching params if not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{id: returned_id, title: "created_post_title"}} =
        Actions.find_or_create({"posts", Post}, %{
          id: id,
          title: "created_post_title"
        })

      assert id !== returned_id
    end
  end

  describe "find_and_update/2: " do
    test "arg queryable - fetch result matching params and update" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id, title: "updated_post_title"}} =
        Actions.find_and_update(
          Post,
          %{id: id},
          %{title: "updated_post_title"}
        )
    end

    test "arg {source, queryable} - fetch result matching params and update" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id, title: "updated_post_title"}} =
        Actions.find_and_update(
          {"posts", Post},
          %{id: id},
          %{title: "updated_post_title"}
        )
    end

    test "arg queryable - return error if not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %{code: :not_found}} =
        Actions.find_and_update(
          Post,
          %{id: id},
          %{title: "updated_post_title"}
        )
    end

    test "arg {source, queryable} - return error if not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %{code: :not_found}} =
        Actions.find_and_update(
          {"posts", Post},
          %{id: id},
          %{title: "updated_post_title"}
        )
    end
  end

  describe "find_and_upsert/3: " do
    test "arg queryable - fetch result matching params and update" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{title: "updated_post_title"}} =
        Actions.find_and_upsert(
          Post,
          %{id: id},
          %{title: "updated_post_title"}
        )
    end

    test "arg {source, queryable} - fetch result matching params and update" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{title: "updated_post_title"}} =
        Actions.find_and_upsert(
          {"posts", Post},
          %{id: id},
          %{title: "updated_post_title"}
        )
    end

    test "arg queryable - insert result matching params if not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{title: "created_post_title"}} =
        Actions.find_and_upsert(
          Post,
          %{id: id},
          %{title: "created_post_title"}
        )
    end

    test "arg {source, queryable} - insert result matching params if not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{title: "created_post_title"}} =
        Actions.find_and_upsert(
          {"posts", Post},
          %{id: id},
          %{title: "created_post_title"}
        )
    end
  end

  describe "get/2: " do
    test "arg queryable - return nil when record does not exist" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert nil === Actions.get(Post, id)
    end

    test "arg queryable - return result matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert %Post{id: ^id} = Actions.get(Post, id)
    end

    test "arg {source, queryable} - return result matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert %Post{id: ^id} = Actions.get({"posts", Post}, id)
    end

    test "arg {source, queryable} - return nil when record does not exist" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert nil === Actions.get({"posts", Post}, id)
    end
  end

  describe "all/1: " do
    test "arg queryable - return all results" do
      assert {:ok, %{id: post_1_id}} = Actions.create(Post)

      assert {:ok, %{id: post_2_id}} = Actions.create(Post)

      assert [%{id: ^post_1_id}, %{id: ^post_2_id}] = Actions.all(Post)
    end

    test "arg {source, queryable} - return all results" do
      assert {:ok, %{id: post_1_id}} = Actions.create(Post)

      assert {:ok, %{id: post_2_id}} = Actions.create(Post)

      assert [%{id: ^post_1_id}, %{id: ^post_2_id}] = Actions.all({"posts", Post})
    end

    test "arg query - return results" do
      assert {:ok, %{id: post_1_id}} = Actions.create(Post)

      assert {:ok, %{id: post_2_id}} = Actions.create(Post)

      query = from p in Post, where: p.id in [^post_1_id, ^post_2_id]

      assert [%{id: ^post_1_id}, %{id: ^post_2_id}] = Actions.all(query)
    end
  end

  describe "all/2: " do
    test "arg queryable - return results matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert [%{id: ^id}] = Actions.all(Post, %{id: id})
    end

    test "arg queryable - return results matching keyword params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert [%{id: ^id}] = Actions.all(Post, [id: id])
    end

    test "arg {source, queryable} - return results matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert [%{id: ^id}] = Actions.all({"posts", Post}, %{id: id})
    end

    test "arg queryable - return results in order when :group_by and :order_by set in params" do
      assert {:ok, %{id: post_1_id}} = Actions.create(Post, %{likes: 1})

      assert {:ok, %{id: post_2_id}} = Actions.create(Post, %{likes: 2})

      assert [%{id: ^post_2_id}, %{id: ^post_1_id}] = Actions.all(Post, %{group_by: :id, order_by: [{:desc, :likes}]})
    end
  end

  describe "all/3: " do
    test "arg queryable - return results in order when :group_by and :order_by set in options" do
      assert {:ok, post_1} = Actions.create(Post, %{likes: 1})

      assert {:ok, post_2} = Actions.create(Post, %{likes: 2})

      assert [^post_2, ^post_1] = Actions.all(Post, %{}, group_by: :id, order_by: [{:desc, :likes}])
    end
  end

  describe "create: " do
    test "arg queryable - create record matching params" do
      assert {:ok, %Post{title: "post_title"}} = Actions.create(Post, %{title: "post_title"})
    end

    test "arg {source, queryable} - create record matching params" do
      assert {:ok, %Post{title: "post_title"}} = Actions.create({"posts", Post}, %{title: "post_title"})
    end

    test "arg queryable - return changeset error when params are invalid" do
      assert {:error, changeset} = Actions.create(Post, %{title: "1"})

      assert {:title, ["should be at least 3 character(s)"]} in errors_on(changeset)
    end

    test "arg {source, queryable} - return changeset error when params are invalid" do
      assert {:error, changeset} = Actions.create({"posts", Post}, %{title: "1"})

      assert {:title, ["should be at least 3 character(s)"]} in errors_on(changeset)
    end
  end

  describe "find/2: " do
    test "arg queryable - fetches a single result matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.find(Post, %{id: id})
    end

    test "arg {source, queryable} - fetches a single result matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.find({"posts", Post}, %{id: id})
    end

    test "arg query - fetches a single result" do
      assert {:ok, %{id: id}} =
        Actions.create(Post, %{title: "post_title"})

      query = from p in Post, where: p.id == ^id

      assert {:ok, %{id: ^id, title: "post_title"}} = Actions.find(query, %{})
    end

    test "arg queryable - return not found error when params empty" do
      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{},
          query: Post
        },
        message: "no records found"
      }} = Actions.find(Post, %{})
    end

    test "arg {source, queryable} - return not found error when params empty" do
      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{},
          query: {"posts", Post}
        },
        message: "no records found"
      }} = Actions.find({"posts", Post}, %{})
    end

    test "arg queryable - return error if not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{id: ^id},
          query: Post
        },
        message: "no records found"
      }} = Actions.find(Post, %{id: id})
    end

    test "arg {source, queryable} - return error if not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{id: ^id},
          query: {"posts", Post}
        },
        message: "no records found"
      }} = Actions.find({"posts", Post}, %{id: id})
    end
  end

  describe "update/3: " do
    test "arg queryable - update a single result matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id, title: "updated_post_title"}} =
        Actions.update(Post, id, %{title: "updated_post_title"})
    end

    test "arg queryable - update a single result matching schema data" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, %{id: ^id, title: "updated_post_title"}} =
        Actions.update(Post, schema_data, %{title: "updated_post_title"})
    end

    test "arg queryable - update a single result matching id and keyword params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok,%{id: ^id, title: "updated_post_title"}} =
        Actions.update(Post, id, [title: "updated_post_title"])
    end

    test "arg {source, queryable} - update a single result matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id, title: "updated_post_title"}} =
        Actions.update({"posts", Post}, id, %{title: "updated_post_title"})
    end

    test "arg {source, queryable} - update a single result matching schema data" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, %{id: ^id, title: "updated_post_title"}} =
        Actions.update({"posts", Post}, schema_data, %{title: "updated_post_title"})
    end

    test "arg {source, queryable} - update a single result matching id and keyword params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok,%{id: ^id, title: "updated_post_title"}} =
        Actions.update({"posts", Post}, id, [title: "updated_post_title"])
    end

    test "arg queryable - return error when result matching id not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          query: Post,
          find_params: %{id: ^id},
          update_params: %{title: "updated_post_title"}
        },
        message: "No item found with id:" <> _
      }} = Actions.update(Post, id, %{title: "updated_post_title"})
    end

    test "arg {source, queryable} - return error when result matching id not found" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          query: {"posts", Post},
          find_params: %{id: ^id},
          update_params: %{title: "updated_post_title"}
        },
        message: "No item found with id:" <> _
      }} = Actions.update({"posts", Post}, id, %{title: "updated_post_title"})
    end
  end

  describe "delete/1: " do
    test "delete a single result matching changeset" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      changeset = Post.changeset(schema_data, %{})

      assert {:ok, %{id: ^id}} = Actions.delete(changeset)
    end



    test "arg schema - delete a single result matching schema" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete(schema_data)
    end

    test "arg list of changeset - delete many changesets" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      changeset = Post.changeset(schema_data, %{})

      assert {:ok, [%{id: ^id}]} = Actions.delete([changeset])
    end

    test "arg list of schema - delete many schemas" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete([schema_data])
    end

    test "arg schema - return changeset with constraint error" do
      assert {:ok, post} = Actions.create(Post, %{title: "title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert {:error, %{
        code: :internal_server_error,
        message: "failed to delete record",
        details: %{
          changeset: changeset,
          schema_data: ^post,
          query: Post
        }
      }} = Actions.delete(post)

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end

    test "arg changeset - return changeset with constraint error"  do
      assert {:ok, post} = Actions.create(Post, %{title: "title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert {:error, %{
        code: :internal_server_error,
        message: "failed to delete record",
        details: %{
          changeset: changeset,
          schema_data: ^post,
          query: Post
        }
      }} =
        post
        |> Post.changeset(%{})
        |> Actions.delete()

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end
  end

  describe "delete/2: " do
    test "arg queryable - find and delete a single result matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete(Post, id)
    end

    test "arg {source, queryable} - delete a single result matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete({"posts", Post}, id)
    end

    test "arg query - find and delete a single result matching query and id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      query = from p in Post

      assert {:ok, %{id: ^id}} = Actions.delete(query, id)
    end

    test "arg queryable - find and delete many results matching list of id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete(Post, [id])
    end

    test "arg {source, queryable} - find and delete many results matching list of id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete({"posts", Post}, [id])
    end

    test "arg query - find and delete many results matching list of id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      query = from p in Post

      assert {:ok, [%{id: ^id}]} = Actions.delete(query, [id])
    end

    test "arg queryable - find and delete a single result matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete(Post, %{id: id})
    end

    test "arg {source, queryable} - find and delete a single result matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete({"posts", Post}, %{id: id})
    end

    test "arg query - find and delete a single result matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      query = from p in Post

      assert {:ok, %{id: ^id}} = Actions.delete(query, %{id: id})
    end

    test "arg queryable - find and delete a single result matching list of params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete(Post, [%{id: id}])
    end

    test "arg {source, queryable} - find and delete a single result matching list of params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete({"posts", Post}, [%{id: id}])
    end

    test "arg query - find and delete a single result matching list of params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      query = from p in Post

      assert {:ok, [%{id: ^id}]} = Actions.delete(query, [%{id: id}])
    end

    test "arg changeset - delete changeset" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      changeset = Post.changeset(schema_data, %{})

      assert {:ok, %{id: ^id}} = Actions.delete(changeset, [])
    end

    test "arg changeset - delete list of changeset" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      changeset = Post.changeset(schema_data, %{})

      assert {:ok, [%{id: ^id}]} = Actions.delete([changeset], [])
    end

    test "arg schema - delete schema data" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete(schema_data, [])
    end

    test "arg schema - delete list of schema data" do
      assert {:ok, %{id: id} = schema_data} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete([schema_data], [])
    end

    test "arg schema - return list of errors" do
      assert {:ok, post_1} = Actions.create(Post)

      assert {:ok, %{id: post_2_id} = post_2} = Actions.create(Post)
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_2_id})

      assert {:error, [
        %ErrorMessage{
          code: :internal_server_error,
          details: %{
            changeset: %Ecto.Changeset{data: %Post{id: ^post_2_id}} = changeset,
            query: EctoShorts.Support.Schemas.Post
          },
          message: "failed to delete record"
        }
      ]} = Actions.delete([post_1, post_2], [])

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end

    test "arg queryable - return constraint error" do
      assert {:ok, post} = Actions.create(Post, %{title: "title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert {:error, %{
        code: :internal_server_error,
        message: "failed to delete record",
        details: %{
          changeset: changeset,
          schema_data: ^post,
          query: Post
        }
      }} = Actions.delete(Post, post.id)

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end

    test "arg {source, queryable} - return constraint error" do
      assert {:ok, post} = Actions.create(Post, %{title: "title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert {:error, %{
        code: :internal_server_error,
        message: "failed to delete record",
        details: %{
          changeset: changeset,
          schema_data: ^post,
          query: Post
        }
      }} = Actions.delete({"posts", Post}, post.id)

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end
  end

  describe "delete/3: " do
    test "arg queryable - fetch and delete a single result matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete(Post, id, [])
    end

    test "arg {source, queryable} - fetch and delete a single result matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete({"posts", Post}, id, [])
    end

    test "arg query - fetch and delete a single result matching query and id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      query = from p in Post

      assert {:ok, %{id: ^id}} = Actions.delete(query, id, [])
    end

    test "arg queryable - fetch and delete many results matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete(Post, [id], [])
    end

    test "arg {source, queryable} - fetch and delete many results matching id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete({"posts", Post}, [id], [])
    end

    test "arg query - fetch and delete many results matching query and filter id" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      query = from p in Post

      assert {:ok, [%{id: ^id}]} = Actions.delete(query, [id], [])
    end

    test "arg queryable - fetch and delete a single result matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete(Post, %{id: id}, [])
    end

    test "arg {source, queryable} - fetch and delete a single result matching params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, %{id: ^id}} = Actions.delete({"posts", Post}, %{id: id}, [])
    end

    test "arg query - fetch and delete a single result matching query and params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      query = from p in Post

      assert {:ok, %{id: ^id}} = Actions.delete(query, %{id: id}, [])
    end

    test "arg queryable - fetch and delete many results matching list of params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete(Post, [%{id: id}], [])
    end

    test "arg {source, queryable} - fetch and delete many results matching list of params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      assert {:ok, [%{id: ^id}]} = Actions.delete({"posts", Post}, [%{id: id}], [])
    end

    test "arg query - fetch and delete many results matching query and list of params" do
      assert {:ok, %{id: id}} = Actions.create(Post)

      query = from p in Post

      assert {:ok, [%{id: ^id}]} = Actions.delete(query, [%{id: id}], [])
    end
  end

  describe "stream/1: " do
    test "arg queryable - return enumerable" do
      assert {:ok, %{id: post_id}} = Actions.create(Post)

      assert {:ok, [%Post{id: ^post_id}]} =
        Repo.transaction(fn ->
          Post
          |> Actions.stream()
          |> Enum.to_list()
        end)
    end

    test "arg {source, queryable} - return enumerable" do
      assert {:ok, %{id: post_id}} = Actions.create(Post)

      assert {:ok, [%Post{id: ^post_id}]} =
        Repo.transaction(fn ->
          {"posts", Post}
          |> Actions.stream()
          |> Enum.to_list()
        end)
    end

    test "arg query - return enumerable" do
      assert {:ok, %{id: post_id}} = Actions.create(Post)

      query = from p in Post

      assert {:ok, [%Post{id: ^post_id}]} =
        Repo.transaction(fn ->
          query
          |> Actions.stream()
          |> Enum.to_list()
        end)
    end
  end

  describe "aggregate/4: " do
    test "count" do
      assert {:ok, _} = Actions.create(Post)

      assert 1 = Actions.aggregate(Post, %{}, :count, :id)
    end

    test "sum" do
      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 2})

      assert 3 = Actions.aggregate(Post, %{}, :sum, :likes)
    end

    test "avg" do
      assert {:ok, _} = Actions.create(Post, %{likes: 2})
      assert {:ok, _} = Actions.create(Post, %{likes: 2})

      expected_decimal = Decimal.new("2.0000000000000000")

      assert ^expected_decimal = Actions.aggregate(Post, %{}, :avg, :likes)
    end

    test "min" do
      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 20})

      assert 1 = Actions.aggregate(Post, %{}, :min, :likes)
    end

    test "max" do
      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 20})

      assert 20 = Actions.aggregate(Post, %{}, :max, :likes)
    end
  end

  describe "transaction/2" do
    test "return multi response" do
      assert {:ok, %{example: "success"}} =
        Multi.new()
        |> Multi.run(:example, fn _repo, _changes -> {:ok, "success"} end)
        |> Actions.transaction()
    end

    test "return multi error" do
      assert {:error, :example, "failed", %{}} =
        Multi.new()
        |> Multi.run(:example, fn _repo, _changes -> {:error, "failed"} end)
        |> Actions.transaction()
    end

    test "arg 0-arity function - return ok" do
      assert {:ok, {:ok, %{id: post_id}}} =
        Actions.transaction(fn ->
          Actions.create(Post)
        end)

      [%Post{id: ^post_id} | _] = Actions.all(Post)
    end

    test "arg 1-arity function - return ok" do
      assert {:ok, {:ok, %{id: post_id}}} =
        Actions.transaction(fn _repo ->
          Actions.create(Post)
        end)

      [%Post{id: ^post_id} | _] = Actions.all(Post)
    end

    test "rollback and return {:error, term()} from function when option :rollback_on_error is true" do
      assert {:error, {:error, "failed"}} =
        Actions.transaction(fn ->
          with {:ok, _} <- Actions.create(Post) do
            {:error, "failed"}
          end
        end)

      posts = Actions.all(Post)

      assert 0 === length(posts)
    end

    test "rollback and return :error from function when option :rollback_on_error is true" do
      assert {:error, :error} =
        Actions.transaction(fn ->
          with {:ok, _} <- Actions.create(Post) do
            :error
          end
        end)

      posts = Actions.all(Post)

      assert 0 === length(posts)
    end

    test "commit changes when {:error, term()} returned from function and option :rollback_on_error is false" do
      assert {:ok, {:error, "failed"}} =
        Actions.transaction(
          fn ->
            with {:ok, _post} <- Actions.create(Post) do
              {:error, "failed"}
            end
          end,
          rollback_on_error: false
        )

      posts = Actions.all(Post)

      assert 1 === length(posts)
    end

    test "commit changes when :error returned from function and option :rollback_on_error is false" do
      assert {:ok, :error} =
        Actions.transaction(
          fn ->
            with {:ok, _post} <- Actions.create(Post) do
              :error
            end
          end,
          rollback_on_error: false
        )

        posts = Actions.all(Post)

        assert 1 === length(posts)
    end
  end

  test "raise when :repo not set in option and configuration" do
    assert_raise ArgumentError, ~r|EctoShorts repo not configured!|, fn ->
      Actions.create(Post, %{}, repo: nil)
    end
  end

  test "raise when :repo and :replica not set in option and configuration" do
    assert_raise ArgumentError, ~r|EctoShorts replica and repo not configured!|, fn ->
      Actions.all(Post, %{}, repo: nil, replica: nil)
    end
  end

  test "can set repo option" do
    {:ok, _} = Repo2.start_test_repo()

    :ok = Sandbox.checkout(Repo2)

    :ok = Sandbox.mode(Repo2, {:shared, self()})

    assert Repo2 = Repo2.get_dynamic_repo()

    assert {:ok, %{id: id}} = Actions.create(Post, %{}, repo: Repo2)

    assert [%{id: ^id}] = Actions.all(Post, id: id, repo: Repo2, replica: nil)
  end

  test "can set replica option" do
    {:ok, _} = Repo2.start_test_repo()

    :ok = Sandbox.checkout(Repo2)

    :ok = Sandbox.mode(Repo2, {:shared, self()})

    assert Repo2 = Repo2.get_dynamic_repo()

    assert {:ok, %{id: id}} = Actions.create(Post, %{}, repo: Repo2)

    assert [%{id: ^id}] = Actions.all(Post, id: id, repo: nil, replica: Repo2)
  end

  test "option: changeset - arg 1-arity function - applies changes" do
    assert {:ok, %{id: post_id} = post} = Actions.create(PostNoConstraint, %{title: "title"})

    assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

    assert {:error, %{
      code: :internal_server_error,
      message: "failed to delete record",
      details: %{
        changeset: changeset,
        schema_data: %PostNoConstraint{id: ^post_id},
        query: PostNoConstraint
      }
    }} =
      Actions.delete(post, changeset: fn changeset ->
        Changeset.no_assoc_constraint(changeset, :comments, name: "comments_post_id_fkey")
      end)

    assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
  end

  test "option: changeset - arg 2-arity function - applies changes" do
    assert {:ok, %{id: post_id} = post} = Actions.create(PostNoConstraint, %{title: "title"})

    assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

    assert {:error, %{
      code: :internal_server_error,
      message: "failed to delete record",
      details: %{
        changeset: changeset,
        schema_data: %PostNoConstraint{id: ^post_id},
        query: PostNoConstraint
      }
    }} =
      Actions.delete(post, changeset: fn changeset, _params ->
        Changeset.no_assoc_constraint(changeset, :comments, name: "comments_post_id_fkey")
      end)

    assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
  end

  test "option: changeset - arg {mod, fun, args} - applies changes" do
    defmodule MockConstraintTestHandler do
      def changeset(changeset) do
        Changeset.no_assoc_constraint(changeset, :comments, name: "comments_post_id_fkey")
      end
    end

    assert {:ok, %{id: post_id} = post} = Actions.create(PostNoConstraint, %{title: "title"})

    assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

    assert {:error, %{
      code: :internal_server_error,
      message: "failed to delete record",
      details: %{
        changeset: changeset,
        schema_data: %PostNoConstraint{id: ^post_id},
        query: PostNoConstraint
      }
    }} =
      Actions.delete(post, changeset: {MockConstraintTestHandler, :changeset, []})

    assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
  end
end
