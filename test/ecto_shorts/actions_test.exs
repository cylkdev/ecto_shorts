defmodule EctoShorts.ActionsTest do
  @moduledoc false
  use EctoShorts.DataCase

  alias Ecto.{Changeset, Multi}
  alias EctoShorts.{
    Actions,
    Support.Repo,
    Support.Repo2,
    Support.Schemas.Comment,
    Support.Schemas.Post,
    Support.Schemas.PostNoConstraint
  }

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

    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo2)

    :ok = Ecto.Adapters.SQL.Sandbox.mode(Repo2, {:shared, self()})

    assert Repo2 = Repo2.get_dynamic_repo()

    assert {:ok, %{id: post_id}} = Actions.create(Post, %{}, repo: Repo2)

    assert [%{id: ^post_id}] = Actions.all(Post, post_id: post_id, repo: Repo2, replica: nil)
  end

  test "can set replica option" do
    {:ok, _} = Repo2.start_test_repo()

    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo2)

    :ok = Ecto.Adapters.SQL.Sandbox.mode(Repo2, {:shared, self()})

    assert Repo2 = Repo2.get_dynamic_repo()

    assert {:ok, %{id: post_id}} = Actions.create(Post, %{}, repo: Repo2)

    assert [%{id: ^post_id}] = Actions.all(Post, post_id: post_id, repo: nil, replica: Repo2)
  end

  describe "preload/2: " do
    test "when field is a valid association, returns result with association data" do
      assert {:ok, %{id: post_id} = post} = Actions.create(Post, %{})

      assert {:ok, %{id: comment_id}} = Actions.create(Comment, %{post_id: post_id})

      assert %Post{
        id: ^post_id,
        comments: [
          %Comment{id: ^comment_id}
        ]
      } = Actions.preload(post, :comments)
    end
  end

  describe "preload/3: " do
    test "when field is a valid association, returns result with association data" do
      assert {:ok, %{id: post_id} = post} = Actions.create(Post, %{})

      assert {:ok, %{id: comment_id}} = Actions.create(Comment, %{post_id: post_id})

      assert %Post{
        id: ^post_id,
        comments: [
          %Comment{id: ^comment_id}
        ]
      } = Actions.preload(post, :comments, [])
    end
  end

  describe "option changeset: " do
    test "when given a changeset and value of option :changeset is a 1-arity function, add changeset validations" do
      assert {:ok, %{id: post_id} = post} = Actions.create(PostNoConstraint, %{title: "post_title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert_raise Ecto.ConstraintError, ~r|constraint error when attempting to delete struct|, fn ->
        Actions.delete(post)
      end

      assert {:error, %{
        code: :internal_server_error,
        message: "failed to delete record",
        details: %{
          changeset: changeset,
          schema_data: %PostNoConstraint{id: ^post_id},
          query: PostNoConstraint
        }
      }} =
        post
        |> PostNoConstraint.changeset(%{})
        |> Actions.delete(changeset: fn changeset ->
          Changeset.no_assoc_constraint(changeset, :comments, name: "comments_post_id_fkey")
        end)

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end

    test "when arg is schema_data and value of option :changeset is a 1-arity function, add changeset validations" do
      assert {:ok, %{id: post_id} = post} = Actions.create(PostNoConstraint, %{title: "post_title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert_raise Ecto.ConstraintError, ~r|constraint error when attempting to delete struct|, fn ->
        Actions.delete(post)
      end

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

    test "when given a changeset and value of option :changeset is a 2-arity function, add changeset validations" do
      assert {:ok, %{id: post_id} = post} = Actions.create(PostNoConstraint, %{title: "post_title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert_raise Ecto.ConstraintError, ~r|constraint error when attempting to delete struct|, fn ->
        Actions.delete(post)
      end

      assert {:error, %{
        code: :internal_server_error,
        message: "failed to delete record",
        details: %{
          changeset: changeset,
          schema_data: %PostNoConstraint{id: ^post_id},
          query: PostNoConstraint
        }
      }} =
        Actions.delete(post, changeset: fn struct, _params ->
          struct
          |> PostNoConstraint.changeset(%{})
          |> Changeset.no_assoc_constraint(:comments, name: "comments_post_id_fkey")
        end)

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end

    test "{mod, fun, args} - add changeset validations" do
      assert {:ok, %{id: post_id} = post} = Actions.create(PostNoConstraint, %{title: "post_title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert {:error, %{
        code: :internal_server_error,
        message: "failed to delete record",
        details: %{
          changeset: changeset,
          schema_data: %PostNoConstraint{id: ^post_id},
          query: PostNoConstraint
        }
      }} = Actions.delete(post, changeset: {EctoShorts.Support.MockPostNoConstraintCallback, :changeset, []})

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end

    test "{mod, fun} - add changeset validations" do
      assert {:ok, %{id: post_id} = post} = Actions.create(PostNoConstraint, %{title: "post_title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert {:error, %{
        code: :internal_server_error,
        message: "failed to delete record",
        details: %{
          changeset: changeset,
          schema_data: %PostNoConstraint{id: ^post_id},
          query: PostNoConstraint
        }
      }} = Actions.delete(post, changeset: {EctoShorts.Support.MockPostNoConstraintCallback, :changeset})

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end
  end

  describe "find_and_create_many/2 : " do
    test "queryable - fetches results matching params" do
      assert {:ok, %{id: post_id, title: "created_title"}} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{0 => %{id: ^post_id, title: "created_title"}}} =
        Actions.find_and_create_many(Post, [{%{id: post_id}, %{title: "created_title"}}])
    end

    test "{source, queryable} - fetches results matching params" do
      assert {:ok, %{id: post_id, title: "created_title"}} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{0 => %{id: ^post_id, title: "created_title"}}} =
        Actions.find_and_create_many({"posts", Post}, [{%{id: post_id}, %{title: "created_title"}}])
    end

    test "query - fetches results matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id, title: "created_title"}} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{0 => %{id: ^post_id, title: "created_title"}}} =
        Actions.find_and_create_many(query, [{%{id: post_id}, %{title: "created_title"}}])
    end

    test "queryable - create results matching params if not found" do
      assert {:ok, %{id: post_id, title: "created_title"} = schema_data} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{id: ^post_id}} = Actions.delete(schema_data)

      assert {:ok, %{0 => %{id: created_post_id, title: "created_title"}}} =
        Actions.find_and_create_many(Post, [{%{id: 123_456}, %{title: "created_title"}}])

      assert created_post_id !== post_id
    end

    test "{source, queryable} - create results matching params if not found" do
      assert {:ok, %{id: post_id, title: "created_title"} = schema_data} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{id: ^post_id}} = Actions.delete(schema_data)

      assert {:ok, %{0 => %{id: created_post_id, title: "created_title"}}} =
        Actions.find_and_create_many({"posts", Post}, [{%{id: 123_456}, %{title: "created_title"}}])

      assert created_post_id !== post_id
    end

    test "query - create results matching params if not found" do
      query = from p in Post

      assert {:ok, %{id: post_id, title: "created_title"} = schema_data} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{id: ^post_id}} = Actions.delete(schema_data)

      assert {:ok, %{0 => %{id: created_post_id, title: "created_title"}}} =
        Actions.find_and_create_many(query, [{%{id: 123_456}, %{title: "created_title"}}])

      assert created_post_id !== post_id
    end

    test "queryable - return ecto multi error on create error" do
      assert {:error,
        1,
        %Ecto.Changeset{} = changeset,
        %{0 => %{unique_identifier: "unique_identifier_a", title: "post_title_a"} = post} = changes
      } =
        Actions.find_and_create_many(
          Post,
          [
            {%{unique_identifier: "unique_identifier_a"}, %{unique_identifier: "unique_identifier_a", title: "post_title_a"}},
            {%{unique_identifier: "non_existent_identifier"}, %{unique_identifier: "unique_identifier_a", title: "post_title_b"}},
            {%{unique_identifier: "unique_identifier_b"}, %{unique_identifier: "unique_identifier_b"}}
          ]
        )

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert %{0 => post} === changes
    end

    test "{source, queryable} - return ecto multi error on create error" do
      assert {:error,
        1,
        %Ecto.Changeset{} = changeset,
        %{0 => %{unique_identifier: "unique_identifier_a", title: "post_title_a"} = post} = changes
      } =
        Actions.find_and_create_many(
          {"posts", Post},
          [
            {%{unique_identifier: "unique_identifier_a"}, %{unique_identifier: "unique_identifier_a", title: "post_title_a"}},
            {%{unique_identifier: "non_existent_identifier"}, %{unique_identifier: "unique_identifier_a", title: "post_title_b"}},
            {%{unique_identifier: "unique_identifier_b"}, %{unique_identifier: "unique_identifier_b"}}
          ]
        )

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert %{0 => post} === changes
    end

    test "query - return ecto multi error on create error" do
      query = from p in Post

      assert {:error,
        1,
        %Ecto.Changeset{} = changeset,
        %{0 => %{unique_identifier: "unique_identifier_a", title: "post_title_a"} = post} = changes
      } =
        Actions.find_and_create_many(
          query,
          [
            {%{unique_identifier: "unique_identifier_a"}, %{unique_identifier: "unique_identifier_a", title: "post_title_a"}},
            {%{unique_identifier: "non_existent_identifier"}, %{unique_identifier: "unique_identifier_a", title: "post_title_b"}},
            {%{unique_identifier: "unique_identifier_b"}, %{unique_identifier: "unique_identifier_b"}}
          ]
        )

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert %{0 => post} === changes
    end
  end

  describe "find_or_create_many/2 : " do
    test "queryable - fetches results matching params" do
      assert {:ok, %{id: post_id, title: "created_title"}} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{0 => %{id: ^post_id, title: "created_title"}}} =
        Actions.find_or_create_many(Post, [%{title: "created_title"}])
    end

    test "{source, queryable} - fetches results matching params" do
      assert {:ok, %{id: post_id, title: "created_title"}} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{0 => %{id: ^post_id, title: "created_title"}}} =
        Actions.find_or_create_many({"posts", Post}, [%{title: "created_title"}])
    end

    test "query - fetches results matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id, title: "created_title"}} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{0 => %{id: ^post_id, title: "created_title"}}} =
        Actions.find_or_create_many(query, [%{title: "created_title"}])
    end

    test "queryable - create results matching params if not found" do
      assert {:ok, %{id: post_id, title: "created_title"} = schema_data} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{id: ^post_id}} = Actions.delete(schema_data)

      assert {:ok, %{0 => %{id: created_post_id, title: "created_title"}}} =
        Actions.find_or_create_many(Post, [%{title: "created_title"}])

      assert created_post_id !== post_id
    end

    test "{source, queryable} - create results matching params if not found" do
      assert {:ok, %{id: post_id, title: "created_title"} = schema_data} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{id: ^post_id}} = Actions.delete(schema_data)

      assert {:ok, %{0 => %{id: created_post_id, title: "created_title"}}} =
        Actions.find_or_create_many({"posts", Post}, [%{title: "created_title"}])

      assert created_post_id !== post_id
    end

    test "query - create results matching params if not found" do
      query = from p in Post

      assert {:ok, %{id: post_id, title: "created_title"} = schema_data} =
        Actions.create(Post, %{title: "created_title"})

      assert {:ok, %{id: ^post_id}} = Actions.delete(schema_data)

      assert {:ok, %{0 => %{id: created_post_id, title: "created_title"}}} =
        Actions.find_or_create_many(query, [%{title: "created_title"}])

      assert created_post_id !== post_id
    end

    test "queryable - return ecto multi error on create error" do
      assert {:error,
        1,
        %Ecto.Changeset{} = changeset,
        %{0 => %{unique_identifier: "unique_identifier_a", title: "post_title_a"} = post} = changes
      } =
        Actions.find_or_create_many(
          Post,
          [
            %{unique_identifier: "unique_identifier_a", title: "post_title_a"},
            %{unique_identifier: "unique_identifier_a", title: "post_title_b"},
            %{unique_identifier: "unique_identifier_b"}
          ]
        )

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert %{0 => post} === changes
    end

    test "{source, queryable} - return ecto multi error on create error" do
      assert {:error,
        1,
        %Ecto.Changeset{} = changeset,
        %{0 => %{unique_identifier: "unique_identifier_a", title: "post_title_a"} = post} = changes
      } =
        Actions.find_or_create_many(
          {"posts", Post},
          [
            %{unique_identifier: "unique_identifier_a", title: "post_title_a"},
            %{unique_identifier: "unique_identifier_a", title: "post_title_b"},
            %{unique_identifier: "unique_identifier_b"}
          ]
        )

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert %{0 => post} === changes
    end

    test "query - return ecto multi error on create error" do
      query = from p in Post

      assert {:error,
        1,
        %Ecto.Changeset{} = changeset,
        %{0 => %{unique_identifier: "unique_identifier_a", title: "post_title_a"} = post} = changes
      } =
        Actions.find_or_create_many(
          query,
          [
            %{unique_identifier: "unique_identifier_a", title: "post_title_a"},
            %{unique_identifier: "unique_identifier_a", title: "post_title_b"},
            %{unique_identifier: "unique_identifier_b"}
          ]
        )

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert %{0 => post} === changes
    end
  end

  describe "find_and_update_many/2 : " do
    test "queryable - fetches and updates result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{0 => %{id: ^post_id, title: "updated_post_title"} }} =
        Actions.find_and_update_many(Post, [{%{id: post_id}, %{title: "updated_post_title"}}])
    end

    test "{source, queryable} - fetches and updates result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{0 => %{id: ^post_id, title: "updated_post_title"} }} =
        Actions.find_and_update_many({"posts", Post}, [{%{id: post_id}, %{title: "updated_post_title"}}])
    end

    test "query - fetches and updates result matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{0 => %{id: ^post_id, title: "updated_post_title"} }} =
        Actions.find_and_update_many(query, [{%{id: post_id}, %{title: "updated_post_title"}}])
    end

    test "queryable - return ecto multi error when not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, 0, error, changes} =
        Actions.find_and_update_many(Post, [{%{id: post_id}, %{title: "updated_post_title"}}])

      assert  %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{id: ^post_id},
          query: EctoShorts.Support.Schemas.Post
        },
        message: "no records found"
      } = error

      assert %{} === changes
    end

    test "{source, queryable} - return ecto multi error when not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, 0, error, changes} =
        Actions.find_and_update_many({"posts", Post}, [{%{id: post_id}, %{title: "updated_post_title"}}])

      assert  %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{id: ^post_id},
          query: {"posts", Post}
        },
        message: "no records found"
      } = error

      assert %{} === changes
    end

    test "query - return ecto multi error when not found" do
      query = from p in Post

      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, 0, error, changes} =
        Actions.find_and_update_many(query, [{%{id: post_id}, %{title: "updated_post_title"}}])

      assert  %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{id: ^post_id},
          query: ^query
        },
        message: "no records found"
      } = error

      assert %{} === changes
    end
  end

  describe "find_and_upsert_many/2 : " do
    test "queryable - fetches many results and create if not found or update if found" do
      assert {:ok, %{id: post_id, unique_identifier: "existing_identifier"}} =
        Actions.create(Post, %{unique_identifier: "existing_identifier"})

      assert {:ok, %{
        0 => %{id: ^post_id, unique_identifier: "updated_identifier"},
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

    test "{source, queryable} - fetches many results and create if not found or update if found" do
      assert {:ok, %{id: post_id, unique_identifier: "existing_identifier"}} =
        Actions.create(Post, %{unique_identifier: "existing_identifier"})

      assert {:ok, %{
        0 => %{id: ^post_id, unique_identifier: "updated_identifier"},
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

    test "query - fetches many results and create if not found or update if found" do
      query = from p in Post

      assert {:ok, %{id: post_id, unique_identifier: "existing_identifier"}} =
        Actions.create(Post, %{unique_identifier: "existing_identifier"})

      assert {:ok, %{
        0 => %{id: ^post_id, unique_identifier: "updated_identifier"},
        1 => %{unique_identifier: "new_identifier"}
      }} =
        Actions.find_and_upsert_many(
          query,
          [
            {%{unique_identifier: "existing_identifier"}, %{unique_identifier: "updated_identifier"}},
            {%{unique_identifier: "non_existent_identifier"}, %{unique_identifier: "new_identifier"}}
          ]
        )
    end
  end

  describe "find_or_create/2 : " do
    test "queryable - fetches result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.find_or_create(Post, %{id: post_id})
    end

    test "{source, queryable} - fetches result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.find_or_create({"posts", Post}, %{id: post_id})
    end

    test "query - fetches result matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.find_or_create(query, %{id: post_id})
    end

    test "queryable - creates result matching params if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{id: created_post_id, title: "new_post_title"}} =
        Actions.find_or_create(Post, %{id: post_id, title: "new_post_title"})

      assert post_id !== created_post_id
    end

    test "{source, queryable} - creates result matching params if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{id: created_post_id, title: "new_post_title"}} =
        Actions.find_or_create({"posts", Post}, %{id: post_id, title: "new_post_title"})

      assert post_id !== created_post_id
    end

    test "query - creates result matching params if not found" do
      query = from p in Post

      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{id: created_post_id, title: "new_post_title"}} =
        Actions.find_or_create(query, %{id: post_id, title: "new_post_title"})

      assert post_id !== created_post_id
    end
  end

  describe "find_and_create/3 : " do
    test "queryable - fetches result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "existing_post_title"})

      assert {:ok, %{id: ^post_id, title: "existing_post_title"}} =
        Actions.find_and_create(Post, %{id: post_id}, %{title: "new_post_title"})
    end

    test "{source, queryable} - fetches result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "existing_post_title"})

      assert {:ok, %{id: ^post_id, title: "existing_post_title"}} =
        Actions.find_and_create({"posts", Post}, %{id: post_id}, %{title: "new_post_title"})
    end

    test "query - fetches result matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "existing_post_title"})

      assert {:ok, %{id: ^post_id, title: "existing_post_title"}} =
        Actions.find_and_create(query, %{id: post_id}, %{title: "new_post_title"})
    end

    test "queryable - creates result matching params if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{id: created_post_id, title: "new_post_title"}} =
        Actions.find_and_create(Post, %{id: post_id}, %{title: "new_post_title"})

      assert post_id !== created_post_id
    end

    test "{source, queryable} - creates result matching params if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{id: created_post_id, title: "new_post_title"}} =
        Actions.find_and_create({"posts", Post}, %{id: post_id}, %{title: "new_post_title"})

      assert post_id !== created_post_id
    end

    test "query - creates result matching params if not found" do
      query = from p in Post

      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{id: created_post_id, title: "new_post_title"}} =
        Actions.find_and_create(query, %{id: post_id}, %{title: "new_post_title"})

      assert post_id !== created_post_id
    end
  end

  describe "find_and_update/2 : " do
    test "queryable - fetches and updates result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id, title: "updated_post_title"}} =
        Actions.find_and_update(Post, %{id: post_id}, %{title: "updated_post_title"})
    end

    test "{source, queryable} - fetches and updates result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id, title: "updated_post_title"}} =
        Actions.find_and_update({"posts", Post}, %{id: post_id}, %{title: "updated_post_title"})
    end

    test "query - fetches and updates result matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id, title: "updated_post_title"}} =
        Actions.find_and_update(query, %{id: post_id}, %{title: "updated_post_title"})
    end

    test "queryable - returns error message if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %{code: :not_found}} =
        Actions.find_and_update(Post, %{id: post_id}, %{title: "updated_post_title"})
    end

    test "{source, queryable} - returns error message if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %{code: :not_found}} =
        Actions.find_and_update({"posts", Post}, %{id: post_id}, %{title: "updated_post_title"})
    end

    test "query - returns error message if not found" do
      query = from p in Post

      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %{code: :not_found}} =
        Actions.find_and_update(query, %{id: post_id}, %{title: "updated_post_title"})
    end
  end

  describe "find_and_upsert/3 : " do
    test "queryable - fetches and updates result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{title: "updated_post_title"}} =
        Actions.find_and_upsert(Post, %{id: post_id}, %{title: "updated_post_title"})
    end

    test "{source, queryable} - fetches and updates result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{title: "updated_post_title"}} =
        Actions.find_and_upsert({"posts", Post}, %{id: post_id}, %{title: "updated_post_title"})
    end

    test "query - fetches and updates result matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{title: "updated_post_title"}} =
        Actions.find_and_upsert(query, %{id: post_id}, %{title: "updated_post_title"})
    end

    test "queryable - creates result matching params if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{title: "new_post_title"}} =
        Actions.find_and_upsert(Post, %{id: post_id}, %{title: "new_post_title"})
    end

    test "{source, queryable} - creates result matching params if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{title: "new_post_title"}} =
        Actions.find_and_upsert({"posts", Post}, %{id: post_id}, %{title: "new_post_title"})
    end

    test "query - creates result matching params if not found" do
      query = from p in Post

      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:ok, %{title: "new_post_title"}} =
        Actions.find_and_upsert(query, %{id: post_id}, %{title: "new_post_title"})
    end
  end

  describe "get/2 : " do
    test "queryable - return nil when record does not exist" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert nil === Actions.get(Post, post_id)
    end

    test "{source, queryable} - return nil when record does not exist" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert nil === Actions.get({"posts", Post}, post_id)
    end

    test "query - return nil when record does not exist" do
      query = from p in Post

      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert nil === Actions.get(query, post_id)
    end

    test "queryable - return result with matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert %{id: ^post_id} = Actions.get(Post, post_id)
    end

    test "{source, queryable} - return result with matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert %{id: ^post_id} = Actions.get({"posts", Post}, post_id)
    end

    test "query - return result with matching id" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert %{id: ^post_id} = Actions.get(query, post_id)
    end
  end

  describe "all/1 : " do
    test "queryable - return all results" do
      assert {:ok, %{id: post_1_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: post_2_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_1_id}, %{id: ^post_2_id}] = Actions.all(Post)
    end

    test "{source, queryable} - return all results" do
      assert {:ok, %{id: post_1_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: post_2_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_1_id}, %{id: ^post_2_id}] = Actions.all({"posts", Post})
    end

    test "query - return results" do
      query = from p in Post

      assert {:ok, %{id: post_1_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: post_2_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_1_id}, %{id: ^post_2_id}] = Actions.all(query)
    end
  end

  describe "all/2 : " do
    test "queryable - return results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all(Post, %{id: post_id})
    end

    test "{source, queryable} - return results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all({"posts", Post}, %{id: post_id})
    end

    test "query - return results matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all(query, %{id: post_id})
    end

    test "queryable - return results matching keyword params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all(Post, [id: post_id])
    end

    test "{source, queryable} - return results matching keyword params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all({"posts", Post}, [id: post_id])
    end

    test "query - return results matching keyword params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all(query, [id: post_id])
    end

    test "return results in order when :group_by and :order_by set in params" do
      assert {:ok, %{id: post_1_id}} = Actions.create(Post, %{likes: 1})
      assert {:ok, %{id: post_2_id}} = Actions.create(Post, %{likes: 2})

      assert [%{id: ^post_2_id}, %{id: ^post_1_id}] =
        Actions.all(Post, %{group_by: :id, order_by: [{:desc, :likes}]})
    end
  end

  describe "all/3 : " do
    test "queryable - return results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all(Post, %{id: post_id}, [])
    end

    test "{source, queryable} - return results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all({"posts", Post}, %{id: post_id}, [])
    end

    test "query - return results matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all(query, %{id: post_id}, [])
    end

    test "queryable - return results matching keyword params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all(Post, [id: post_id], [])
    end

    test "{source, queryable} - return results matching keyword params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all({"posts", Post}, [id: post_id], [])
    end

    test "query - return results matching keyword params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert [%{id: ^post_id}] = Actions.all(query, [id: post_id], [])
    end

    test "return results in order when :group_by and :order_by set in options" do
      assert {:ok, post_1} = Actions.create(Post, %{likes: 1})

      assert {:ok, post_2} = Actions.create(Post, %{likes: 2})

      assert [^post_2, ^post_1] = Actions.all(Post, %{}, group_by: :id, order_by: [{:desc, :likes}])
    end
  end

  describe "create/2 : " do
    test "queryable - create record matching params" do
      assert {:ok, %{title: "post_title"}} = Actions.create(Post, %{title: "post_title"})
    end

    test "{source, queryable} - create record matching params" do
      assert {:ok, %{title: "post_title"}} = Actions.create({"posts", Post}, %{title: "post_title"})
    end

    test "queryable - return changeset error when params are invalid" do
      assert {:error, changeset} = Actions.create(Post, %{title: "short"})

      assert {:title, ["should be at least 10 character(s)"]} in errors_on(changeset)
    end

    test "{source, queryable} - return changeset error when params are invalid" do
      assert {:error, changeset} = Actions.create({"posts", Post}, %{title: "short"})

      assert {:title, ["should be at least 10 character(s)"]} in errors_on(changeset)
    end
  end

  describe "create/3 : " do
    test "queryable - create record matching params" do
      assert {:ok, %{title: "post_title"}} = Actions.create(Post, %{title: "post_title"}, [])
    end

    test "{source, queryable} - create record matching params" do
      assert {:ok, %{title: "post_title"}} = Actions.create({"posts", Post}, %{title: "post_title"}, [])
    end

    test "queryable - return changeset error when params are invalid" do
      assert {:error, changeset} = Actions.create(Post, %{title: "short"}, [])

      assert {:title, ["should be at least 10 character(s)"]} in errors_on(changeset)
    end

    test "{source, queryable} - return changeset error when params are invalid" do
      assert {:error, changeset} = Actions.create({"posts", Post}, %{title: "short"}, [])

      assert {:title, ["should be at least 10 character(s)"]} in errors_on(changeset)
    end
  end

  describe "find/2 : " do
    test "queryable - fetches a single result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.find(Post, %{id: post_id})
    end

    test "{source, queryable} - fetches a single result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.find({"posts", Post}, %{id: post_id})
    end

    test "query - fetches a single result" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_title"})

      assert {:ok, %{id: ^post_id, title: "post_title"}} = Actions.find(query, %{id: post_id})
    end

    test "queryable - return not found error when params empty" do
      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{},
          query: Post
        },
        message: "no records found"
      }} = Actions.find(Post, %{})
    end

    test "{source, queryable} - return not found error when params empty" do
      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{},
          query: {"posts", Post}
        },
        message: "no records found"
      }} = Actions.find({"posts", Post}, %{})
    end

    test "queryable - returns error message if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{id: ^post_id},
          query: Post
        },
        message: "no records found"
      }} = Actions.find(Post, %{id: post_id})
    end

    test "{source, queryable} - returns error message if not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          params: %{id: ^post_id},
          query: {"posts", Post}
        },
        message: "no records found"
      }} = Actions.find({"posts", Post}, %{id: post_id})
    end
  end

  describe "update/3 : " do
    test "queryable - update a single result with matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id, title: "updated_post_title"}} =
        Actions.update(Post, post_id, %{title: "updated_post_title"})
    end

    test "{source, queryable} - update a single result with matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id, title: "updated_post_title"}} =
        Actions.update({"posts", Post}, post_id, %{title: "updated_post_title"})
    end

    test "query - update a single result with matching id" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id, title: "updated_post_title"}} =
        Actions.update(query, post_id, %{title: "updated_post_title"})
    end

    test "queryable - update a single result matching schema data" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id, title: "updated_post_title"}} =
        Actions.update(Post, schema_data, %{title: "updated_post_title"})
    end

    test "{source, queryable} - update a single result matching schema data" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id, title: "updated_post_title"}} =
        Actions.update({"posts", Post}, schema_data, %{title: "updated_post_title"})
    end

    test "queryable - update a single result with matching id and keyword params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok,%{id: ^post_id, title: "updated_post_title"}} =
        Actions.update(Post, post_id, [title: "updated_post_title"])
    end

    test "{source, queryable} - update a single result with matching id and keyword params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok,%{id: ^post_id, title: "updated_post_title"}} =
        Actions.update({"posts", Post}, post_id, [title: "updated_post_title"])
    end

    test "query - update a single result with matching id and keyword params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok,%{id: ^post_id, title: "updated_post_title"}} =
        Actions.update(query, post_id, [title: "updated_post_title"])
    end

    test "queryable - returns error message when result with matching id not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          query: Post,
          params: %{id: ^post_id}
        },
        message: "no records found"
      }} = Actions.update(Post, post_id, %{title: "updated_post_title"})
    end

    test "{source, queryable} - returns error message when result with matching id not found" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          query: {"posts", Post},
          params: %{id: ^post_id}
        },
        message: "no records found"
      }} = Actions.update({"posts", Post}, post_id, %{title: "updated_post_title"})
    end

    test "query - returns error message when result with matching id not found" do
      query = from p in Post

      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, _} = Repo.delete(schema_data)

      assert {:error, %ErrorMessage{
        code: :not_found,
        details: %{
          query: ^query,
          params: %{id: ^post_id}
        },
        message: "no records found"
      }} = Actions.update(query, post_id, %{title: "updated_post_title"})
    end
  end

  describe "delete/1 : " do
    test "delete a single result matching changeset" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      changeset = Post.changeset(schema_data, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(changeset)
    end

    test "data - delete a single result matching schema" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(schema_data)
    end

    test "list of changeset - delete many changesets" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      changeset = Post.changeset(schema_data, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete([changeset])
    end

    test "list of data - delete many schemas" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete([schema_data])
    end

    test "data - return changeset with constraint error" do
      assert {:ok, post} = Actions.create(Post, %{title: "post_title"})

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

    test "changeset - return changeset with constraint error"  do
      assert {:ok, post} = Actions.create(Post, %{title: "post_title"})

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

  describe "delete/2 : " do
    test "queryable - find and delete a single result with matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(Post, post_id)
    end

    test "{source, queryable} - delete a single result with matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete({"posts", Post}, post_id)
    end

    test "query - find and delete a single result matching query and id" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(query, post_id)
    end

    test "queryable - find and delete many results matching list of id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete(Post, [post_id])
    end

    test "{source, queryable} - find and delete many results matching list of id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete({"posts", Post}, [post_id])
    end

    test "query - find and delete many results matching list of id" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete(query, [post_id])
    end

    test "queryable - find and delete a single result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(Post, %{id: post_id})
    end

    test "{source, queryable} - find and delete a single result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete({"posts", Post}, %{id: post_id})
    end

    test "query - find and delete a single result matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(query, %{id: post_id})
    end

    test "queryable - find and delete a single result matching list of params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete(Post, [%{id: post_id}])
    end

    test "{source, queryable} - find and delete a single result matching list of params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete({"posts", Post}, [%{id: post_id}])
    end

    test "query - find and delete a single result matching list of params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete(query, [%{id: post_id}])
    end

    test "changeset - delete changeset" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      changeset = Post.changeset(schema_data, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(changeset, [])
    end

    test "changeset - delete list of changeset" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      changeset = Post.changeset(schema_data, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete([changeset], [])
    end

    test "data - delete schema data" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(schema_data, [])
    end

    test "data - delete list of schema data" do
      assert {:ok, %{id: post_id} = schema_data} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete([schema_data], [])
    end

    test "data - return list of errors" do
      assert {:ok, post_1} = Actions.create(Post, %{})

      assert {:ok, %{id: post_2_id} = post_2} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_2_id})

      assert {:error, [
        %ErrorMessage{
          code: :internal_server_error,
          details: %{
            changeset: %Ecto.Changeset{data: %{id: ^post_2_id}} = changeset,
            query: EctoShorts.Support.Schemas.Post
          },
          message: "failed to delete record"
        }
      ]} = Actions.delete([post_1, post_2], [])

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end

    test "queryable - return constraint error" do
      assert {:ok, post} = Actions.create(Post, %{title: "post_title"})

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

    test "{source, queryable} - return constraint error" do
      assert {:ok, post} = Actions.create(Post, %{title: "post_title"})

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

    test "query - return constraint error" do
      query = from p in Post

      assert {:ok, post} = Actions.create(Post, %{title: "post_title"})

      assert {:ok, _comment} = Actions.create(Comment, %{post_id: post.id})

      assert {:error, %{
        code: :internal_server_error,
        message: "failed to delete record",
        details: %{
          changeset: changeset,
          schema_data: ^post,
          query: Post
        }
      }} = Actions.delete(query, post.id)

      assert {:comments, ["are still associated with this entry"]} in errors_on(changeset)
    end
  end

  describe "delete/3 : " do
    test "queryable - fetch and delete a single result with matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(Post, post_id, [])
    end

    test "{source, queryable} - fetch and delete a single result with matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete({"posts", Post}, post_id, [])
    end

    test "query - fetch and delete a single result matching query and id" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(query, post_id, [])
    end

    test "queryable - fetch and delete many results matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete(Post, [post_id], [])
    end

    test "{source, queryable} - fetch and delete many results matching id" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete({"posts", Post}, [post_id], [])
    end

    test "query - fetch and delete many results matching query and filter id" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete(query, [post_id], [])
    end

    test "queryable - fetch and delete a single result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(Post, %{id: post_id}, [])
    end

    test "{source, queryable} - fetch and delete a single result matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete({"posts", Post}, %{id: post_id}, [])
    end

    test "query - fetch and delete a single result matching query and params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, %{id: ^post_id}} = Actions.delete(query, %{id: post_id}, [])
    end

    test "queryable - fetch and delete many results matching list of params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete(Post, [%{id: post_id}], [])
    end

    test "{source, queryable} - fetch and delete many results matching list of params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete({"posts", Post}, [%{id: post_id}], [])
    end

    test "query - fetch and delete many results matching query and list of params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} = Actions.delete(query, [%{id: post_id}], [])
    end
  end

  describe "stream/1 : " do
    test "queryable - return enumerable" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} =
        Repo.transaction(fn ->
          Post
          |> Actions.stream()
          |> Enum.to_list()
        end)
    end

    test "{source, queryable} - return enumerable" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} =
        Repo.transaction(fn ->
          {"posts", Post}
          |> Actions.stream()
          |> Enum.to_list()
        end)
    end

    test "query - return enumerable" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})

      assert {:ok, [%{id: ^post_id}]} =
        Repo.transaction(fn ->
          query
          |> Actions.stream()
          |> Enum.to_list()
        end)
    end
  end

  describe "aggregate/4 : " do
    test "queryable - count" do
      assert {:ok, _} = Actions.create(Post, %{})

      assert 1 = Actions.aggregate(Post, %{}, :count, :id)
    end

    test "{source, queryable} - count" do
      assert {:ok, _} = Actions.create(Post, %{})

      assert 1 = Actions.aggregate({"posts", Post}, %{}, :count, :id)
    end

    test "query - count" do
      query = from p in Post

      assert {:ok, _} = Actions.create(Post, %{})

      assert 1 = Actions.aggregate(query, %{}, :count, :id)
    end

    test "queryable - sum" do
      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 2})

      assert 3 = Actions.aggregate(Post, %{}, :sum, :likes)
    end

    test "{source, queryable} - sum" do
      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 2})

      assert 3 = Actions.aggregate({"posts", Post}, %{}, :sum, :likes)
    end

    test "query - sum" do
      query = from p in Post

      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 2})

      assert 3 = Actions.aggregate(query, %{}, :sum, :likes)
    end

    test "queryable - avg" do
      assert {:ok, _} = Actions.create(Post, %{likes: 2})
      assert {:ok, _} = Actions.create(Post, %{likes: 2})

      expected_decimal = Decimal.new("2.0000000000000000")

      assert ^expected_decimal = Actions.aggregate(Post, %{}, :avg, :likes)
    end

    test "{source, queryable} - avg" do
      assert {:ok, _} = Actions.create(Post, %{likes: 2})
      assert {:ok, _} = Actions.create(Post, %{likes: 2})

      expected_decimal = Decimal.new("2.0000000000000000")

      assert ^expected_decimal = Actions.aggregate({"posts", Post}, %{}, :avg, :likes)
    end

    test "query - avg" do
      query = from p in Post

      assert {:ok, _} = Actions.create(Post, %{likes: 2})
      assert {:ok, _} = Actions.create(Post, %{likes: 2})

      expected_decimal = Decimal.new("2.0000000000000000")

      assert ^expected_decimal = Actions.aggregate(query, %{}, :avg, :likes)
    end

    test "queryable - min" do
      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 20})

      assert 1 = Actions.aggregate(Post, %{}, :min, :likes)
    end

    test "{source, queryable} - min" do
      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 20})

      assert 1 = Actions.aggregate({"posts", Post}, %{}, :min, :likes)
    end

    test "query - min" do
      query = from p in Post

      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 20})

      assert 1 = Actions.aggregate(query, %{}, :min, :likes)
    end

    test "queryable - max" do
      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 20})

      assert 20 = Actions.aggregate(Post, %{}, :max, :likes)
    end

    test "{source, queryable} - max" do
      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 20})

      assert 20 = Actions.aggregate({"posts", Post}, %{}, :max, :likes)
    end

    test "query - max" do
      query = from p in Post

      assert {:ok, _} = Actions.create(Post, %{likes: 1})
      assert {:ok, _} = Actions.create(Post, %{likes: 20})

      assert 20 = Actions.aggregate(query, %{}, :max, :likes)
    end
  end

  describe "transaction/2" do
    test "when given an ecto multi, returns ecto multi response" do
      assert {:ok, %{example: "success"}} =
        Multi.new()
        |> Multi.run(:example, fn _repo, _changes -> {:ok, "success"} end)
        |> Actions.transaction()
    end

    test "when given an ecto multi, returns ecto multi error" do
      assert {:error, :example, "failed", %{}} =
        Multi.new()
        |> Multi.run(:example, fn _repo, _changes -> {:error, "failed"} end)
        |> Actions.transaction()
    end

    test "when 0-arity function returns {:ok, term}, return {:ok, {:ok, term}}" do
      assert {:ok, {:ok, %Post{id: 1}}} = Actions.transaction(fn -> {:ok, %Post{id: 1}} end)
    end

    test "when 1-arity function returns {:ok, term}, return {:ok, {:ok, term}}" do
      assert {:ok, {:ok, %Post{id: 1}}} = Actions.transaction(fn _repo -> {:ok, %Post{id: 1}} end)
    end

    test "when 0-arity function returns :ok, return {:ok, :ok}" do
      assert {:ok, :ok} =  Actions.transaction(fn -> :ok end)
    end

    test "when 1-arity function returns :ok, return {:ok, :ok}" do
      assert {:ok, :ok} =  Actions.transaction(fn _repo -> :ok end)
    end

    test "when option :rollback_on_error not set and function returns a term, roll back and return {:error, term}" do
      assert {:error, ["some_string"]} =
        Actions.transaction(fn ->
          with {:ok, _} <- Actions.create(Post, %{unique_identifier: "duplicate"}) do
            ["some_string"]
          end
        end)

      assert [] = Actions.all(Post)
    end

    test "when option :rollback_on_error is true and function returns a term, roll back and return {:error, term}" do
      assert {:error, ["some_string"]} =
        Actions.transaction(
          fn ->
            with {:ok, _} <- Actions.create(Post, %{unique_identifier: "duplicate"}) do
              ["some_string"]
            end
          end,
          rollback_on_error: true
        )

      assert [] = Actions.all(Post)
    end

    test "when option :rollback_on_error is false and function returns a term, does not roll back and return {:ok, term}" do
      assert {:ok, ["some_string"]} =
        Actions.transaction(
          fn ->
            with {:ok, _} <- Actions.create(Post, %{title: "post_title"}) do
              ["some_string"]
            end
          end,
          rollback_on_error: false
        )

      assert [%Post{title: "post_title"}] = Actions.all(Post)
    end

    test "when option :rollback_on_error not set and constraint violation occurs, roll back and return {:error, changeset}" do
      assert {:error, {:error, changeset}} =
        Actions.transaction(fn ->
          with {:ok, _} <- Actions.create(Post, %{unique_identifier: "duplicate"}) do
            Actions.create(Post, %{unique_identifier: "duplicate"})
          end
        end)

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert [] = Actions.all(Post)
    end

    test "when option :rollback_on_error is true and constraint violation occurs, roll back and return {:error, changeset}" do
      assert {:error, {:error, changeset}} =
        Actions.transaction(
          fn ->
            with {:ok, _} <- Actions.create(Post, %{unique_identifier: "duplicate"}) do
              Actions.create(Post, %{unique_identifier: "duplicate"})
            end
          end,
          rollback_on_error: true
        )

      assert {:unique_identifier, ["has already been taken"]} in errors_on(changeset)

      assert [] = Actions.all(Post)
    end

    test "when option :rollback_on_error not set and function returns :error, roll back and return {:error, :error}" do
      assert {:error, :error} =
        Actions.transaction(fn ->
          with {:ok, _} <- Actions.create(Post, %{title: "post_title"}) do
            :error
          end
        end)

      assert [] = Actions.all(Post)
    end

    test "when option :rollback_on_error is true and function returns :error, roll back and return {:error, :error}" do
      assert {:error, :error} =
        Actions.transaction(
          fn ->
            with {:ok, _} <- Actions.create(Post, %{title: "post_title"}) do
              :error
            end
          end,
          rollback_on_error: true
        )

      assert [] = Actions.all(Post)
    end

    test "when option :rollback_on_error is false and constraint violation occurs, returns {:error, :rollback}" do
      # When a constraint violation occurs the entire transaction is voided and {:error, rollback} is returned.
      # This is the expected behaviour: https://github.com/elixir-ecto/ecto/issues/2617#issuecomment-404795075

      assert {:error, :rollback} =
        Actions.transaction(
          fn ->
            Enum.map(1..2, fn _ ->
              Actions.create(Post, %{title: "post_1_title", unique_identifier: "duplicate"})
            end)
          end,
          rollback_on_error: false
        )

      assert [] = Actions.all(Post)
    end

    test "when option :rollback_on_error is false and function returns {:error, term}, does not roll back and return {:ok, {:error, term}}" do
      assert {:ok, {:error, "failed"}} =
        Actions.transaction(
          fn ->
            with {:ok, _} <- Actions.create(Post, %{title: "post_title"}) do
              {:error, "failed"}
            end
          end,
          rollback_on_error: false
        )

      assert [%Post{title: "post_title"}] = Actions.all(Post)
    end

    test "when option :rollback_on_error is false and function returns :error, does not roll back and return {:ok, :error}" do
      assert {:ok, :error} =
        Actions.transaction(
          fn ->
            with {:ok, _} <- Actions.create(Post, %{title: "post_title"}) do
              :error
            end
          end,
          rollback_on_error: false
        )

      assert [%Post{title: "post_title"}] = Actions.all(Post)
    end
  end

  describe "batch_all/5 : " do
    test "queryable - returns results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        Actions.batch_all(Post, :id, [post_id], %{title: "post_created_title"}, :set)
    end

    test "{source, queryable} - returns results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        Actions.batch_all({"posts", Post}, :id, [post_id], %{title: "post_created_title"}, :set)
    end

    test "query - returns results matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        Actions.batch_all(query, :id, [post_id], %{title: "post_created_title"}, :set)
    end

    test "queryable - returns a map with values as a single result" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        Actions.batch_all(Post, :id, [post_id], %{}, :set)
    end

    test "{source, queryable} - returns a map with values as a single result" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        Actions.batch_all({"posts", Post}, :id, [post_id], %{}, :set)
    end

    test "query - returns a map with values as a single result" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        Actions.batch_all(query, :id, [post_id], %{}, :set)
    end

    test "queryable - returns a map with values as lists of results" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        Actions.batch_all(Post, :id, [post_id], %{}, :bag)
    end

    test "{source, queryable} - returns a map with values as lists of results" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        Actions.batch_all({"posts", Post}, :id, [post_id], %{}, :bag)
    end

    test "query - returns a map with values as lists of results" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        Actions.batch_all(query, :id, [post_id], %{}, :bag)
    end
  end

  describe "batch_all/6 : " do
    test "queryable - returns results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        Actions.batch_all(Post, :id, [post_id], %{title: "post_created_title"}, :set, [])
    end

    test "{source, queryable} - returns results matching params" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        Actions.batch_all({"posts", Post}, :id, [post_id], %{title: "post_created_title"}, :set, [])
    end

    test "query - returns results matching params" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{title: "post_created_title"})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id, title: "post_created_title"}} =
        Actions.batch_all(query, :id, [post_id], %{title: "post_created_title"}, :set, [])
    end

    test "queryable - returns a map with values as a single result" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        Actions.batch_all(Post, :id, [post_id], %{}, :set, [])
    end

    test "{source, queryable} - returns a map with values as a single result" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        Actions.batch_all({"posts", Post}, :id, [post_id], %{}, :set, [])
    end

    test "query - returns a map with values as a single result" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => %{id: ^post_id}} =
        Actions.batch_all(query, :id, [post_id], %{}, :set, [])
    end

    test "queryable - returns a map with values as lists of results" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        Actions.batch_all(Post, :id, [post_id], %{}, :bag, [])
    end

    test "{source, queryable} - returns a map with values as lists of results" do
      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        Actions.batch_all({"posts", Post}, :id, [post_id], %{}, :bag, [])
    end

    test "query - returns a map with values as lists of results" do
      query = from p in Post

      assert {:ok, %{id: post_id}} = Actions.create(Post, %{})
      assert {:ok, _} = Actions.create(Comment, %{post_id: post_id})

      assert %{^post_id => [%{id: ^post_id}]} =
        Actions.batch_all(query, :id, [post_id], %{}, :bag, [])
    end
  end

  describe "&insert_all/2: " do
    test "when given params and params are valid, returns created records" do
      assert {:ok, {1, nil}} =
        Actions.insert_all(Post, [%{title: "post_1_title"}])

      assert [%Post{title: "post_1_title"}] = Actions.all(Post)
    end
  end

  describe "&insert_all/3: " do
    test "when given source, inserts records into table" do
      assert {:ok, params} = EctoShorts.CommonParams.convert_to_insert_all_params(Post, [%{title: "post_1_title"}])

      assert {:ok, {1, nil}} = Actions.insert_all("posts", params)

      assert [%Post{title: "post_1_title"}] = Actions.all(Post)
    end

    test "when given {source, queryable} and valid params, creates params with queryable changeset and inserts into table" do
      assert {:ok, {1, nil}} = Actions.insert_all({"posts", Post}, [%{title: "post_1_title"}])

      assert [%Post{title: "post_1_title"}] = Actions.all(Post)
    end

    test "when given params and params are valid, returns created records" do
      assert {
        :ok,
        {
          2,
          [
            %Post{
              title: "post_1_title",
              inserted_at: returned_post_1_inserted_at,
              updated_at: returned_post_1_updated_at
            },
            %Post{
              title: "post_2_title",
              inserted_at: returned_post_2_inserted_at,
              updated_at: returned_post_2_updated_at
            }
          ]
        }
      } = Actions.insert_all(Post, [%{title: "post_1_title"}, %{title: "post_2_title"}], returning: true)

      assert NaiveDateTime.diff(returned_post_1_inserted_at, NaiveDateTime.utc_now()) <= 0
      assert NaiveDateTime.diff(returned_post_1_updated_at, NaiveDateTime.utc_now()) <= 0

      assert NaiveDateTime.diff(returned_post_2_inserted_at, NaiveDateTime.utc_now()) <= 0
      assert NaiveDateTime.diff(returned_post_2_updated_at, NaiveDateTime.utc_now()) <= 0
    end

    test "when given {changeset, params} and params are valid, returns updated records" do
      assert {:ok, %{id: post_1_id} = post_1} = Actions.create(Post, %{})
      assert {:ok, %{id: post_2_id} = post_2} = Actions.create(Post, %{})

      post_1_changeset = Post.changeset(post_1, %{})
      post_2_changeset = Post.changeset(post_2, %{})

      assert {
        :ok,
        {
          2,
          [
            %Post{
              id: ^post_1_id,
              title: "post_1_updated_title",
              inserted_at: returned_post_1_inserted_at,
              updated_at: returned_post_1_updated_at
            },
            %Post{
              id: ^post_2_id,
              title: "post_2_updated_title",
              inserted_at: returned_post_2_inserted_at,
              updated_at: returned_post_2_updated_at
            }
          ]
        }
      } =
        Actions.insert_all(
          Post,
          [
            {post_1_changeset, %{title: "post_1_updated_title"}},
            {post_2_changeset, %{title: "post_2_updated_title"}}
          ],
          returning: true,
          on_conflict: {:replace_all_except, [:id, :inserted_at]},
          conflict_target: [:id]
        )

      assert returned_post_1_inserted_at === post_1.inserted_at
      assert NaiveDateTime.diff(returned_post_1_updated_at, post_1.updated_at) >= 0

      assert returned_post_2_inserted_at === post_2.inserted_at
      assert NaiveDateTime.diff(returned_post_2_updated_at, post_2.updated_at) >= 0
    end

    test "when given {struct, params} and params are valid, returns updated records" do
      assert {:ok, %{id: post_1_id} = post_1} = Actions.create(Post, %{})
      assert {:ok, %{id: post_2_id} = post_2} = Actions.create(Post, %{})

      assert {
        :ok,
        {
          2,
          [
            %Post{
              id: ^post_1_id,
              title: "post_1_updated_title",
              inserted_at: returned_post_1_inserted_at,
              updated_at: returned_post_1_updated_at
            },
            %Post{
              id: ^post_2_id,
              title: "post_2_updated_title",
              inserted_at: returned_post_2_inserted_at,
              updated_at: returned_post_2_updated_at
            }
          ]
        }
      } =
        Actions.insert_all(
          Post,
          [
            {post_1, %{title: "post_1_updated_title"}},
            {post_2, %{title: "post_2_updated_title"}}
          ],
          returning: true,
          on_conflict: {:replace_all_except, [:id, :inserted_at]},
          conflict_target: [:id]
        )

      assert returned_post_1_inserted_at === post_1.inserted_at
      assert NaiveDateTime.diff(returned_post_1_updated_at, post_1.updated_at) >= 0

      assert returned_post_2_inserted_at === post_2.inserted_at
      assert NaiveDateTime.diff(returned_post_2_updated_at, post_2.updated_at) >= 0
    end

    test "when given {struct, params} and params has id, find and update or find and create (upsert) result" do

    end
  end
end
