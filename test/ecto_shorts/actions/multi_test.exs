defmodule EctoShorts.Actions.MultiTest do
  use EctoShorts.DataCase

  alias Ecto.Changeset
  alias EctoShorts.Actions
  alias EctoShorts.Schema.Comment
  alias EctoShorts.Schema.Post

  describe "create_many/3" do
    test "inserts multiple records" do
      params = [
        %{title: "A"},
        %{title: "B"}
      ]

      assert {:ok, [%Post{title: "A"} = post_a, %Post{title: "B"} = post_b]} =
               Actions.create_many(Post, params)

      assert %Post{title: "A"} = Repo.get!(Post, post_a.id)
      assert %Post{title: "B"} = Repo.get!(Post, post_b.id)
    end

    test "returns {:error, error} and rolls back when a record fails validation" do
      params = [
        %{permalink: "exising"},
        %{permalink: "exising"}
      ]

      assert {:error,
              %{
                code: :conflict,
                message: "failed to create record.",
                details: %{
                  schema: Post,
                  action: :create,
                  index: 1,
                  changeset: %Changeset{},
                  params: %{permalink: "exising"}
                }
              }} = Actions.create_many(Post, params)
    end

    test "returns {:error, error} and rolls back on constraint failure" do
      params = [
        %{title: "A", permalink: "create-many-dup"},
        %{title: "B", permalink: "create-many-dup"}
      ]

      assert {:error,
              %{
                code: :conflict,
                message: "failed to create record.",
                details: %{
                  index: 1,
                  changeset: %Changeset{},
                  params: %{permalink: "create-many-dup"}
                }
              }} =
               Actions.create_many(Post, params)
    end
  end

  describe "find_many/3" do
    test "returns {:ok, records} when all are found" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      params = [
        %{title: "A"},
        %{title: "B"}
      ]

      assert {:ok, [%Post{title: "A"}, %Post{title: "B"}]} =
               Actions.find_many(Post, params)
    end

    test "returns {:error, error} when any record is not found (includes changes_so_far)" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      params = [
        %{title: "A"},
        %{title: "Missing"}
      ]

      assert {:error,
              %{
                code: :not_found,
                message: "record not found.",
                details: %{
                  schema: Post,
                  action: :find,
                  index: 1,
                  params: %{title: "Missing"},
                  changes_so_far: [%Post{title: "A"}]
                }
              }} =
               Actions.find_many(Post, params)
    end
  end

  describe "update_many/3" do
    test "updates multiple records successfully" do
      post_a =
        %Post{}
        |> Post.changeset(%{title: "Original A"})
        |> Repo.insert!()

      post_b =
        %Post{}
        |> Post.changeset(%{title: "Original B"})
        |> Repo.insert!()

      params = [
        %{id: post_a.id, title: "Updated A"},
        %{id: post_b.id, title: "Updated B"}
      ]

      assert {:ok, [%Post{title: "Updated A"}, %Post{title: "Updated B"}]} =
               Actions.update_many(Post, params)

      assert %Post{title: "Updated A"} = Repo.get!(Post, post_a.id)
      assert %Post{title: "Updated B"} = Repo.get!(Post, post_b.id)
    end

    test "returns {:error, error} and rolls back when a record fails validation" do
      post_a =
        %Post{}
        |> Post.changeset(%{title: "Valid"})
        |> Repo.insert!()

      post_b =
        %Post{}
        |> Post.changeset(%{title: "Also Valid"})
        |> Repo.insert!()

      params = [
        %{id: post_a.id, title: "Updated"},
        %{id: post_b.id, views: "not_an_integer"}
      ]

      assert {:error,
              %{
                code: :conflict,
                message: "failed to update record.",
                details: %{
                  schema: Post,
                  action: :update,
                  index: 1,
                  changeset: %Changeset{},
                  params: %{views: "not_an_integer"}
                }
              }} =
               Actions.update_many(Post, params)

      # transaction rollback: records should not be updated
      assert %Post{title: "Valid"} = Repo.get!(Post, post_a.id)
      assert %Post{title: "Also Valid"} = Repo.get!(Post, post_b.id)
    end

    test "returns {:error, error} when a record is not found" do
      post =
        %Post{}
        |> Post.changeset(%{title: "Existing"})
        |> Repo.insert!()

      params = [
        %{id: post.id, title: "Updated"},
        %{id: 999_999, title: "Not Found"}
      ]

      assert {:error, %{code: :not_found, message: "record not found.", details: details}} =
               Actions.update_many(Post, params)

      assert details.index === 1

      # transaction rollback: first record should not be updated
      assert %Post{title: "Existing"} = Repo.get!(Post, post.id)
    end
  end

  describe "delete_many/2" do
    test "deletes multiple records" do
      post_a =
        %Post{}
        |> Post.changeset(%{title: "A"})
        |> Repo.insert!()

      post_b =
        %Post{}
        |> Post.changeset(%{title: "B"})
        |> Repo.insert!()

      assert {:ok, [%Post{title: "A"}, %Post{title: "B"}]} =
               Actions.delete_many(Post, [post_a, post_b])

      assert Repo.get(Post, post_a.id) === nil
      assert Repo.get(Post, post_b.id) === nil
    end

    test "returns {:error, error} when a delete fails" do
      blocked =
        %Post{}
        |> Post.changeset(%{title: "Blocked"})
        |> Repo.insert!()

      %Comment{}
      |> Comment.changeset(%{body: "hello", post_id: blocked.id})
      |> Repo.insert!()

      assert {:error, %{code: :conflict, message: "failed to delete record."}} =
               Actions.delete_many(Post, [blocked])
    end
  end

  describe "find_or_create_many/3" do
    test "creates missing records" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      params = [
        %{title: "Existing"},
        %{title: "Created"}
      ]

      assert {:ok, [%Post{title: "Existing"}, %Post{title: "Created"}]} =
               Actions.find_or_create_many(Post, params)
    end
  end

  describe "find_and_upsert_many/3" do
    test "creates records when not found" do
      params = [
        {%{title: "A"}, %{title: "A"}},
        {%{title: "B"}, %{title: "B"}}
      ]

      assert {:ok, [%Post{title: "A"}, %Post{title: "B"}]} =
               Actions.find_and_upsert_many(Post, params)
    end

    test "updates a record when found" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      params = [
        {%{title: "A"}, %{title: "Updated"}}
      ]

      assert {:ok, [%Post{title: "Updated"}]} = Actions.find_and_upsert_many(Post, params)
    end
  end
end
