defmodule EctoShorts.Actions.TransactionTest do
  use EctoShorts.DataCase, async: true

  alias Ecto.Changeset
  alias Ecto.Multi
  alias EctoShorts.Actions
  alias EctoShorts.Schema.Post

  describe "transact/2" do
    test "normalizes {:ok, :ok} to :ok" do
      assert :ok = Actions.transact(fn -> :ok end, repo: Repo)
    end

    test "rolls back and normalizes {:error, :error} to :error" do
      assert :error =
               Actions.transact(
                 fn repo ->
                   repo.insert!(%Post{title: "ShouldRollback"})
                   :error
                 end,
                 repo: Repo
               )
    end

    test "unwraps nested ok tuples in strict mode" do
      assert {:ok, 1} = Actions.transact(fn -> {:ok, {:ok, 1}} end, repo: Repo)
    end

    test "in non-strict mode, preserves status tuples as committed data" do
      assert {:ok, {:error, :reason}} =
               Actions.transact(fn -> {:error, :reason} end, repo: Repo, strict: false)
    end

    test "runs an Ecto.Multi and returns the operation results" do
      multi =
        Multi.new()
        |> Multi.insert(:post_a, Post.changeset(%Post{}, %{title: "A"}))
        |> Multi.insert(:post_b, Post.changeset(%Post{}, %{title: "B"}))

      assert {:ok, [%Post{title: "A"}, %Post{title: "B"}]} = Actions.transact(multi, repo: Repo)
    end

    test "returns {:error, changeset} when an Ecto.Multi operation fails and rolls back" do
      multi =
        Multi.new()
        |> Multi.insert(:post_a, Post.changeset(%Post{}, %{title: "A", permalink: "dup"}))
        |> Multi.insert(:post_b, Post.changeset(%Post{}, %{title: "B", permalink: "dup"}))

      assert {:error, %Changeset{}} = Actions.transact(multi, repo: Repo)
    end
  end

  describe "transaction/2" do
    test "wraps a function in a transaction and returns {:ok, result}" do
      assert {:ok, {:ok, %Post{title: "Transacted"}}} =
               Actions.transaction(fn ->
                 Actions.create(Post, %{title: "Transacted"})
               end)
    end
  end
end
