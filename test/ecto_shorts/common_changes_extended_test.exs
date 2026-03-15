defmodule EctoShorts.CommonChangesExtendedTest do
  use EctoShorts.DataCase, async: true

  alias Ecto.Changeset
  alias EctoShorts.{Actions, CommonChanges, Repo}
  alias EctoShorts.Schema.{Comment, Post}

  describe "has_nil_change?/2 with a list of fields" do
    test "returns true when all listed fields have nil changes" do
      changeset = Post.changeset(%Post{}, %{})
      assert CommonChanges.has_nil_change?(changeset, [:title, :permalink])
    end

    test "returns false when at least one listed field has a non-nil change" do
      changeset = Post.changeset(%Post{}, %{title: "hello"})
      refute CommonChanges.has_nil_change?(changeset, [:title, :permalink])
    end
  end

  describe "has_nil_change?/2 with a single field" do
    test "returns false when the field has a non-nil change" do
      changeset = Post.changeset(%Post{}, %{title: "hello"})
      refute CommonChanges.has_nil_change?(changeset, :title)
    end

    test "returns true when the field change is nil" do
      changeset = Post.changeset(%Post{}, %{})
      assert CommonChanges.has_nil_change?(changeset, :title)
    end
  end

  describe "has_empty_change?/2 with a list of fields" do
    test "returns true when all listed fields have empty changes" do
      changeset =
        %Post{}
        |> Post.changeset(%{})
        |> Changeset.put_change(:tags, [])
        |> Changeset.put_change(:permalink, nil)

      assert CommonChanges.has_empty_change?(changeset, [:tags])
    end

    test "returns false when at least one listed field has a non-empty change" do
      changeset = Post.changeset(%Post{}, %{title: "hello"})
      refute CommonChanges.has_empty_change?(changeset, [:title, :permalink])
    end
  end

  describe "has_empty_change?/2 with a single field" do
    test "returns true when the field change is an empty list" do
      changeset =
        %Post{}
        |> Post.changeset(%{})
        |> Changeset.put_change(:tags, [])

      assert CommonChanges.has_empty_change?(changeset, :tags)
    end

    test "returns true when the field change is an empty map" do
      changeset =
        %Post{}
        |> Post.changeset(%{})
        |> Changeset.put_change(:notes, %{})

      assert CommonChanges.has_empty_change?(changeset, :notes)
    end

    test "returns false when there is no change" do
      changeset = Post.changeset(%Post{}, %{})
      refute CommonChanges.has_empty_change?(changeset, :tags)
    end
  end

  describe "truncate_datetime_change/3 passthrough" do
    test "passes non-datetime values through unchanged" do
      changeset =
        %Post{}
        |> Post.changeset(%{})
        |> Changeset.put_change(:title, "keep me")

      result = CommonChanges.truncate_datetime_change(changeset, :title)
      assert Changeset.get_change(result, :title) == "keep me"
    end
  end

  describe "trim_string_change/2 passthrough" do
    test "passes non-string change values through unchanged" do
      changeset =
        %Post{}
        |> Post.changeset(%{})
        |> Changeset.put_change(:views, 42)

      result = CommonChanges.trim_string_change(changeset, :views)
      assert Changeset.get_change(result, :views) == 42
    end
  end

  describe "apply_when/3 raises on bad change function return" do
    test "raises ArgumentError when the change function returns a non-changeset" do
      changeset = Post.changeset(%Post{}, %{})

      assert_raise ArgumentError, ~r/Expected function to return a changeset/, fn ->
        CommonChanges.apply_when(
          changeset,
          fn _cs -> true end,
          fn _cs -> :not_a_changeset end
        )
      end
    end
  end

  describe "preload_change_assoc/2 with schema structs (put_assoc path)" do
    test "puts schema structs directly via put_assoc when cast params contain schema structs" do
      {:ok, post} = Actions.create(Post, %{title: "post"})

      {:ok, comment} =
        %Comment{}
        |> Comment.changeset(%{body: "comment one", post_id: post.id})
        |> Repo.insert()

      post = Repo.preload(post, :comments)

      changeset =
        post
        |> Post.changeset(%{comments: [comment]})
        |> CommonChanges.preload_change_assoc(:comments)

      assert is_struct(changeset, Changeset)
    end
  end

  describe "preload_change_assoc/2 member update path" do
    test "fetches existing records when cast params are id-only maps" do
      {:ok, post} = Actions.create(Post, %{title: "post"})

      {:ok, comment} =
        %Comment{}
        |> Comment.changeset(%{body: "long enough body", post_id: post.id})
        |> Repo.insert()

      post = Repo.preload(post, :comments)
      comment_id = comment.id

      changeset =
        post
        |> Post.changeset(%{comments: [%{id: comment_id}]})
        |> CommonChanges.preload_change_assoc(:comments)

      assert is_struct(changeset, Changeset)
    end
  end
end
