defmodule EctoShorts.Actions.BatchTest do
  use EctoShorts.DataCase, async: true

  alias EctoShorts.Actions
  alias EctoShorts.Schema.Post

  describe "batch/4" do
    test "returns an empty map when no params are given" do
      assert %{} = Actions.batch(Post, [], [:title], :many, [])
    end

    test "returns one record per key when cardinality is :one" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      result = Actions.batch(Post, [%{title: "A"}, %{title: "B"}], :title, :one, [])

      assert %{
               "A" => %Post{title: "A"},
               "B" => %Post{title: "B"}
             } = result
    end

    test "groups records by a single key when the key is an atom" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      result = Actions.batch(Post, [%{title: "A"}, %{title: "B"}], :title, :many, [])

      assert %{
               "A" => [%Post{title: "A"}],
               "B" => [%Post{title: "B"}]
             } = result
    end

    test "groups records by a list of keys" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      result = Actions.batch(Post, [%{title: "A"}, %{title: "B"}], [:title], :many, [])

      assert %{
               %{title: "A"} => [%Post{title: "A"}],
               %{title: "B"} => [%Post{title: "B"}]
             } = result
    end

    test "returns an empty map when params are missing the batch key" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      assert %{} = Actions.batch(Post, [%{permalink: "missing-title"}], [:title], :many, [])
    end
  end

  describe "batch_preload/4" do
    setup do
      post =
        %Post{}
        |> Post.changeset(%{title: "Existing", permalink: "existing"})
        |> Repo.insert!()

      %{post: post}
    end

    test "keeps the entry unchanged when it is a struct-and-map tuple", %{post: post} do
      input = {post, %{title: "Ignored"}}

      assert [result] = Actions.batch_preload(Post, [input], :permalink, [])
      assert result === input
    end

    test "keeps the entry unchanged when it is a struct-and-keyword tuple", %{post: post} do
      input = {post, [title: "Ignored"]}

      assert [result] = Actions.batch_preload(Post, [input], :permalink, [])
      assert result === input
    end

    test "replaces the map with the matching record when the entry is a map-and-map tuple", %{post: post} do
      input = {%{permalink: "existing"}, %{title: "New"}}

      assert [{%Post{id: id}, %{title: "New"}}] =
               Actions.batch_preload(Post, [input], :permalink, [])

      assert id === post.id
    end

    test "replaces the keyword list with the matching record when the entry is a keyword-and-keyword tuple",
         %{
           post: post
         } do
      input = {[permalink: "existing"], [title: "New"]}

      assert [{%Post{id: id}, [title: "New"]}] =
               Actions.batch_preload(Post, [input], :permalink, [])

      assert id === post.id
    end

    test "replaces the keyword list with the matching record when the entry is a keyword-and-map tuple", %{
      post: post
    } do
      input = {[permalink: "existing"], %{title: "New"}}

      assert [{%Post{id: id}, %{title: "New"}}] =
               Actions.batch_preload(Post, [input], :permalink, [])

      assert id === post.id
    end

    test "replaces the map with the matching record when the entry is a map-and-keyword tuple", %{
      post: post
    } do
      input = {%{permalink: "existing"}, [title: "New"]}

      assert [{%Post{id: id}, [title: "New"]}] =
               Actions.batch_preload(Post, [input], :permalink, [])

      assert id === post.id
    end

    test "wraps the map entry into a tuple with the matching record", %{post: post} do
      input = %{permalink: "existing", title: "New"}

      assert [{%Post{id: id}, params}] = Actions.batch_preload(Post, [input], :permalink, [])
      assert id === post.id
      assert params === input
    end

    test "wraps the keyword entry into a tuple with the matching record", %{post: post} do
      input = [permalink: "existing", title: "New"]

      assert [{%Post{id: id}, params}] = Actions.batch_preload(Post, [input], :permalink, [])
      assert id === post.id
      assert params === input
    end

    test "keeps a nil entry unchanged", %{post: _post} do
      assert [nil] = Actions.batch_preload(Post, [nil], :permalink, [])
    end
  end

  describe "batch/5 with :preload" do
    test "preloads associations on batched structs with :one cardinality" do
      %Post{}
      |> Post.changeset(%{title: "Alpha"})
      |> Repo.insert!()

      result = Actions.batch(Post, [%{title: "Alpha"}], :title, :one, preload: [:comments])

      assert %Post{comments: []} = result["Alpha"]
    end

    test "preloads associations on batched lists with :many cardinality" do
      %Post{}
      |> Post.changeset(%{title: "Beta"})
      |> Repo.insert!()

      result = Actions.batch(Post, [%{title: "Beta"}], :title, :many, preload: [:comments])

      assert [%Post{comments: []}] = result["Beta"]
    end
  end
end
