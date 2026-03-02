defmodule EctoShorts.Actions.BatchTest do
  use EctoShorts.DataCase, async: true

  alias EctoShorts.Actions
  alias EctoShorts.Schema.Post

  describe "batch/4" do
    test "returns empty map when params_list is empty" do
      assert %{} = Actions.batch(Post, [], [:title], :many, [])
    end

    test "when cardinality is :one, returns a single record per key (not a list)" do
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

    test "accepts a single batch_key (non-list) and returns scalar-keyed results" do
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

    test "returns records grouped by batch keys" do
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

    test "skips params missing required batch keys (does not fall back to returning all records)" do
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

    test "preload with {struct(), map()} leaves entry unchanged", %{post: post} do
      input = {post, %{title: "Ignored"}}

      assert [result] = Actions.batch_preload(Post, [input], :permalink, [])
      assert result === input
    end

    test "preload with {struct(), keyword()} leaves entry unchanged", %{post: post} do
      input = {post, [title: "Ignored"]}

      assert [result] = Actions.batch_preload(Post, [input], :permalink, [])
      assert result === input
    end

    test "preload with {map(), map()} replaces with {schema_struct, other_params}", %{post: post} do
      input = {%{permalink: "existing"}, %{title: "New"}}

      assert [{%Post{id: id}, %{title: "New"}}] =
               Actions.batch_preload(Post, [input], :permalink, [])

      assert id === post.id
    end

    test "preload with {keyword(), keyword()} replaces with {schema_struct, other_params}", %{
      post: post
    } do
      input = {[permalink: "existing"], [title: "New"]}

      assert [{%Post{id: id}, [title: "New"]}] =
               Actions.batch_preload(Post, [input], :permalink, [])

      assert id === post.id
    end

    test "preload with {keyword(), map()} replaces with {schema_struct, other_params}", %{
      post: post
    } do
      input = {[permalink: "existing"], %{title: "New"}}

      assert [{%Post{id: id}, %{title: "New"}}] =
               Actions.batch_preload(Post, [input], :permalink, [])

      assert id === post.id
    end

    test "preload with {map(), keyword()} replaces with {schema_struct, other_params}", %{
      post: post
    } do
      input = {%{permalink: "existing"}, [title: "New"]}

      assert [{%Post{id: id}, [title: "New"]}] =
               Actions.batch_preload(Post, [input], :permalink, [])

      assert id === post.id
    end

    test "preload with map() replaces with {schema_struct, params}", %{post: post} do
      input = %{permalink: "existing", title: "New"}

      assert [{%Post{id: id}, params}] = Actions.batch_preload(Post, [input], :permalink, [])
      assert id === post.id
      assert params === input
    end

    test "preload with keyword() replaces with {schema_struct, params}", %{post: post} do
      input = [permalink: "existing", title: "New"]

      assert [{%Post{id: id}, params}] = Actions.batch_preload(Post, [input], :permalink, [])
      assert id === post.id
      assert params === input
    end

    test "preload with nil leaves entry unchanged", %{post: _post} do
      assert [nil] = Actions.batch_preload(Post, [nil], :permalink, [])
    end
  end
end
