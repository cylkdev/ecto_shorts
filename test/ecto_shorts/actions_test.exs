defmodule EctoShorts.ActionsTest do
  use EctoShorts.DataCase

  alias Ecto.Changeset
  alias Ecto.Multi
  alias EctoShorts.Actions
  alias EctoShorts.Schema.Comment
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.User

  import Ecto.Query

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

  describe "exists?/3" do
    test "returns true when a matching record exists" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      assert Actions.exists?(Post, %{title: "Existing"}) === true
    end

    test "returns false when no matching record exists" do
      assert Actions.exists?(Post, %{title: "NonExistent"}) === false
    end
  end

  describe "all/3" do
    test "filters by id" do
      post_a =
        %Post{}
        |> Post.changeset(%{title: "A"})
        |> Repo.insert!()

      _post_b =
        %Post{}
        |> Post.changeset(%{title: "B"})
        |> Repo.insert!()

      assert [%Post{title: "A"}] = Actions.all(Post, %{id: post_a.id})
    end

    test "supports filter keys where and or_where" do
      %Post{}
      |> Post.changeset(%{title: "WhereMatch", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "OrWhereMatch", published: false})
      |> Repo.insert!()

      results =
        Actions.all(Post, %{
          where: %{published: true},
          or_where: %{title: "OrWhereMatch"},
          order_by: %{asc: :title}
        })

      assert Enum.map(results, & &1.title) === ["OrWhereMatch", "WhereMatch"]
    end

    test "filters on a join association" do
      author =
        %User{}
        |> User.changeset(%{first_name: "John"})
        |> Repo.insert!()

      other_author =
        %User{}
        |> User.changeset(%{first_name: "Jane"})
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Authored", permalink: "authored-actions", author_id: author.id})
      |> Repo.insert!()

      _other_post =
        %Post{}
        |> Post.changeset(%{
          title: "Other",
          permalink: "other-actions",
          author_id: other_author.id
        })
        |> Repo.insert!()

      assert [%Post{title: "Authored"}] = Actions.all(Post, %{author: %{first_name: "John"}})
    end

    test "applies order_by from opts" do
      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      results = Actions.all(Post, %{}, order_by: [asc: :title])

      assert [%Post{title: "A"}, %Post{title: "B"}] = results
    end

    test "opts order_by overrides params order_by" do
      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      results = Actions.all(Post, %{order_by: [desc: :title]}, order_by: [asc: :title])

      assert [%Post{title: "A"}, %Post{title: "B"}] = results
    end
  end

  describe "create/3" do
    test "inserts a record" do
      assert {:ok, %Post{title: "A"} = post} = Actions.create(Post, %{title: "A"})
      assert %Post{title: "A"} = Repo.get!(Post, post.id)
    end

    test "returns changeset error on insert constraint failure" do
      %Post{}
      |> Post.changeset(%{title: "A", permalink: "create-dup"})
      |> Repo.insert!()

      assert {:error, %Changeset{} = changeset} =
               Actions.create(Post, %{title: "B", permalink: "create-dup"})

      assert "has already been taken" in errors_on(changeset).permalink
    end

    test "applies changeset callback from opts" do
      assert {:ok, %Post{title: "Overridden"}} =
               Actions.create(
                 Post,
                 %{title: "Original"},
                 changeset: fn schema, schema_data_or_changeset, params ->
                   schema.changeset(
                     schema_data_or_changeset,
                     Map.put(params, :title, "Overridden")
                   )
                 end
               )
    end
  end

  describe "get/3" do
    test "returns the record when found" do
      post =
        %Post{}
        |> Post.changeset(%{title: "A"})
        |> Repo.insert!()

      assert %Post{title: "A"} = Actions.get(Post, post.id, repo: Repo)
    end

    test "returns nil when not found" do
      assert nil === Actions.get(Post, -1, repo: Repo)
    end
  end

  describe "find/3" do
    test "returns {:ok, record} when found" do
      post =
        %Post{}
        |> Post.changeset(%{title: "A"})
        |> Repo.insert!()

      assert {:ok, %Post{title: "A"}} = Actions.find(Post, %{id: post.id}, [])
    end

    test "returns {:error, error} when not found" do
      assert {:error, %{code: :not_found, message: "record not found.", details: details}} =
               Actions.find(Post, %{id: -1}, [])

      assert details.params === %{id: -1}
    end

    test "returns {:error, error} when params is empty and queryable is a schema module" do
      assert {:error, %{code: :not_found, message: "record not found."}} =
               Actions.find(Post, %{}, [])
    end

    test "allows empty params when queryable is an Ecto.Query" do
      %Post{}
      |> Post.changeset(%{title: "Only"})
      |> Repo.insert!()

      query = from(p in Post)

      assert {:ok, %Post{title: "Only"}} = Actions.find(query, %{}, [])
    end

    test "supports nested :preload" do
      author =
        %User{}
        |> User.changeset(%{first_name: "Nested"})
        |> Repo.insert!()

      post =
        %Post{}
        |> Post.changeset(%{title: "WithComments", author_id: author.id})
        |> Repo.insert!()

      %Comment{}
      |> Comment.changeset(%{body: "A comment", post_id: post.id, author_id: author.id})
      |> Repo.insert!()

      assert {:ok, %Post{title: "WithComments"} = result} =
               Actions.find(Post, %{id: post.id, preload: [comments: :author]}, [])

      assert [%Comment{body: "A comment"} = comment] = result.comments
      assert %User{first_name: "Nested"} = comment.author
    end
  end

  describe "update/4" do
    test "updates a record by id" do
      post =
        %Post{}
        |> Post.changeset(%{title: "Before"})
        |> Repo.insert!()

      assert {:ok, %Post{title: "After"}} = Actions.update(Post, post.id, %{title: "After"})
      assert %Post{title: "After"} = Repo.get!(Post, post.id)
    end

    test "updates a record by schema struct" do
      post =
        %Post{}
        |> Post.changeset(%{title: "Before"})
        |> Repo.insert!()

      assert {:ok, %Post{title: "After"}} = Actions.update(Post, post, %{title: "After"})
      assert %Post{title: "After"} = Repo.get!(Post, post.id)
    end

    test "returns {:error, error} when updating a missing id" do
      assert {:error, %{code: :not_found, message: "record not found.", details: details}} =
               Actions.update(Post, -1, %{title: "Ignored"})

      assert details.params === %{id: -1}
    end

    test "applies changeset callback from opts" do
      post =
        %Post{}
        |> Post.changeset(%{title: "Before"})
        |> Repo.insert!()

      assert {:ok, %Post{title: "Overridden"}} =
               Actions.update(
                 Post,
                 post.id,
                 %{title: "After"},
                 changeset: fn schema, schema_data_or_changeset, params ->
                   schema.changeset(
                     schema_data_or_changeset,
                     Map.put(params, :title, "Overridden")
                   )
                 end
               )
    end
  end

  describe "delete/2" do
    test "deletes a record by id" do
      post =
        %Post{}
        |> Post.changeset(%{title: "ToDelete"})
        |> Repo.insert!()

      assert {:ok, %Post{}} = Actions.delete(Post, post.id)
      assert Repo.get(Post, post.id) === nil
    end

    test "returns {:error, error} when deleting a missing id" do
      assert {:error, %{code: :not_found, message: "record not found.", details: details}} =
               Actions.delete(Post, -1)

      assert details.params === %{id: -1}
    end

    test "deletes a record by schema struct" do
      post =
        %Post{}
        |> Post.changeset(%{title: "ToDelete"})
        |> Repo.insert!()

      assert {:ok, %Post{}} = Actions.delete(post, [])
      assert Repo.get(Post, post.id) === nil
    end

    test "deletes a list of schema structs" do
      post_a =
        %Post{}
        |> Post.changeset(%{title: "A"})
        |> Repo.insert!()

      post_b =
        %Post{}
        |> Post.changeset(%{title: "B"})
        |> Repo.insert!()

      assert {:ok, [%Post{title: "A"}, %Post{title: "B"}]} = Actions.delete([post_a, post_b], [])
      assert Repo.get(Post, post_a.id) === nil
      assert Repo.get(Post, post_b.id) === nil
    end
  end

  describe "delete/3" do
    test "deletes a record by id with opts" do
      post =
        %Post{}
        |> Post.changeset(%{title: "ToDelete"})
        |> Repo.insert!()

      assert {:ok, %Post{}} = Actions.delete(Post, post.id, [])
      assert Repo.get(Post, post.id) === nil
    end

    test "returns {:error, error} when deleting a missing id with opts" do
      assert {:error, %{code: :not_found, message: "record not found.", details: details}} =
               Actions.delete(Post, -1, [])

      assert details.params === %{id: -1}
    end
  end

  describe "stream/3" do
    test "streams filtered results" do
      post_a =
        %Post{}
        |> Post.changeset(%{title: "A"})
        |> Repo.insert!()

      _post_b =
        %Post{}
        |> Post.changeset(%{title: "B"})
        |> Repo.insert!()

      assert {:ok, [%Post{title: "A"}]} =
               Repo.transaction(fn ->
                 Post
                 |> Actions.stream(%{id: post_a.id})
                 |> Enum.to_list()
               end)
    end

    test "supports order_by via params" do
      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      assert {:ok, [%Post{title: "A"}, %Post{title: "B"}]} =
               Repo.transaction(fn ->
                 Post
                 |> Actions.stream(%{order_by: %{asc: :title}})
                 |> Enum.to_list()
               end)
    end
  end

  describe "aggregate/5" do
    test "counts matching records (default options)" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      assert 2 === Actions.aggregate(Post)
      assert 1 === Actions.aggregate(Post, %{title: "A"})
    end

    test "supports non-count aggregate functions" do
      %Post{}
      |> Post.changeset(%{title: "Low", views: 1})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "High", views: 10})
      |> Repo.insert!()

      assert 10 === Actions.aggregate(Post, %{}, :max, :views)
      assert 1 === Actions.aggregate(Post, %{title: "Low"}, :max, :views)
    end
  end

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

  describe "find_and_create/3" do
    test "creates a record when not found" do
      assert {:ok, %Post{title: "Created"}} =
               Actions.find_and_create(Post, %{title: "Missing"}, %{title: "Created"})
    end

    test "returns {:ok, record} when found" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      assert {:ok, %Post{title: "Existing"}} =
               Actions.find_and_create(Post, %{title: "Existing"}, %{title: "Created"})
    end
  end

  describe "find_and_create/4" do
    test "creates a record when not found" do
      assert {:ok, %Post{title: "Created"}} =
               Actions.find_and_create(Post, %{title: "Missing"}, %{title: "Created"}, [])
    end

    test "returns {:ok, record} when found" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      assert {:ok, %Post{title: "Existing"}} =
               Actions.find_and_create(Post, %{title: "Existing"}, %{title: "Created"}, [])
    end
  end

  describe "find_and_update/3" do
    test "updates a record when found" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      assert {:ok, %Post{title: "Updated"}} =
               Actions.find_and_update(Post, %{title: "Existing"}, %{title: "Updated"})
    end

    test "returns {:error, error} when not found" do
      assert {:error, %{code: :not_found, message: "record not found."}} =
               Actions.find_and_update(Post, %{title: "Missing"}, %{title: "Updated"})
    end
  end

  describe "find_and_update/4" do
    test "updates a record when found" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      assert {:ok, %Post{title: "Updated"}} =
               Actions.find_and_update(Post, %{title: "Existing"}, %{title: "Updated"}, [])
    end

    test "returns {:error, error} when not found" do
      assert {:error, %{code: :not_found, message: "record not found."}} =
               Actions.find_and_update(Post, %{title: "Missing"}, %{title: "Updated"}, [])
    end
  end

  describe "find_and_upsert/3" do
    test "updates a record when found" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      assert {:ok, %Post{title: "Updated"}} =
               Actions.find_and_upsert(Post, %{title: "Existing"}, %{title: "Updated"})
    end

    test "creates a record when not found" do
      assert {:ok, %Post{title: "Upserted"}} =
               Actions.find_and_upsert(Post, %{title: "Missing"}, %{title: "Upserted"})
    end
  end

  describe "find_and_upsert/4" do
    test "updates a record when found" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      assert {:ok, %Post{title: "Updated"}} =
               Actions.find_and_upsert(Post, %{title: "Existing"}, %{title: "Updated"}, [])
    end

    test "creates a record when not found" do
      assert {:ok, %Post{title: "Upserted"}} =
               Actions.find_and_upsert(Post, %{title: "Missing"}, %{title: "Upserted"}, [])
    end
  end

  describe "find_and_delete/3" do
    test "deletes a record when found" do
      %Post{}
      |> Post.changeset(%{title: "ToDelete"})
      |> Repo.insert!()

      assert {:ok, %Post{title: "ToDelete"}} = Actions.find_and_delete(Post, %{title: "ToDelete"})
    end

    test "returns {:error, error} when not found" do
      assert {:error, %{code: :not_found, message: "record not found."}} =
               Actions.find_and_delete(Post, %{title: "Missing"})
    end
  end

  describe "find_or_create/3" do
    test "returns {:ok, record} when found" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      assert {:ok, %Post{title: "Existing"}} = Actions.find_or_create(Post, %{title: "Existing"})
    end

    test "supports query_fields option" do
      %Post{}
      |> Post.changeset(%{title: "Existing"})
      |> Repo.insert!()

      assert {:ok, %Post{title: "Existing"}} =
               Actions.find_or_create(
                 Post,
                 %{title: "Existing"},
                 query_fields: [:title]
               )
    end

    test "creates a record when not found" do
      assert {:ok, %Post{title: "Created"}} =
               Actions.find_or_create(
                 Post,
                 %{title: "Created"}
               )
    end
  end

  describe "insert_all/3" do
    test "inserts multiple records successfully" do
      assert {:ok, {2, nil}} =
               Actions.insert_all(
                 Post,
                 [%{title: "First"}, %{title: "Second"}],
                 []
               )

      assert [%Post{title: "First"}, %Post{title: "Second"}] = Repo.all(Post)
    end

    test "inserts multiple records successfully and returns records when option :returning is true" do
      assert {:ok, {2, [%Post{title: "First"}, %Post{title: "Second"}]}} =
               Actions.insert_all(
                 Post,
                 [%{title: "First"}, %{title: "Second"}],
                 returning: true
               )
    end

    test "returns {:error, [changeset]} when validation fails on any record" do
      assert {:error, [changeset]} = Actions.insert_all(Post, [%{views: "oops"}])

      assert %Ecto.Changeset{valid?: false} = changeset
      assert Keyword.has_key?(changeset.errors, :views)
    end

    test "performs upsert when records include complete primary keys" do
      # Setup: create an existing post
      existing_post =
        %Post{}
        |> Post.changeset(%{title: "Original Title"})
        |> Repo.insert!()

      # Action: insert_all with the same ID but different title
      # This should update the existing record because we provide the ID (complete PK)
      # and on_conflict_replace: [:title] tells it to replace the title on conflict
      assert {:ok, {1, nil}} =
               Actions.insert_all(
                 Post,
                 [%{id: existing_post.id, title: "Updated Title"}],
                 on_conflict_replace: [:title]
               )

      # Verify: the record was updated, not duplicated
      assert %Post{title: "Updated Title"} = Repo.get!(Post, existing_post.id)
    end

    test "generates conflict options by default so duplicate primary keys do not raise" do
      existing_post =
        %Post{}
        |> Post.changeset(%{title: "Original"})
        |> Repo.insert!()

      exception =
        try do
          Repo.insert_all(Post, [%{id: existing_post.id, title: "Will Raise"}], [])
          nil
        rescue
          e in Postgrex.Error ->
            e
        end

      assert %Postgrex.Error{} = exception

      assert {:ok, {1, nil}} =
               Actions.insert_all(
                 Post,
                 [%{id: existing_post.id, title: "Updated"}],
                 []
               )

      assert %Post{title: "Updated"} = Repo.get!(Post, existing_post.id)
    end

    test "allows caller-provided on_conflict option to override computed defaults" do
      # Setup: create an existing post
      existing_post =
        %Post{}
        |> Post.changeset(%{title: "Original"})
        |> Repo.insert!()

      # Action: insert_all with on_conflict: :nothing
      # Even though on_conflict_replace: [:title] is set, the explicit on_conflict: :nothing
      # in opts takes precedence (via Keyword.merge), so the insert is ignored
      assert {:ok, {0, nil}} =
               Actions.insert_all(
                 Post,
                 [%{id: existing_post.id, title: "Attempted Update"}],
                 on_conflict_replace: [:title],
                 on_conflict: :nothing
               )

      # Verify: the record was NOT updated because on_conflict: :nothing prevents it
      assert %Post{title: "Original"} = Repo.get!(Post, existing_post.id)
    end

    test "with validate: false, inserts records that would fail validation" do
      # The Comment schema has validate_length(:body, min: 3).
      # With validate: false, we can insert a body shorter than 3 characters,
      # bypassing that validation entirely.
      assert {:ok, {2, nil}} =
               Actions.insert_all(
                 Comment,
                 [
                   %{body: "x"},
                   %{body: "y"}
                 ],
                 validate: false
               )

      # Verify: both records were inserted despite failing the min length validation
      assert [%Comment{body: "x"}, %Comment{body: "y"}] = Repo.all(Comment)
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

  describe "delete_all/3" do
    test "deletes matching records and returns {count, nil}" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      assert {2, nil} = Actions.delete_all(Post, %{title: "A"})
      assert [%Post{title: "B"}] = Repo.all(Post)
    end

    test "returns {0, nil} when nothing matches" do
      %Post{}
      |> Post.changeset(%{title: "Only"})
      |> Repo.insert!()

      assert {0, nil} = Actions.delete_all(Post, %{title: "Missing"})
    end
  end

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

    test "creates a post and can find it by published field" do
      %Post{}
      |> Post.changeset(%{title: "Published", published: true})
      |> Repo.insert!()

      _unpublished_post =
        %Post{}
        |> Post.changeset(%{title: "Unpublished", published: false})
        |> Repo.insert!()

      assert [%Post{title: "Published", published: true}] = Actions.all(Post, %{published: true})
    end

    test "supports :select true (selects the binding)" do
      %Post{}
      |> Post.changeset(%{title: "Selected", published: true})
      |> Repo.insert!()

      assert [%Post{title: "Selected"}] = Actions.all(Post, %{select: true, title: "Selected"})
    end

    test "supports :select for a single field" do
      post =
        %Post{}
        |> Post.changeset(%{title: "SelectId", published: true})
        |> Repo.insert!()

      assert [id] = Actions.all(Post, %{select: :id, id: post.id})
      assert id === post.id
    end

    test "supports :select {:map, map} for custom field aliases" do
      post =
        %Post{}
        |> Post.changeset(%{title: "SelectAlias", published: true})
        |> Repo.insert!()

      assert [%{custom_id: id}] =
               Actions.all(Post, %{
                 select: %{map: %{custom_id: :id}},
                 id: post.id
               })

      assert id === post.id
    end

    test "supports :select {:map, fields} (Ecto map/2)" do
      post =
        %Post{}
        |> Post.changeset(%{title: "SelectMap", published: true})
        |> Repo.insert!()

      assert [%{id: id, title: title}] =
               Actions.all(Post, %{
                 select: %{map: [:id, :title]},
                 id: post.id
               })

      assert id === post.id
      assert title === "SelectMap"
    end

    test "supports :select {:struct, fields} (Ecto struct/2)" do
      post =
        %Post{}
        |> Post.changeset(%{title: "SelectStruct", published: true})
        |> Repo.insert!()

      assert [%Post{id: id}] =
               Actions.all(Post, %{
                 select: %{struct: [:id]},
                 id: post.id
               })

      assert id === post.id
    end

    test "supports :or_where filter key" do
      %Post{}
      |> Post.changeset(%{title: "Published", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Unpublished", published: false})
      |> Repo.insert!()

      assert [%Post{title: "Published"}, %Post{title: "Unpublished"}] =
               Actions.all(Post, %{
                 published: true,
                 or_where: %{published: false}
               })
    end

    test "supports :limit" do
      %Post{}
      |> Post.changeset(%{title: "One"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Two"})
      |> Repo.insert!()

      assert [%Post{title: "One"}] =
               Actions.all(Post, %{
                 order_by: %{asc: :id},
                 limit: 1
               })
    end

    test "supports :first (alias for limit)" do
      %Post{}
      |> Post.changeset(%{title: "One"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Two"})
      |> Repo.insert!()

      assert [%Post{title: "One"}] =
               Actions.all(Post, %{
                 order_by: %{asc: :id},
                 first: 1
               })
    end

    test "supports :offset" do
      %Post{}
      |> Post.changeset(%{title: "One"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Two"})
      |> Repo.insert!()

      assert [%Post{title: "Two"}] =
               Actions.all(Post, %{
                 order_by: %{asc: :id},
                 offset: 1
               })
    end

    test "supports :preload" do
      author =
        %User{}
        |> User.changeset(%{first_name: "Preload"})
        |> Repo.insert!()

      post =
        %Post{}
        |> Post.changeset(%{title: "WithAuthor", author_id: author.id})
        |> Repo.insert!()

      assert [%Post{title: "WithAuthor"} = result] =
               Actions.all(Post, %{
                 id: post.id,
                 preload: [:author]
               })

      assert %User{first_name: "Preload"} = result.author
    end

    test "supports nested :preload" do
      author =
        %User{}
        |> User.changeset(%{first_name: "Nested"})
        |> Repo.insert!()

      post =
        %Post{}
        |> Post.changeset(%{title: "WithComments", author_id: author.id})
        |> Repo.insert!()

      %Comment{}
      |> Comment.changeset(%{body: "A comment", post_id: post.id, author_id: author.id})
      |> Repo.insert!()

      assert [%Post{title: "WithComments"} = result] =
               Actions.all(Post, %{
                 id: post.id,
                 preload: [comments: :author]
               })

      assert [%Comment{body: "A comment"} = comment] = result.comments
      assert %User{first_name: "Nested"} = comment.author
    end

    test "supports :last" do
      %Post{}
      |> Post.changeset(%{title: "One"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Two"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Three"})
      |> Repo.insert!()

      results = Actions.all(Post, %{last: 2})

      assert Enum.count(results) === 2
      assert Enum.map(results, & &1.title) === ["Two", "Three"]
    end

    test "supports :last with key" do
      %Post{}
      |> Post.changeset(%{title: "One"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Two"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Three"})
      |> Repo.insert!()

      results = Actions.all(Post, %{last: %{id: 2}})

      assert Enum.count(results) === 2
      assert Enum.map(results, & &1.title) === ["Two", "Three"]
    end

    test "supports positional binding selector via :bind/:at" do
      author =
        %User{}
        |> User.changeset(%{first_name: "author"})
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{
        title: "Published",
        published: true,
        author_id: author.id
      })
      |> Repo.insert!()

      _unpublished_post =
        %Post{}
        |> Post.changeset(%{
          title: "Unpublished",
          published: false,
          author_id: author.id
        })
        |> Repo.insert!()

      q =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      assert [result] = Actions.all(q, %{bind: %{at: %{1 => %{published: true}}}})
      assert %Post{title: "Published", published: true} = result
    end

    test "supports named binding selector via :bind/:as" do
      %Post{}
      |> Post.changeset(%{title: "Published", published: true})
      |> Repo.insert!()

      _unpublished_post =
        %Post{}
        |> Post.changeset(%{title: "Unpublished", published: false})
        |> Repo.insert!()

      q = from(p in Post, as: :post)

      assert [result] = Actions.all(q, %{bind: %{as: %{post: %{published: true}}}})
      assert %Post{title: "Published", published: true} = result
    end

    test "supports explicit operator" do
      %Post{}
      |> Post.changeset(%{title: "True", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "False", published: false})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{published: %{!=: true}})
      assert %Post{title: "False", published: false} = result
    end

    test "supports negated explicit operator" do
      %Post{}
      |> Post.changeset(%{title: "True", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "False", published: false})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{published: %{not: %{==: true}}})
      assert %Post{title: "False", published: false} = result
    end

    test "supports negated explicit operator (not !=)" do
      %Post{}
      |> Post.changeset(%{title: "True", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "False", published: false})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{published: %{not: %{!=: true}}})
      assert %Post{title: "True", published: true} = result
    end

    test "supports explicit IN operator for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "True", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "False", published: false})
      |> Repo.insert!()

      assert [%Post{title: "True", published: true}] =
               Actions.all(Post, %{published: %{in: [true]}})
    end

    test "supports explicit NOT IN operator for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "True", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "False", published: false})
      |> Repo.insert!()

      assert [%Post{title: "False", published: false}] =
               Actions.all(Post, %{published: %{not: %{in: [true]}}})
    end

    test "coerces not === with list RHS to NOT IN for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "True", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "False", published: false})
      |> Repo.insert!()

      assert [%Post{title: "False", published: false}] =
               Actions.all(Post, %{published: %{not: %{==: [true]}}})
    end

    test "coerces not !== with list RHS to IN for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "True", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "False", published: false})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{published: %{not: %{!=: [true]}}})
      assert %Post{title: "True", published: true} = result
    end

    test "supports >= comparison for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "Low", views: 9})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "High", views: 10})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{views: %{>=: 10}})
      assert %Post{title: "High", views: 10} = result
    end

    test "supports < comparison for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "Low", views: 9})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "High", views: 10})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{views: %{<: 10}})
      assert %Post{title: "Low", views: 9} = result
    end

    test "supports <= comparison for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "Low", views: 10})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "High", views: 11})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{views: %{<=: 10}})
      assert %Post{title: "Low", views: 10} = result
    end

    test "supports negated > comparison for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "Low", views: 5})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "High", views: 15})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{views: %{not: %{>: 10}}})
      assert %Post{title: "Low", views: 5} = result
    end

    test "supports LOWER operator for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "Hello"})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "Other"})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{title: %{lower: "hello"}})
      assert %Post{title: "Hello"} = result
    end

    test "supports UPPER operator for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "Hello"})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "Other"})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{title: %{upper: "HELLO"}})
      assert %Post{title: "Hello"} = result
    end

    test "supports negated LOWER operator for scalar fields" do
      _excluded =
        %Post{}
        |> Post.changeset(%{title: "Hello"})
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Other"})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{title: %{not: %{lower: "hello"}}})
      assert %Post{title: "Other"} = result
    end

    test "supports negated UPPER operator for scalar fields" do
      _excluded =
        %Post{}
        |> Post.changeset(%{title: "Hello"})
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Other"})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{title: %{not: %{upper: "HELLO"}}})
      assert %Post{title: "Other"} = result
    end

    test "supports LIKE operator for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "Hello world"})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "Goodbye"})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{title: %{like: "Hello"}})
      assert %Post{title: "Hello world"} = result
    end

    test "supports LIKE operator for scalar fields with list RHS (LIKE ANY)" do
      %Post{}
      |> Post.changeset(%{title: "Hello"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "World"})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "Other"})
        |> Repo.insert!()

      results = Actions.all(Post, %{title: %{like: ["Hello", "World"]}, order_by: [asc: :title]})

      assert Enum.count(results) === 2
      assert Enum.any?(results, &match?(%Post{title: "Hello"}, &1))
      assert Enum.any?(results, &match?(%Post{title: "World"}, &1))
    end

    test "supports negated LIKE operator for scalar fields" do
      _match =
        %Post{}
        |> Post.changeset(%{title: "Hello"})
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Other"})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{title: %{not: %{like: "Hello"}}})
      assert %Post{title: "Other"} = result
    end

    test "supports LOWER operator for array fields" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["Elixir"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["ruby"]})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{lower: "elixir"}})
      assert %Post{title: "Match"} = result
    end

    test "supports UPPER operator for array fields" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["Elixir"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["ruby"]})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{upper: "ELIXIR"}})
      assert %Post{title: "Match"} = result
    end

    test "supports negated LOWER operator for array fields" do
      _excluded =
        %Post{}
        |> Post.changeset(%{title: "Excluded", tags: ["Elixir"]})
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Kept", tags: ["ruby"]})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{not: %{lower: "elixir"}}})
      assert %Post{title: "Kept"} = result
    end

    test "supports negated UPPER operator for array fields" do
      _excluded =
        %Post{}
        |> Post.changeset(%{title: "Excluded", tags: ["Elixir"]})
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Kept", tags: ["ruby"]})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{not: %{upper: "ELIXIR"}}})
      assert %Post{title: "Kept"} = result
    end

    test "supports negated ILIKE operator for scalar fields with list RHS (NOT ILIKE ANY)" do
      _excluded =
        %Post{}
        |> Post.changeset(%{title: "HELLO"})
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Other"})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{title: %{not: %{ilike: ["hello", "world"]}}})
      assert %Post{title: "Other"} = result
    end

    test "array field supports membership via :in with scalar RHS" do
      %Post{}
      |> Post.changeset(%{
        title: "Match",
        tags: ["elixir", "erlang"]
      })
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{
          title: "NoMatch",
          tags: ["ruby"]
        })
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{in: "elixir"}})
      assert %Post{title: "Match"} = result
    end

    test "supports LIKE operator for array fields" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["elixir"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["ruby"]})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{like: "elixir"}})
      assert %Post{title: "Match"} = result
    end

    test "supports ILIKE operator for array fields" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["Elixir"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["ruby"]})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{ilike: "elixir"}})
      assert %Post{title: "Match"} = result
    end

    test "supports LIKE operator for array fields with list RHS" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["erlang"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{
          title: "NoMatch",
          tags: ["ruby"]
        })
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{like: ["elixir", "erlang"]}})
      assert %Post{title: "Match"} = result
    end

    test "supports negated LIKE operator for array fields" do
      _excluded =
        %Post{}
        |> Post.changeset(%{
          title: "Excluded",
          tags: ["elixir"]
        })
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Kept", tags: ["ruby"]})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{not: %{like: "elixir"}}})
      assert %Post{title: "Kept"} = result
    end

    test "supports negated ILIKE operator for array fields" do
      _excluded =
        %Post{}
        |> Post.changeset(%{
          title: "Excluded",
          tags: ["Elixir"]
        })
        |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Kept", tags: ["ruby"]})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{not: %{ilike: "elixir"}}})
      assert %Post{title: "Kept"} = result
    end

    test "array field compares equality when RHS is a list and operator defaults to ==" do
      %Post{}
      |> Post.changeset(%{
        title: "Match",
        tags: ["elixir", "erlang"]
      })
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{
          title: "NoMatch",
          tags: ["elixir"]
        })
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: ["elixir", "erlang"]})
      assert %Post{title: "Match", tags: ["elixir", "erlang"]} = result
    end

    test "array field supports negated equality when RHS is a list" do
      %Post{}
      |> Post.changeset(%{
        title: "Match",
        tags: ["elixir", "erlang"]
      })
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{
        title: "NoMatch",
        tags: ["elixir"]
      })
      |> Repo.insert!()

      results = Actions.all(Post, %{tags: %{not: %{==: ["elixir"]}}})
      assert Enum.count(results) === 1
      assert Enum.any?(results, &match?(%Post{title: "Match"}, &1))
    end

    test "array field supports > comparison against scalar (any element matches)" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["b"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["a"]})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{>: "a"}})
      assert %Post{title: "Match"} = result
    end

    test "array field supports >= comparison against scalar (any element matches)" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["b"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["a"]})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{>=: "b"}})
      assert %Post{title: "Match"} = result
    end

    test "array field supports < comparison against scalar (any element matches)" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["a"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["b"]})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{<: "b"}})
      assert %Post{title: "Match"} = result
    end

    test "array field supports <= comparison against scalar (any element matches)" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["a"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["b"]})
        |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{<=: "a"}})
      assert %Post{title: "Match"} = result
    end

    test "array field supports negated > comparison against scalar" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["a"]})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{
        title: "NoMatch",
        tags: ["b"]
      })
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{tags: %{not: %{>: "a"}}})
      assert %Post{title: "Match"} = result
    end

    test "supports keyword-list params" do
      %Post{}
      |> Post.changeset(%{title: "Published", published: true})
      |> Repo.insert!()

      _unpublished_post =
        %Post{}
        |> Post.changeset(%{title: "Unpublished", published: false})
        |> Repo.insert!()

      assert [%Post{title: "Published", published: true}] = Actions.all(Post, published: true)
    end

    test "treats non-keyword lists as values (defaults operator to ==)" do
      %Post{}
      |> Post.changeset(%{title: "True", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "False", published: false})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{published: [true]})
      assert %Post{title: "True", published: true} = result
    end

    test "supports multiple conditions under a single filter" do
      %Post{}
      |> Post.changeset(%{title: "True", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "False", published: false})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{where: %{published: [==: true, !=: false]}})
      assert %Post{title: "True", published: true} = result
    end

    test "supports boolean :and operator for multiple comparisons on same field" do
      %Post{}
      |> Post.changeset(%{title: "Match", views: 15})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "NoMatch", views: 25})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{views: %{and: [>: 10, <: 20]}})
      assert %Post{title: "Match", views: 15} = result
    end

    test "supports boolean :or operator for multiple comparisons on same field" do
      %Post{}
      |> Post.changeset(%{title: "Match", views: 3})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "NoMatch", views: 7})
      |> Repo.insert!()

      assert [result] = Actions.all(Post, %{views: %{or: [<: 5, >: 10]}})
      assert %Post{title: "Match", views: 3} = result
    end

    test "boolean operator group under :or_where" do
      %Post{}
      |> Post.changeset(%{
        title: "Published",
        published: true,
        views: 0
      })
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{
        title: "Unpublished",
        published: false,
        views: 3
      })
      |> Repo.insert!()

      results =
        Actions.all(Post, %{
          published: true,
          or_where: %{views: %{or: [<: 5, >: 10]}}
        })

      assert Enum.count(results) === 2
      assert Enum.any?(results, &match?(%Post{title: "Published"}, &1))
      assert Enum.any?(results, &match?(%Post{title: "Unpublished"}, &1))
    end

    #   test "can join association and filter on association field" do
    #     author =
    #       %User{}
    #       |> User.changeset(%{first_name: "John"})
    #       |> Repo.insert!()

    #     other_author =
    #       %User{}
    #       |> User.changeset(%{first_name: "Jane"})
    #       |> Repo.insert!()

    #     %Post{}
    #     |> Post.changeset(%{title: "Authored", permalink: "authored-join", author_id: author.id})
    #     |> Repo.insert!()

    #     _other_post =
    #       %Post{}
    #       |> Post.changeset(%{title: "Other", permalink: "other-join", author_id: other_author.id})
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{author: %{first_name: "John"}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Authored"} = result
    #     assert result.author_id === author.id
    #   end

    #   test "can join association with :as and filter on association field" do
    #     author =
    #       %User{}
    #       |> User.changeset(%{first_name: "John"})
    #       |> Repo.insert!()

    #     other_author =
    #       %User{}
    #       |> User.changeset(%{first_name: "Jane"})
    #       |> Repo.insert!()

    #     %Post{}
    #     |> Post.changeset(%{title: "Authored", permalink: "authored-join-as", author_id: author.id})
    #     |> Repo.insert!()

    #     _other_post =
    #       %Post{}
    #       |> Post.changeset(%{
    #         title: "Other",
    #         permalink: "other-join-as",
    #         author_id: other_author.id
    #       })
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{author: %{as: :author_join, first_name: "John"}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Authored"} = result
    #     assert result.author_id === author.id
    #   end

    #   test "can recursively filter through nested associations" do
    #     john =
    #       %User{}
    #       |> User.changeset(%{first_name: "John"})
    #       |> Repo.insert!()

    #     jane =
    #       %User{}
    #       |> User.changeset(%{first_name: "Jane"})
    #       |> Repo.insert!()

    #     post_with_jane_comment =
    #       %Post{}
    #       |> Post.changeset(%{title: "HasJaneComment", permalink: "has-jane-comment"})
    #       |> Repo.insert!()

    #     post_with_john_comment =
    #       %Post{}
    #       |> Post.changeset(%{title: "HasJohnComment", permalink: "has-john-comment"})
    #       |> Repo.insert!()

    #     %Comment{}
    #     |> Comment.changeset(%{post_id: post_with_jane_comment.id, author_id: jane.id, body: "Nice"})
    #     |> Repo.insert!()

    #     %Comment{}
    #     |> Comment.changeset(%{post_id: post_with_john_comment.id, author_id: john.id, body: "Nice"})
    #     |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{comments: %{author: %{first_name: "Jane"}}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "HasJaneComment"} = result
    #   end

    #   test "creates associated records and can filter on association field" do
    #     author =
    #       %User{}
    #       |> User.changeset(%{first_name: "John"})
    #       |> Repo.insert!()

    #     other_author =
    #       %User{}
    #       |> User.changeset(%{first_name: "Jane"})
    #       |> Repo.insert!()

    #     %Post{}
    #     |> Post.changeset(%{title: "Authored", permalink: "authored", author_id: author.id})
    #     |> Repo.insert!()

    #     _other_post =
    #       %Post{}
    #       |> Post.changeset(%{title: "Other", permalink: "other", author_id: other_author.id})
    #       |> Repo.insert!()

    #     q =
    #       from(p in Post,
    #         as: :post,
    #         join: a in assoc(p, :author),
    #         as: :author,
    #         order_by: [asc: p.id]
    #       )

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{as: %{author: %{first_name: "John"}}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Authored"} = result
    #     assert result.author_id === author.id
    #   end

    #   test "scalar field: !== with list RHS behaves like NOT IN" do
    #     _a =
    #       %Post{}
    #       |> Post.changeset(%{title: "A", views: 10})
    #       |> Repo.insert!()

    #     _b =
    #       %Post{}
    #       |> Post.changeset(%{title: "B", views: 20})
    #       |> Repo.insert!()

    #     %Post{}
    #     |> Post.changeset(%{title: "C", views: 30})
    #     |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{views: %{!=: [10, 20]}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "C", views: 30} = result
    #   end

    #   test "array field: scalar RHS defaults to membership" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Match", tags: ["elixir", "erlang"]})
    #     |> Repo.insert!()

    #     _no_match =
    #       %Post{}
    #       |> Post.changeset(%{title: "NoMatch", tags: ["ruby"]})
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{tags: "elixir"},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Match", tags: ["elixir", "erlang"]} = result
    #   end

    #   test "array field: list RHS with :in uses overlap-any semantics" do
    #     %Post{}
    #     |> Post.changeset(%{
    #       title: "Match",
    #       tags: ["elixir", "erlang"]
    #     })
    #     |> Repo.insert!()

    #     _no_match =
    #       %Post{}
    #       |> Post.changeset(%{title: "NoMatch", tags: ["ruby"]})
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{tags: %{in: ["elixir"]}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Match"} = result
    #   end

    #   test "array field: in all uses contains-all semantics" do
    #     %Post{}
    #     |> Post.changeset(%{
    #       title: "Match",
    #       tags: ["elixir", "erlang"]
    #     })
    #     |> Repo.insert!()

    #     _no_match =
    #       %Post{}
    #       |> Post.changeset(%{
    #         title: "NoMatch",
    #         tags: ["elixir"]
    #       })
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{tags: %{in: %{all: ["elixir", "erlang"]}}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Match"} = result
    #   end

    #   test "scalar field: supports > comparison" do
    #     _low =
    #       %Post{}
    #       |> Post.changeset(%{title: "Low", views: 5})
    #       |> Repo.insert!()

    #     %Post{}
    #     |> Post.changeset(%{title: "High", views: 15})
    #     |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{views: %{>: 10}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "High", views: 15} = result
    #   end

    #   test "array field: negated overlap via not in" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Match", tags: ["ruby"]})
    #     |> Repo.insert!()

    #     _no_match =
    #       %Post{}
    #       |> Post.changeset(%{
    #         title: "NoMatch",
    #         tags: ["elixir"]
    #       })
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{tags: %{not: %{in: ["elixir"]}}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Match", tags: ["ruby"]} = result
    #   end

    #   test "scalar field: === nil generates IS NULL semantics" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Nil", permalink: "scalar-nil", published_at: nil})
    #     |> Repo.insert!()

    #     _not_nil =
    #       %Post{}
    #       |> Post.changeset(%{
    #         title: "NotNil",
    #         permalink: "scalar-not-nil",
    #         published_at: DateTime.utc_now()
    #       })
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{published_at: nil},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Nil", published_at: nil} = result
    #   end

    #   test "scalar field: !== nil generates IS NOT NULL semantics" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Nil", permalink: "scalar-ne-nil-nil", published_at: nil})
    #     |> Repo.insert!()

    #     %Post{}
    #     |> Post.changeset(%{
    #       title: "NotNil",
    #       permalink: "scalar-ne-nil-not-nil",
    #       published_at: DateTime.utc_now()
    #     })
    #     |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{published_at: %{!=: nil}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "NotNil"} = result
    #     assert result.published_at !== nil
    #   end

    #   test "scalar field: === with list RHS behaves like IN" do
    #     %Post{}
    #     |> Post.changeset(%{title: "A", permalink: "scalar-eq-list-a", views: 10})
    #     |> Repo.insert!()

    #     _b =
    #       %Post{}
    #       |> Post.changeset(%{title: "B", permalink: "scalar-eq-list-b", views: 20})
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{views: %{==: [10]}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "A", views: 10} = result
    #   end

    #   test "scalar field: supports negated < comparison" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Low", permalink: "scalar-not-lt-low", views: 5})
    #     |> Repo.insert!()

    #     %Post{}
    #     |> Post.changeset(%{title: "High", permalink: "scalar-not-lt-high", views: 15})
    #     |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{views: %{not: %{<: 10}}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "High", views: 15} = result
    #   end

    #   test "supports :ids (scalar special filter)" do
    #     a =
    #       %Post{}
    #       |> Post.changeset(%{title: "A", permalink: "ids-a"})
    #       |> Repo.insert!()

    #     b =
    #       %Post{}
    #       |> Post.changeset(%{title: "B", permalink: "ids-b"})
    #       |> Repo.insert!()

    #     _c =
    #       %Post{}
    #       |> Post.changeset(%{title: "C", permalink: "ids-c"})
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{id: [a.id, b.id]},
    #         []
    #       )

    #     assert ["A", "B"] = q |> Repo.all() |> Enum.map(& &1.title) |> Enum.sort()
    #   end

    #   test "supports :before (scalar special filter)" do
    #     a =
    #       %Post{}
    #       |> Post.changeset(%{title: "A", permalink: "before-a"})
    #       |> Repo.insert!()

    #     b =
    #       %Post{}
    #       |> Post.changeset(%{title: "B", permalink: "before-b"})
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{before: b.id},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "A"} = result
    #     assert result.id === a.id
    #   end

    #   test "supports :after (scalar special filter)" do
    #     a =
    #       %Post{}
    #       |> Post.changeset(%{title: "A", permalink: "after-a"})
    #       |> Repo.insert!()

    #     b =
    #       %Post{}
    #       |> Post.changeset(%{title: "B", permalink: "after-b"})
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{after: a.id},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "B"} = result
    #     assert result.id === b.id
    #   end

    #   test "array field: negated !== with list RHS behaves like ==" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Match", tags: ["elixir"]})
    #     |> Repo.insert!()

    #     _no_match =
    #       %Post{}
    #       |> Post.changeset(%{
    #         title: "NoMatch",
    #         tags: ["ruby"]
    #       })
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{tags: %{not: %{!=: ["elixir"]}}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Match", tags: ["elixir"]} = result
    #   end

    #   test "array field: supports negated >= comparison against scalar" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Match", tags: ["a"]})
    #     |> Repo.insert!()

    #     _no_match =
    #       %Post{}
    #       |> Post.changeset(%{
    #         title: "NoMatch",
    #         tags: ["b"]
    #       })
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{tags: %{not: %{>=: "b"}}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Match"} = result
    #   end

    #   test "array field: supports negated < comparison against scalar" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Match", permalink: "array-not-lt-scalar-match", tags: ["b"]})
    #     |> Repo.insert!()

    #     _no_match =
    #       %Post{}
    #       |> Post.changeset(%{
    #         title: "NoMatch",
    #         permalink: "array-not-lt-scalar-no-match",
    #         tags: ["a"]
    #       })
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{tags: %{not: %{<: "b"}}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Match"} = result
    #   end

    #   test "array field: supports negated <= comparison against scalar" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Match", permalink: "array-not-lte-scalar-match", tags: ["b"]})
    #     |> Repo.insert!()

    #     _no_match =
    #       %Post{}
    #       |> Post.changeset(%{
    #         title: "NoMatch",
    #         permalink: "array-not-lte-scalar-no-match",
    #         tags: ["a"]
    #       })
    #       |> Repo.insert!()

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         %{tags: %{not: %{<=: "a"}}},
    #         []
    #       )

    #     assert [result] = Repo.all(q)
    #     assert %Post{title: "Match"} = result
    #   end

    #   test "select filter with nested params reduces over values" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Test", published: true})
    #     |> Repo.insert!()

    #     q = from(p in Post, as: :post)

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         q,
    #         %{as: %{post: %{select: %{struct: [:id, :title]}}}},
    #         []
    #       )

    #     assert [%Post{title: "Test"}] = Repo.all(q)
    #   end

    #   test "where filter with non-map scalar value calls query builder directly" do
    #     %Post{}
    #     |> Post.changeset(%{title: "Test", published: true})
    #     |> Repo.insert!()

    #     q = from(p in Post, as: :post)

    #     q =
    #       CommonFilters.convert_params_to_filter(
    #         Post,
    #         q,
    #         %{as: %{post: %{where: %{published: true}}}},
    #         []
    #       )

    #     assert [%Post{title: "Test"}] = Repo.all(q)
    #   end
  end
end
