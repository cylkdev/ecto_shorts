defmodule EctoShorts.Actions.CRUDTest do
  use EctoShorts.DataCase, async: true

  alias Ecto.Changeset
  alias EctoShorts.Actions
  alias EctoShorts.Schema.Comment
  alias EctoShorts.Schema.Post
  alias EctoShorts.Schema.PostWithLock
  alias EctoShorts.Schema.User

  import Ecto.Query

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

  describe "all/3 filters" do
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

      assert [result] = Actions.all(q, %{bind: %{at: 1, published: true}})
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

      assert [result] = Actions.all(q, %{bind: %{as: :post, published: true}})
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

    test "scalar field: == nil generates IS NULL" do
      %Post{}
      |> Post.changeset(%{title: "Nil", permalink: "scalar-nil", published_at: nil})
      |> Repo.insert!()

      _not_nil =
        %Post{}
        |> Post.changeset(%{
          title: "NotNil",
          permalink: "scalar-not-nil",
          published_at: DateTime.utc_now()
        })
        |> Repo.insert!()

      assert [%Post{title: "Nil", published_at: nil}] =
               Actions.all(Post, %{published_at: nil})
    end

    test "scalar field: != nil generates IS NOT NULL" do
      %Post{}
      |> Post.changeset(%{title: "Nil", permalink: "scalar-ne-nil", published_at: nil})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{
        title: "NotNil",
        permalink: "scalar-ne-not-nil",
        published_at: DateTime.utc_now()
      })
      |> Repo.insert!()

      assert [%Post{title: "NotNil"}] =
               Actions.all(Post, %{published_at: %{!=: nil}})
    end

    test "scalar field: != with list RHS behaves like NOT IN" do
      %Post{}
      |> Post.changeset(%{title: "A", views: 10})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "B", views: 20})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "C", views: 30})
      |> Repo.insert!()

      assert [%Post{title: "C", views: 30}] =
               Actions.all(Post, %{views: %{!=: [10, 20]}})
    end

    test "scalar field: == with list RHS behaves like IN" do
      %Post{}
      |> Post.changeset(%{title: "A", views: 10})
      |> Repo.insert!()

      _b =
        %Post{}
        |> Post.changeset(%{title: "B", views: 20})
        |> Repo.insert!()

      assert [%Post{title: "A", views: 10}] =
               Actions.all(Post, %{views: %{==: [10]}})
    end

    test "supports > comparison for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "Low", views: 5})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "High", views: 15})
      |> Repo.insert!()

      assert [%Post{title: "High", views: 15}] =
               Actions.all(Post, %{views: %{>: 10}})
    end

    test "supports negated < comparison for scalar fields" do
      %Post{}
      |> Post.changeset(%{title: "Low", views: 5})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "High", views: 15})
      |> Repo.insert!()

      assert [%Post{title: "High", views: 15}] =
               Actions.all(Post, %{views: %{not: %{<: 10}}})
    end

    test "supports :before custom filter" do
      post_a =
        %Post{}
        |> Post.changeset(%{title: "A"})
        |> Repo.insert!()

      post_b =
        %Post{}
        |> Post.changeset(%{title: "B"})
        |> Repo.insert!()

      assert [%Post{title: "A"}] = Actions.all(Post, %{before: post_b.id})
      assert post_a.id < post_b.id
    end

    test "supports :after custom filter" do
      post_a =
        %Post{}
        |> Post.changeset(%{title: "A"})
        |> Repo.insert!()

      post_b =
        %Post{}
        |> Post.changeset(%{title: "B"})
        |> Repo.insert!()

      assert [%Post{title: "B"}] = Actions.all(Post, %{after: post_a.id})
      assert post_b.id > post_a.id
    end

    test "array field: negated != with list RHS behaves like ==" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["elixir"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["ruby"]})
        |> Repo.insert!()

      assert [%Post{title: "Match", tags: ["elixir"]}] =
               Actions.all(Post, %{tags: %{not: %{!=: ["elixir"]}}})
    end

    test "array field supports negated >= comparison against scalar" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["a"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["b"]})
        |> Repo.insert!()

      assert [%Post{title: "Match"}] =
               Actions.all(Post, %{tags: %{not: %{>=: "b"}}})
    end

    test "array field supports negated < comparison against scalar" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["b"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["a"]})
        |> Repo.insert!()

      assert [%Post{title: "Match"}] =
               Actions.all(Post, %{tags: %{not: %{<: "b"}}})
    end

    test "array field supports negated <= comparison against scalar" do
      %Post{}
      |> Post.changeset(%{title: "Match", tags: ["b"]})
      |> Repo.insert!()

      _no_match =
        %Post{}
        |> Post.changeset(%{title: "NoMatch", tags: ["a"]})
        |> Repo.insert!()

      assert [%Post{title: "Match"}] =
               Actions.all(Post, %{tags: %{not: %{<=: "a"}}})
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

  describe "delete" do
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

    test "supports max_rows option for custom chunk size" do
      for i <- 1..5 do
        %Post{}
        |> Post.changeset(%{title: "Post #{i}"})
        |> Repo.insert!()
      end

      assert {:ok, posts} =
               Repo.transaction(fn ->
                 Post
                 |> Actions.stream(%{order_by: %{asc: :id}}, max_rows: 2)
                 |> Enum.to_list()
               end)

      assert length(posts) === 5
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

  describe "preload/3" do
    test "preloads an association on a struct" do
      author =
        %User{}
        |> User.changeset(%{first_name: "Preloader"})
        |> Repo.insert!()

      post =
        %Post{}
        |> Post.changeset(%{title: "WithAuthor", author_id: author.id})
        |> Repo.insert!()

      post = Repo.get!(Post, post.id)

      assert %Ecto.Association.NotLoaded{} = post.author

      result = Actions.preload(post, :author)

      assert %Post{title: "WithAuthor"} = result
      assert %User{first_name: "Preloader"} = result.author
    end
  end

  describe "all/1" do
    test "returns all records for the given schema" do
      %Post{}
      |> Post.changeset(%{title: "A"})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "B"})
      |> Repo.insert!()

      results = Actions.all(Post)

      assert length(results) === 2
    end
  end

  describe "all/2 with keyword opts" do
    test "accepts keyword list with filter params" do
      %Post{}
      |> Post.changeset(%{title: "Published", published: true})
      |> Repo.insert!()

      %Post{}
      |> Post.changeset(%{title: "Unpublished", published: false})
      |> Repo.insert!()

      assert [%Post{title: "Published", published: true}] =
               Actions.all(Post, published: true)
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

  describe "find_and_update/4" do
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

  describe "find_and_upsert/4" do
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

  describe "update/4 optimistic locking via schema callback" do
    test "auto-detects locking from schema callback and succeeds on fresh record" do
      post =
        %PostWithLock{}
        |> PostWithLock.changeset(%{title: "Original"})
        |> Repo.insert!()

      assert post.lock_version === 1

      assert {:ok, %PostWithLock{title: "Updated", lock_version: 2}} =
               Actions.update(PostWithLock, post, %{title: "Updated"})
    end

    test "returns {:error, %{code: :stale}} on stale record" do
      post =
        %PostWithLock{}
        |> PostWithLock.changeset(%{title: "Original"})
        |> Repo.insert!()

      PostWithLock
      |> where([p], p.id == ^post.id)
      |> Repo.update_all(set: [lock_version: 99])

      assert {:error, %{code: :stale, message: "record has been modified by another process."}} =
               Actions.update(PostWithLock, post, %{title: "Too Late"})
    end

    test "increments lock_version on each successful update" do
      post =
        %PostWithLock{}
        |> PostWithLock.changeset(%{title: "V1"})
        |> Repo.insert!()

      assert {:ok, %PostWithLock{lock_version: 2} = post} =
               Actions.update(PostWithLock, post, %{title: "V2"})

      assert {:ok, %PostWithLock{lock_version: 3}} =
               Actions.update(PostWithLock, post, %{title: "V3"})
    end
  end

  describe "update/4 optimistic locking via option" do
    test "explicit optimistic_lock option applies locking" do
      post =
        %PostWithLock{}
        |> PostWithLock.changeset(%{title: "Original"})
        |> Repo.insert!()

      assert {:ok, %PostWithLock{title: "Updated", lock_version: 2}} =
               Actions.update(PostWithLock, post, %{title: "Updated"}, optimistic_lock: :lock_version)
    end

    test "explicit optimistic_lock option detects stale record" do
      post =
        %PostWithLock{}
        |> PostWithLock.changeset(%{title: "Original"})
        |> Repo.insert!()

      PostWithLock
      |> where([p], p.id == ^post.id)
      |> Repo.update_all(set: [lock_version: 99])

      assert {:error, %{code: :stale}} =
               Actions.update(PostWithLock, post, %{title: "Too Late"}, optimistic_lock: :lock_version)
    end

    test "optimistic_lock: false disables auto-detection from schema callback" do
      post =
        %PostWithLock{}
        |> PostWithLock.changeset(%{title: "Original"})
        |> Repo.insert!()

      PostWithLock
      |> where([p], p.id == ^post.id)
      |> Repo.update_all(set: [lock_version: 99])

      assert {:ok, %PostWithLock{title: "Updated"}} =
               Actions.update(PostWithLock, post, %{title: "Updated"}, optimistic_lock: false)
    end

    test "supports {field, incrementer} tuple option" do
      post =
        %PostWithLock{}
        |> PostWithLock.changeset(%{title: "Original"})
        |> Repo.insert!()

      assert {:ok, %PostWithLock{title: "Updated", lock_version: 11}} =
               Actions.update(PostWithLock, post, %{title: "Updated"},
                 optimistic_lock: {:lock_version, fn _ -> 11 end}
               )
    end
  end

  describe "update/4 optimistic locking on schema without callback" do
    test "no locking when schema has no callback and no option" do
      post =
        %Post{}
        |> Post.changeset(%{title: "Original"})
        |> Repo.insert!()

      assert {:ok, %Post{title: "Updated"}} =
               Actions.update(Post, post, %{title: "Updated"})
    end
  end

  describe "find_and_update/4 optimistic locking" do
    test "inherits locking from schema callback" do
      post =
        %PostWithLock{}
        |> PostWithLock.changeset(%{title: "Original"})
        |> Repo.insert!()

      PostWithLock
      |> where([p], p.id == ^post.id)
      |> Repo.update_all(set: [lock_version: 99])

      assert {:ok, %PostWithLock{title: "Updated", lock_version: 100}} =
               Actions.find_and_update(PostWithLock, %{id: post.id}, %{title: "Updated"})
    end

    test "returns stale error when record changes between find and update" do
      post =
        %PostWithLock{}
        |> PostWithLock.changeset(%{title: "Original"})
        |> Repo.insert!()

      assert {:ok, %PostWithLock{title: "First Update", lock_version: 2}} =
               Actions.find_and_update(PostWithLock, %{id: post.id}, %{title: "First Update"})

      assert {:error, %{code: :stale}} =
               Actions.update(PostWithLock, post, %{title: "Stale Update"})
    end
  end
end
