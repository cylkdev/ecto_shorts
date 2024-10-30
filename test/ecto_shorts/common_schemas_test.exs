defmodule EctoShorts.CommonSchemasTest do
  use EctoShorts.DataCase
  doctest EctoShorts.CommonSchemas

  alias Ecto.Changeset

  alias EctoShorts.{
    CommonSchemas,
    Support.Schemas.Post,
    Support.Schemas.PostAbstract,
    Support.Schemas.PostNoConstraint,
    Support.Schemas.PostWithPrefix,
    Support.Schemas.PostWithPrefixAbstract
  }

  import Ecto.Query

  describe "get_schema_reflection/1: " do
    test "when given queryable, return expected result" do
      assert [
        :id,
        :title,
        :unique_identifier,
        :likes,
        :tags,
        :views,
        :user_id,
        :inserted_at,
        :updated_at
      ] = EctoShorts.CommonSchemas.get_schema_reflection(Post, :fields)
    end

    test "when given {source, queryable}, return expected result" do
      assert [
        :id,
        :title,
        :unique_identifier,
        :likes,
        :tags,
        :views,
        :user_id,
        :inserted_at,
        :updated_at
      ] = EctoShorts.CommonSchemas.get_schema_reflection({"posts", PostAbstract}, :fields)
    end
  end

  describe "get_schema_reflection/2: " do
    test "when given queryable, return expected result" do
      assert :string = EctoShorts.CommonSchemas.get_schema_reflection(Post, :type, :title)
    end

    test "when given {source, queryable}, return expected result" do
      assert :string = EctoShorts.CommonSchemas.get_schema_reflection({"posts", PostAbstract}, :type, :title)
    end
  end

  describe "get_schema_struct/2: " do
    test "when given queryable, returns built struct" do
      assert %Post{
        __meta__: %Ecto.Schema.Metadata{
          state: :built,
          source: "posts",
          prefix: nil,
          context: nil
        }
      } = EctoShorts.CommonSchemas.get_schema_struct(Post)
    end

    test "when given {source, queryable}, returns loaded struct" do
      assert %PostAbstract{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: nil,
          context: nil
        }
      } = EctoShorts.CommonSchemas.get_schema_struct({"posts", PostAbstract})
    end

    test "when given queryable and module has @schema_prefix attribute, sets prefix to module attribute value" do
      assert %PostWithPrefix{
        __meta__: %Ecto.Schema.Metadata{
          state: :built,
          source: "posts",
          prefix: "custom_schema_prefix",
          context: nil
        }
      } = EctoShorts.CommonSchemas.get_schema_struct(PostWithPrefix)
    end

    test "when given {source, queryable} and module has @schema_prefix attribute, sets prefix to module attribute value" do
      assert %PostWithPrefixAbstract{
        __meta__: %Ecto.Schema.Metadata{
          state: :loaded,
          source: "posts",
          prefix: "custom_schema_prefix",
          context: nil
        }
      } = EctoShorts.CommonSchemas.get_schema_struct({"posts", PostWithPrefixAbstract})
    end
  end

  describe "get_schema_prefix/2: " do
    test "when given queryable and module has @schema_prefix attribute, returns the value of @schema_prefix" do
      assert "custom_schema_prefix" = CommonSchemas.get_schema_prefix(PostWithPrefix)
    end

    test "when given {source, queryable} and module has @schema_prefix attribute, returns the value of @schema_prefix" do
      assert "custom_schema_prefix" = CommonSchemas.get_schema_prefix({"posts", PostWithPrefixAbstract})
    end

    test "when given queryable and schema module does not have @schema_prefix attribute, returns nil" do
      assert nil === CommonSchemas.get_schema_prefix(Post)
    end

    test "when given {source, queryable} and schema module does not have @schema_prefix attribute, returns nil" do
      assert nil === CommonSchemas.get_schema_prefix({"posts", PostAbstract})
    end
  end

  describe "get_schema_source/2: " do
    test "when given {source, queryable}, returns source from schema module" do
      assert "posts" = CommonSchemas.get_schema_source(Post)
    end

    test "when given {source, queryable}, returns source in given tuple" do
      assert "custom_source" = CommonSchemas.get_schema_source({"custom_source", PostAbstract})
    end
  end

  describe "get_schema_queryable/2: " do
    test "when given queryable, returns the given queryable" do
      assert Post = CommonSchemas.get_schema_queryable(Post)
    end

    test "when given {source, queryable}, returns queryable module in given tuple" do
      assert PostAbstract = CommonSchemas.get_schema_queryable({"custom_source", PostAbstract})
    end
  end

  describe "get_schema_query/1: " do
    test "when given a query, returns the query" do
      query = from p in Post

      assert ^query = CommonSchemas.get_schema_query(query)
    end

    test "when given a queryable, returns query" do
      assert %Ecto.Query{} = CommonSchemas.get_schema_query(Post)
    end

    test "when given queryable and schema has module attribute @schema_prefix set, sets the from prefix to @schema_prefix value" do
      assert %Ecto.Query{
        from: %{
          prefix: "custom_schema_prefix",
          source: {"posts", PostWithPrefix}
        }
      } = CommonSchemas.get_schema_query(PostWithPrefix)
    end

    test "when given {source, queryable} and schema has module attribute @schema_prefix set, sets the from prefix to @schema_prefix value" do
      assert %Ecto.Query{
        from: %{
          prefix: "custom_schema_prefix",
          source: {"posts", PostWithPrefixAbstract}
        }
      } = CommonSchemas.get_schema_query({"posts", PostWithPrefixAbstract})
    end
  end

  describe "prepare_changeset/2: " do
    test "when given (changeset, params, opts), return a changeset" do
      changeset = Post.changeset(%Post{id: 1}, %{})

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset(changeset, %{title: "post_title"})
    end

    test "when given (schema_data, params, opts), return a changeset" do
      post = %Post{id: 1}

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset(post, %{title: "post_title"})
    end

    test "when given (queryable, params, opts), return a changeset" do
      assert %Ecto.Changeset{
        data: %Post{},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset(Post, %{title: "post_title"})
    end

    test "when given ({source, queryable}, params, opts), return a changeset" do
      assert %Ecto.Changeset{
        data: %PostAbstract{},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset({"posts", PostAbstract}, %{title: "post_title"})
    end
  end

  describe "prepare_changeset/3: " do
    test "when given (changeset, params, opts), return a changeset" do
      changeset = Post.changeset(%Post{id: 1}, %{})

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset(changeset, %{title: "post_title"}, [])
    end

    test "when given (schema_data, params, opts), return a changeset" do
      post = %Post{id: 1}

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset(post, %{title: "post_title"}, [])
    end

    test "when given (queryable, params, opts), return a changeset" do
      assert %Ecto.Changeset{
        data: %Post{},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset(Post, %{title: "post_title"}, [])
    end

    test "when given ({source, queryable}, params, opts), return a changeset" do
      assert %Ecto.Changeset{
        data: %PostAbstract{},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset({"posts", PostAbstract}, %{title: "post_title"}, [])
    end
  end

  describe "prepare_changeset/4: " do
    test "when given (queryable, changeset, params), returns a valid changeset" do
      changeset = Post.changeset(%Post{id: 1}, %{})

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset(Post, changeset, %{title: "post_title"}, [])
    end

    test "when given ({source, queryable}, changeset, params), returns a valid changeset" do
      changeset = Post.changeset(%Post{id: 1}, %{})

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset({"posts", PostAbstract}, changeset, %{title: "post_title"}, [])
    end

    test "when given (queryable, schema_data, params), return a changeset" do
      post = %Post{id: 1}

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset(Post, post, %{title: "post_title"}, [])
    end

    test "when given ({source, queryable}, schema_data, params), return a changeset" do
      post = %Post{id: 1}

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } = CommonSchemas.prepare_changeset({"posts", PostAbstract}, post, %{title: "post_title"}, [])
    end

    test "when given schema_data the value of option :changeset is a 1-arity function, add constraint" do
      post = %Post{id: 1}

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } =
        CommonSchemas.prepare_changeset(
          PostNoConstraint,
          post,
          %{title: "post_title"},
          changeset: fn changeset ->
            Changeset.no_assoc_constraint(changeset, :comments, name: "comments_post_id_fkey")
          end
        )
    end

    test "when given schema_data the value of option :changeset is a 2-arity function, add constraint" do
      post = %Post{id: 1}

      assert %Ecto.Changeset{
        data: %Post{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } =
        CommonSchemas.prepare_changeset(
          PostNoConstraint,
          post,
          %{title: "post_title"},
          changeset: fn changeset, params ->
            changeset
            |> PostNoConstraint.changeset(params)
            |> Changeset.no_assoc_constraint(:comments, name: "comments_post_id_fkey")
          end
        )
    end

    test "when given schema_data the value of option :changeset is {module, fun, args}, add constraint" do
      post = %PostNoConstraint{id: 1}

      assert %Ecto.Changeset{
        data: %PostNoConstraint{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } =
        CommonSchemas.prepare_changeset(
          PostNoConstraint,
          post,
          %{title: "post_title"},
          changeset: {EctoShorts.Support.MockPostNoConstraintCallback, :changeset, []}
        )
    end

    test "when given schema_data the value of option :changeset is {module, fun}, add constraint" do
      post = %PostNoConstraint{id: 1}

      assert %Ecto.Changeset{
        data: %PostNoConstraint{id: 1},
        changes: %{title: "post_title"},
        valid?: true
      } =
        CommonSchemas.prepare_changeset(
          PostNoConstraint,
          post,
          %{title: "post_title"},
          changeset: {EctoShorts.Support.MockPostNoConstraintCallback, :changeset}
        )
    end
  end
end
