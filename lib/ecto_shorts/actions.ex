defmodule EctoShorts.Actions do
  @moduledoc """
  Data-driven CRUD, bulk, and transactional database operations.

  This module provides a consistent function interface for common database
  operations. All query building is delegated to `EctoShorts.CommonFilters`
  and all changeset management to `EctoShorts.CommonChanges`. You pass a
  schema module and a params map, and Actions handles the rest.

  Use this module when you want to:

  * Perform standard CRUD operations with minimal boilerplate
  * Build queries from data (maps or keyword lists) instead of macros
  * Run transactional multi-record operations with automatic rollback
  * Batch-fetch records by key for efficient lookups

  Do not use this module when you need:

  * Streaming with custom chunk sizes - use `c:Ecto.Repo.stream/2` directly

  ## Getting started

  Configure your `Ecto.Repo` in your application config:

      # config/config.exs
      config :ecto_shorts, repo: MyApp.Repo

  Then call any function with a schema module and params:

      alias EctoShorts.Actions

      # Create a record
      {:ok, post} = Actions.create(Post, %{title: "Hello", body: "World"})

      # Find a single record
      {:ok, post} = Actions.find(Post, %{id: 1})

      # Fetch all matching records
      posts = Actions.all(Post, %{published: true, limit: 10})

      # Update a record
      {:ok, post} = Actions.update(Post, post, %{title: "Updated"})

      # Delete a record
      {:ok, _} = Actions.delete(post)

  ## Function groups

  Functions are organized into five groups based on their behavior:

  * **CRUD** - Single-record operations: `all/1-3`, `create/3`, `find/3`,
    `update/4`, `delete/1-3`, `get/3`, `exists?/3`, `stream/3`,
    `aggregate/5`, `preload/3`, and the `find_and_*` variants.

  * **Bulk** - Multi-row operations without transactions: `insert_all/3`,
    `update_all/4`, `delete_all/3`. These map directly to Ecto.Repo
    callbacks and do not run changesets.

  * **Multi** - Transactional multi-record operations using `Ecto.Multi`:
    `create_many/3`, `update_many/3`, `delete_many/3`, `find_many/3`,
    `find_or_create_many/3`, `find_and_upsert_many/3`. Any failure rolls
    back the entire transaction.

  * **Batch** - Keyed lookups for efficient fetching: `batch/5` and
    `batch_preload/4`.

  * **Transaction** - Transaction wrappers: `transaction/2` and `transact/2`.

  ## Choosing the right function

  This section helps you pick the right function for your use case.

  ### Reading records

  | Function | Use when | Returns |
  |----------|----------|---------|
  | `get/3` | You have a primary key and want the struct or `nil` | `struct \| nil` |
  | `find/3` | You have filter params and want `{:ok, struct}` or an error | `{:ok, struct} \| {:error, reason}` |
  | `all/3` | You want a list of matching records | `[struct]` |
  | `exists?/3` | You only need to know if a match exists | `boolean` |
  | `aggregate/5` | You need a count, sum, avg, min, or max | `term` |
  | `stream/3` | You need to process large datasets without loading all into memory | `Enumerable.t()` |

  Examples:

      # Primary key lookup - returns nil if not found
      post = Actions.get(Post, 1)

      # Filter lookup - returns error tuple if not found
      {:ok, post} = Actions.find(Post, %{slug: "hello-world"})
      {:error, %ErrorMessage{code: :not_found}} = Actions.find(Post, %{slug: "missing"})

      # List all matching
      posts = Actions.all(Post, %{published: true, order_by: [desc: :inserted_at]})

      # Existence check
      true = Actions.exists?(Post, %{published: true})

      # Aggregate
      count = Actions.aggregate(Post, %{published: true}, :count, :id)

  ### Creating records

  | Function | Use when | Returns |
  |----------|----------|---------|
  | `create/3` | Creating a single record with changeset validation | `{:ok, struct} \| {:error, changeset}` |
  | `insert_all/3` | Bulk inserting many records without changesets | `{:ok, {count, nil \| [struct]}}` |
  | `create_many/3` | Creating many records with changesets in a transaction | `{:ok, [struct]} \| {:error, reason}` |

  Use `create/3` for single records when you need validation:

      {:ok, post} = Actions.create(Post, %{title: "Hello", body: "World"})
      {:error, %Ecto.Changeset{}} = Actions.create(Post, %{title: nil})

  Use `insert_all/3` for bulk inserts when performance matters more than
  per-record validation:

      {:ok, {100, nil}} = Actions.insert_all(Post, list_of_100_maps)

  Use `create_many/3` when you need both validation and atomicity:

      {:ok, posts} = Actions.create_many(Post, [%{title: "A"}, %{title: "B"}])

  ### Updating records

  | Function | Use when | Returns |
  |----------|----------|---------|
  | `update/4` | Updating a single record by struct or id | `{:ok, struct} \| {:error, reason}` |
  | `find_and_update/4` | Finding then updating in one call | `{:ok, struct} \| {:error, reason}` |
  | `find_and_upsert/4` | Updating if exists, creating if not | `{:ok, struct} \| {:error, reason}` |
  | `update_all/4` | Bulk updating many records without changesets | `{count, nil}` |
  | `update_many/3` | Updating many records with changesets in a transaction | `{:ok, [struct]} \| {:error, reason}` |

  Examples:

      # Update by struct
      {:ok, post} = Actions.update(Post, post, %{title: "New Title"})

      # Update by id
      {:ok, post} = Actions.update(Post, 1, %{title: "New Title"})

      # Find then update
      {:ok, post} = Actions.find_and_update(Post, %{slug: "hello"}, %{views: 100})

      # Upsert pattern
      {:ok, post} = Actions.find_and_upsert(Post, %{slug: "hello"}, %{views: 100})

      # Bulk update (no changesets)
      {10, nil} = Actions.update_all(Post, %{published: false}, %{set: %{published: true}})

  ### Deleting records

  | Function | Use when | Returns |
  |----------|----------|---------|
  | `delete/1-3` | Deleting a single record or list | `{:ok, struct} \| {:error, reason}` |
  | `find_and_delete/3` | Finding then deleting in one call | `{:ok, struct} \| {:error, reason}` |
  | `delete_all/3` | Bulk deleting many records | `{count, nil}` |
  | `delete_many/3` | Deleting many records in a transaction | `{:ok, [struct]} \| {:error, reason}` |

  Examples:

      # Delete by struct
      {:ok, deleted} = Actions.delete(post)

      # Delete by id
      {:ok, deleted} = Actions.delete(Post, 1)

      # Delete a list
      {:ok, deleted_list} = Actions.delete([post1, post2])

      # Find then delete
      {:ok, deleted} = Actions.find_and_delete(Post, %{slug: "hello"})

      # Bulk delete
      {10, nil} = Actions.delete_all(Post, %{published: false})

  ### Multi vs Bulk

  **Bulk** functions (`insert_all/3`, `update_all/4`, `delete_all/3`):

  * Execute a single SQL statement
  * Do not run changesets or validations
  * Do not wrap in a transaction
  * Return `{count, nil | [struct]}`
  * Best for: high-volume operations where speed matters

  **Multi** functions (`create_many/3`, `update_many/3`, `delete_many/3`, etc.):

  * Execute multiple SQL statements inside an `Ecto.Multi`
  * Run changesets and validations for each record
  * Wrap everything in a transaction - any failure rolls back all changes
  * Return `{:ok, [struct]}` or `{:error, reason}`
  * Best for: operations that must succeed or fail together

  ## Return values and error handling

  Functions return different shapes based on their category.

  ### Single-record functions

  Functions like `create/3`, `find/3`, `update/4`, and `delete/1-3` return:

  * `{:ok, struct}` - operation succeeded
  * `{:error, %Ecto.Changeset{}}` - validation or constraint failed
  * `{:error, %ErrorMessage{}}` - record not found or other error

  Pattern-match on the error shape:

      case Actions.create(Post, params) do
        {:ok, post} ->
          # Success
          post

        {:error, %Ecto.Changeset{} = changeset} ->
          # Validation failed - inspect changeset.errors
          {:error, changeset}

        {:error, %ErrorMessage{code: code}} ->
          # Other error - code is :not_found, :conflict, etc.
          {:error, code}
      end

  ### List functions

  Functions like `all/3` return a list directly (not wrapped in a tuple):

      posts = Actions.all(Post, %{published: true})
      # => [%Post{}, %Post{}, ...]

  ### Bulk functions

  Functions like `insert_all/3`, `update_all/4`, and `delete_all/3` return:

  * `{count, nil}` - count of affected rows, no structs returned
  * `{count, [struct]}` - when `:returning` option is set

  For `insert_all/3`, the return is wrapped:

      {:ok, {100, nil}} = Actions.insert_all(Post, list_of_maps)
      {:error, [%Ecto.Changeset{}]} = Actions.insert_all(Post, invalid_list)

  ### Multi functions

  Functions like `create_many/3`, `update_many/3`, etc. return:

  * `{:ok, [struct]}` - all operations succeeded
  * `{:error, reason}` - one operation failed, all rolled back

  ### Error messages

  When a functions in this API fails, it returns an error tuple:

      {:error, message()}

  `message()` is the “error payload”. It can be any Elixir term
  (for example: a string, a map, or a struct). The exact shape
  is decided by the error adapter.

  By default, the error adapter is `ErrorMessage`.

  See EctoShorts.Actions.Error for customizing the error payload.

  ## CRUD operations

  ### Creating records

  Use `create/3` to insert a new record:

      {:ok, post} = Actions.create(Post, %{title: "Hello", body: "World"})

  The function builds a changeset using the schema's `changeset/2` function,
  then calls `c:Ecto.Repo.insert/2`. Validation errors return an error tuple:

      {:error, %Ecto.Changeset{errors: [title: {"can't be blank", _}]}} =
        Actions.create(Post, %{title: nil})

  Override the changeset function with the `:changeset` option:

      {:ok, post} = Actions.create(Post, params, changeset: &Post.admin_changeset/2)

  ### Reading records

  **Single record by filter** - use `find/3`:

      {:ok, post} = Actions.find(Post, %{slug: "hello-world"})

  Returns `{:error, %ErrorMessage{code: :not_found}}` when no match exists.
  Passing an empty map to `find/3` returns a `:not_found` error immediately
  without querying the database.

  **Single record by primary key** - use `get/3`:

      post = Actions.get(Post, 1)
      # => %Post{id: 1, ...} or nil

  **List of records** - use `all/3`:

      posts = Actions.all(Post, %{published: true, limit: 10, order_by: :title})

  The params map supports all `EctoShorts.CommonFilters` keys:

      posts = Actions.all(Post, %{
        published: true,
        inserted_at: %{>=: ~U[2024-01-01 00:00:00Z]},
        order_by: [desc: :inserted_at],
        limit: 10,
        preload: :author
      })

  **Existence check** - use `exists?/3`:

      true = Actions.exists?(Post, %{published: true})

  **Aggregates** - use `aggregate/5`:

      count = Actions.aggregate(Post, %{published: true}, :count, :id)
      total_views = Actions.aggregate(Post, %{}, :sum, :views)

  **Streaming** - use `stream/3` inside a transaction:

      Actions.transact(fn ->
        Post
        |> Actions.stream(%{published: true})
        |> Stream.each(&process_post/1)
        |> Stream.run()
      end)

  ### Updating records

  Update by struct:

      {:ok, updated} = Actions.update(Post, post, %{title: "New Title"})

  Update by id (fetches the record first):

      {:ok, updated} = Actions.update(Post, 1, %{title: "New Title"})

  Find then update in one call:

      {:ok, updated} = Actions.find_and_update(Post, %{slug: "hello"}, %{views: 100})

  Upsert pattern (update if exists, create if not):

      {:ok, post} = Actions.find_and_upsert(
        Post,
        %{slug: "hello"},           # find params
        %{title: "Hello", views: 0} # upsert params
      )

  ### Deleting records

  Delete by struct:

      {:ok, deleted} = Actions.delete(post)

  Delete by id:

      {:ok, deleted} = Actions.delete(Post, 1)

  Delete a list (stops on first failure):

      {:ok, deleted_list} = Actions.delete([post1, post2])

  Find then delete:

      {:ok, deleted} = Actions.find_and_delete(Post, %{slug: "hello"})

  ## Bulk operations

  Bulk functions execute a single SQL statement without changesets or
  transactions. Use them for high-volume operations.

  ### insert_all/3

  Insert many records at once:

      entries = [
        %{title: "Post 1", body: "Body 1"},
        %{title: "Post 2", body: "Body 2"}
      ]

      {:ok, {2, nil}} = Actions.insert_all(Post, entries)

  By default, entries are validated through the schema's `changeset/2`.
  Skip validation with `validate: false`:

      {:ok, {2, nil}} = Actions.insert_all(Post, entries, validate: false)

  Handle conflicts with `:on_conflict` options:

      {:ok, {2, nil}} = Actions.insert_all(Post, entries,
        on_conflict: :nothing,
        conflict_target: :slug
      )

  ### update_all/4

  Update many records matching a filter:

      {10, nil} = Actions.update_all(Post, %{published: false}, %{set: %{published: true}})

  Supported update operations:

  * `:set` - set field values
  * `:inc` - increment numeric fields
  * `:push` - append to array fields
  * `:pull` - remove from array fields

  Examples:

      # Set multiple fields
      Actions.update_all(Post, %{draft: true}, %{set: %{published: true, draft: false}})

      # Increment a counter
      Actions.update_all(Post, %{id: 1}, %{inc: %{views: 1}})

  ### delete_all/3

  Delete many records matching a filter:

      {10, nil} = Actions.delete_all(Post, %{published: false})

  Delete all records (use with caution):

      {count, nil} = Actions.delete_all(Post)

  ## Multi (transactional) operations

  Multi functions wrap multiple operations in an `Ecto.Multi` transaction.
  If any operation fails, all changes are rolled back.

  ### create_many/3

  Create multiple records atomically:

      {:ok, posts} = Actions.create_many(Post, [
        %{title: "Post 1", body: "Body 1"},
        %{title: "Post 2", body: "Body 2"}
      ])

  If any record fails validation, the entire transaction rolls back:

      {:error, reason} = Actions.create_many(Post, [
        %{title: "Valid"},
        %{title: nil}  # Invalid - rolls back the first insert too
      ])

  ### find_many/3

  Find multiple records atomically:

      {:ok, posts} = Actions.find_many(Post, [%{id: 1}, %{id: 2}])

  If any record is not found, the transaction fails:

      {:error, reason} = Actions.find_many(Post, [%{id: 1}, %{id: 999}])

  ### update_many/3

  Update multiple records atomically. Entries can be tuples or maps:

      # Tuple format: {find_params, update_params}
      {:ok, posts} = Actions.update_many(Post, [
        {%{id: 1}, %{title: "Updated 1"}},
        {%{id: 2}, %{title: "Updated 2"}}
      ])

      # Map format with :id key
      {:ok, posts} = Actions.update_many(Post, [
        %{id: 1, title: "Updated 1"},
        %{id: 2, title: "Updated 2"}
      ])

  ### delete_many/3

  Delete multiple records atomically:

      {:ok, deleted} = Actions.delete_many(Post, [post1, post2])

  Entries can be structs, param maps, or raw id values.

  ### find_or_create_many/3

  Find or create multiple records atomically:

      {:ok, posts} = Actions.find_or_create_many(Post, [
        %{slug: "existing", title: "Existing"},
        %{slug: "new", title: "New Post"}
      ])

  ### find_and_upsert_many/3

  Upsert multiple records atomically:

      {:ok, posts} = Actions.find_and_upsert_many(Post, [
        {%{slug: "hello"}, %{views: 100}},
        {%{slug: "world"}, %{views: 200}}
      ])

  ## Batch operations

  Batch functions efficiently fetch records by key.

  ### batch/5

  Fetch records grouped by a batch key:

      # Single key
      results = Actions.batch(Post, [%{author_id: 1}, %{author_id: 2}], :author_id)
      # => %{1 => [%Post{}, ...], 2 => [%Post{}, ...]}

      # With :one cardinality (raises if multiple found)
      results = Actions.batch(User, [%{id: 1}, %{id: 2}], :id, :one)
      # => %{1 => %User{}, 2 => %User{}}

      # Composite keys
      results = Actions.batch(Post, params, [:author_id, :category_id])
      # => %{%{author_id: 1, category_id: 2} => [...], ...}

  ### batch_preload/4

  Fetch records and zip them into the original entries:

      entries = [%{user_id: 1, data: "a"}, %{user_id: 2, data: "b"}]
      enriched = Actions.batch_preload(User, entries, :user_id)
      # Each entry now has the User fields merged in

  ## Transactions

  ### transaction/2

  Wrap a function or `Ecto.Multi` in a transaction:

      {:ok, result} = Actions.transaction(fn ->
        {:ok, post} = Actions.create(Post, %{title: "Hello"})
        {:ok, comment} = Actions.create(Comment, %{post_id: post.id, body: "Hi"})
        {post, comment}
      end)

  ### transact/2

  Like `transaction/2` but normalizes the return value:

      # With :strict (default), {:error, reason} triggers rollback
      {:ok, post} = Actions.transact(fn ->
        Actions.create(Post, %{title: "Hello"})
      end)

      # Errors are unwrapped
      {:error, changeset} = Actions.transact(fn ->
        Actions.create(Post, %{title: nil})
      end)

  Use `transact/2` with `Ecto.Multi`:

      multi =
        Ecto.Multi.new()
        |> Ecto.Multi.insert(:post, Post.changeset(%Post{}, %{title: "Hello"}))

      {:ok, [post]} = Actions.transact(multi)

  ## Shared options

  Every public function accepts an `opts` keyword list. These keys are
  recognized across the module:

  * `:repo` - the `Ecto.Repo` for write operations. Defaults to
    `EctoShorts.Config.repo/0`.

  * `:replica` - the `Ecto.Repo` for read operations. Falls back to
    `:repo` when not set. Defaults to `EctoShorts.Config.replica/0`.

  * `:changeset` - a function that replaces the schema's default
    `changeset/2`. Can be 1-arity (receives params), 2-arity (receives
    struct and params), or 3-arity (receives struct, params, and opts).

  * `:dynamic_adapter` - a module implementing `EctoShorts.Dynamics.Adapter`.
    Defaults to `EctoShorts.Config.dynamic_adapter/0`.

  * `:error_module` - a module implementing `EctoShorts.Actions.Error`.
    Defaults to `EctoShorts.Config.error_module/0`.

  ## Configuration

  Configure EctoShorts in your application config:

      # config/config.exs
      config :ecto_shorts,
        repo: MyApp.Repo,
        replica: MyApp.Repo.Replica,
        error_module: MyApp.Error

  Override at runtime by passing options:

      Actions.all(Post, %{published: true}, repo: MyApp.OtherRepo)

  See `EctoShorts.Config` for all configuration options.

  ## Filtering with CommonFilters

  The `params` argument in read functions supports the full
  `EctoShorts.CommonFilters` language:

      # Field equality
      Actions.all(Post, %{published: true})

      # Comparison operators
      Actions.all(Post, %{views: %{>: 100}})

      # Logical operators
      Actions.all(Post, %{or: [[published: true], [draft: true]]})

      # Query operations
      Actions.all(Post, %{
        order_by: [desc: :inserted_at],
        limit: 10,
        preload: [:author, :comments]
      })

  See `EctoShorts.CommonFilters` for the complete filtering language.

  ## Custom changesets

  Override the default `changeset/2` function with the `:changeset` option:

      # 2-arity: receives struct and params
      Actions.create(Post, params, changeset: &Post.admin_changeset/2)

      # 1-arity: receives params only (for create)
      Actions.create(Post, params, changeset: fn params ->
        Post.changeset(%Post{}, Map.put(params, :source, "api"))
      end)

      # 3-arity: receives struct, params, and opts
      Actions.update(Post, post, params, changeset: fn struct, params, opts ->
        Post.changeset(struct, params, opts)
      end)

  See `EctoShorts.CommonChanges` for changeset helpers.

  ## Edge cases and warnings

  > #### Empty params in find/3 {: .warning}
  >
  > Calling `find/3` with an empty map returns `{:error, :not_found}`
  > immediately without querying the database. This prevents accidental
  > fetches of arbitrary records.

  > #### Concurrent updates {: .info}
  >
  > `update/4` and `find_and_update/4` do not use optimistic locking by
  > default. For concurrent updates, use `Ecto.Changeset.optimistic_lock/3`
  > in your changeset function.

  > #### Large batch sizes {: .info}
  >
  > `insert_all/3` and `update_all/4` execute a single SQL statement.
  > Very large batches may exceed database limits. Consider chunking
  > into smaller batches for thousands of records.

  See also `EctoShorts.CommonFilters`, `EctoShorts.CommonChanges`,
  `EctoShorts.Config`, and `EctoShorts.Actions.Error`.
  """

  @moduledoc groups: [
               %{title: "CRUD", description: "Single-record create, read, update, and delete operations."},
               %{title: "Bulk", description: "Multi-row operations without transactions."},
               %{title: "Multi", description: "Transactional multi-record operations using Ecto.Multi."},
               %{title: "Batch", description: "Batch lookups and preloads."},
               %{title: "Transaction", description: "Transaction wrappers."}
             ]

  alias EctoShorts.Actions.Batch
  alias EctoShorts.Actions.Error
  alias EctoShorts.Actions.Multi

  alias EctoShorts.{
    Config,
    CommonFilters,
    CommonParams,
    CommonSchema
  }

  @cardinalities [:one, :many]

  @doc group: "CRUD"
  @doc since: "3.0.0"
  @doc """
  Preloads associations on the given struct or list of structs.

  Delegates to `c:Ecto.Repo.preload/3` on the configured replica repo.

  ## Arguments

    * `data` - a struct or list of structs to preload.
    * `preloads` - an atom, list, or keyword list of associations.
    * `opts` - forwarded to `c:Ecto.Repo.preload/3`.

  ## Examples

      post_with_author = EctoShorts.Actions.preload(post, :author)
      posts_with_tags  = EctoShorts.Actions.preload(posts, [:author, :comments])

  See also `all/3` and `EctoShorts.CommonChanges.preload_change_assoc/3`.
  """
  def preload(data, preloads, opts \\ []) do
    Config.replica!(opts).preload(data, preloads, opts)
  end

  @doc group: "CRUD"
  @doc since: "3.0.0"
  @doc """
  Returns `true` if at least one record matches `params`, `false` otherwise.

  Builds the query with `EctoShorts.CommonFilters` and delegates to
  `c:Ecto.Repo.exists?/2` on the configured replica repo.

  ## Arguments

    * `source` - a schema module or queryable.
    * `params` - filter params (see `EctoShorts.CommonFilters`).
    * `opts` - forwarded to `c:Ecto.Repo.exists?/2`.

  ## Examples

      true = EctoShorts.Actions.exists?(EctoShorts.Schema.Post, %{published: true})

  See also `find/3` and `all/3`.
  """
  def exists?(source, params, opts \\ []) do
    source
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).exists?(opts)
  end

  @doc group: "CRUD"
  @doc """
  Fetches all records for the given queryable.

  Equivalent to `all(queryable, %{}, [])`.

  ## Arguments

    * `queryable` - a schema module, `{source, schema}` tuple, or
      `Ecto.Query`.

  See also `all/2`, `all/3`, and `find/3`.
  """
  def all(queryable) do
    all(queryable, %{}, [])
  end

  @doc group: "CRUD"
  @doc """
  Fetches all records matching `params` or `opts`.

  When the second argument is a map it is used as filter params. When
  it is a keyword list, filter keys are extracted and `:repo`,
  `:replica`, and `:dynamic_adapter` are forwarded as options.

  ## Arguments

    * `queryable` - a schema module, `{source, schema}` tuple, or
      `Ecto.Query`.
    * `params` - a map of filter params, or a keyword list of mixed
      filter params and options.

  ## Examples

      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, %{published: true})
      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, replica: MyApp.Repo)

  See also `all/1`, `all/3`, and `find/3`.
  """
  def all(queryable, params) when is_map(params) do
    all(queryable, params, [])
  end

  def all(queryable, opts) do
    params =
      opts
      |> Keyword.drop([:repo, :replica, :dynamic_adapter])
      |> Map.new()

    all(queryable, params, Keyword.take(opts, [:repo, :replica, :dynamic_adapter]))
  end

  @doc group: "CRUD"
  @doc """
  Fetches all records matching `params`.

  Returns a list of structs.

  ## Arguments

    * `queryable` - a schema module, `{source, schema}` tuple, or
      `Ecto.Query`.
    * `params` - filter params (see `EctoShorts.CommonFilters`).
    * `opts` - keyword list of options.

  ## Options

    * `:order_by` - merged into `params` before query building.
    * `:group_by` - merged into `params` before query building.

  All other options are forwarded to `c:Ecto.Repo.all/2`.

  ## Examples

      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, %{published: true}, order_by: :title)

  See also `find/3`, `stream/3`, and `EctoShorts.CommonFilters`.
  """
  def all(queryable, params, opts) do
    params =
      params
      |> put_param(opts, :order_by)
      |> put_param(opts, :group_by)

    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).all(opts)
  end

  @doc group: "CRUD"
  @doc """
  Inserts a new record built from `params`.

  Builds a changeset via the schema's `changeset/2` (or the
  `:changeset` option) and delegates to `c:Ecto.Repo.insert/2`.

  Returns `{:ok, struct}` or `{:error, changeset}`.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `params` - a map of attributes for the new record.
    * `opts` - forwarded to `c:Ecto.Repo.insert/2`.

  ## Examples

      iex> EctoShorts.Actions.create(EctoShorts.Schema.Post, %{title: "Hello", body: "World"}, repo: EctoShorts.Repo)
      {:ok, %EctoShorts.Schema.Post{title: "Hello", body: "World", ...}}

  See also `find/3`, `update/4`, and `EctoShorts.CommonChanges`.
  """
  def create(schema, params, opts \\ []) do
    schema
    |> CommonSchema.create_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
  end

  @doc group: "CRUD"
  @doc """
  Fetches a single record by primary key.

  Returns the struct or `nil`. Delegates to `c:Ecto.Repo.get/3` on
  the configured replica repo.

  ## Arguments

    * `queryable` - a schema module or queryable.
    * `id` - the primary key value.
    * `opts` - forwarded to `c:Ecto.Repo.get/3`.

  ## Examples

      post = EctoShorts.Actions.get(EctoShorts.Schema.Post, 1)

  See also `find/3` and `all/3`.
  """
  def get(queryable, id, opts \\ []) do
    Config.replica!(opts).get(queryable, id, opts)
  end

  @doc group: "CRUD"
  @doc """
  Finds a single record matching `params`.

  Returns `{:ok, struct}` when a record is found, or
  `{:error, %ErrorMessage{code: :not_found}}` otherwise. When `params`
  is an empty map and `queryable` is not an `Ecto.Query`, the error is
  returned immediately without querying.

  ## Arguments

    * `queryable` - a schema module or queryable.
    * `params` - filter params (see `EctoShorts.CommonFilters`).
    * `opts` - keyword list of options.

  ## Options

    * `:order_by` - merged into `params` before query building.
    * `:group_by` - merged into `params` before query building.

  ## Examples

      iex> EctoShorts.Actions.find(EctoShorts.Schema.Post, %{id: 1})
      {:ok, %EctoShorts.Schema.Post{id: 1, ...}}

      iex> EctoShorts.Actions.find(EctoShorts.Schema.Post, %{id: -1})
      {:error, %ErrorMessage{code: :not_found, message: "record not found."}}

  See also `all/3`, `create/3`, and `find_or_create/3`.
  """
  def find(queryable, params, opts \\ [])

  def find(query, params, opts)
      when params === %{} and not is_struct(query, Ecto.Query) do
    {:error,
     Error.call(
       :not_found,
       "record not found.",
       %{
         query: query,
         params: params
       },
       opts
     )}
  end

  def find(source, params, opts) do
    params =
      params
      |> put_param(opts, :order_by)
      |> put_param(opts, :group_by)

    opts = Keyword.drop(opts, [:order_by, :group_by])

    case source
         |> CommonFilters.convert_params_to_filter(params, opts)
         |> Config.replica!(opts).one(opts) do
      nil ->
        {:error,
         Error.call(
           :not_found,
           "record not found.",
           %{
             query: source,
             params: params
           },
           opts
         )}

      record ->
        {:ok, record}
    end
  end

  @doc group: "CRUD"
  @doc """
  Updates a record by id or by struct.

  When `id_or_schema_struct` is an integer or binary, the record is
  fetched with `find/3` first. When it is a struct, the changeset is
  built and updated directly.

  Returns `{:ok, struct}`, `{:error, changeset}`, or
  `{:error, %ErrorMessage{code: :not_found}}`.

  ## Arguments

    * `queryable` - the Ecto schema module.
    * `id_or_schema_struct` - a primary key value or an existing struct.
    * `params` - a map of attributes to update.
    * `opts` - forwarded to `c:Ecto.Repo.update/2`.

  ## Examples

      {:ok, post} = EctoShorts.Actions.update(EctoShorts.Schema.Post, 1, %{title: "New"}, repo: EctoShorts.Repo)
      {:ok, post} = EctoShorts.Actions.update(EctoShorts.Schema.Post, post, %{title: "New"})

  See also `find_and_update/4`, `create/3`, and `EctoShorts.CommonChanges`.
  """
  def update(queryable, id_or_schema_struct, params, opts \\ [])

  def update(queryable, id, params, opts) when is_integer(id) or is_binary(id) do
    with {:ok, record} <- find(queryable, %{id: id}, opts) do
      update(queryable, record, params, opts)
    end
  end

  def update(queryable, schema_struct, params, opts) do
    queryable
    |> CommonSchema.create_changeset(schema_struct, params, opts)
    |> Config.repo!(opts).update(opts)
  end

  @doc group: "CRUD"
  @doc """
  Deletes a struct, changeset, or list of either.

  A delete changeset is built via the schema's `changeset/2` and
  deleted through the configured repo. When given a list, stops on
  the first failure (already-deleted entries are not rolled back).

  Returns `{:ok, struct}`, `{:ok, [struct]}`, or `{:error, reason}`.

  ## Arguments

    * `data` - a struct, `Ecto.Changeset`, or list of either.

  ## Examples

      {:ok, deleted} = EctoShorts.Actions.delete(post)
      {:ok, deleted_list} = EctoShorts.Actions.delete([post1, post2])

  See also `delete/2`, `delete/3`, and `find_and_delete/3`.
  """
  def delete(data) do
    delete(data, [])
  end

  @doc group: "CRUD"
  @doc """
  Deletes a struct, changeset, or list of either with options.

  ## Arguments

    * `data` - a struct, `Ecto.Changeset`, or list of either.
    * `opts` - forwarded to `c:Ecto.Repo.delete/2`.

  See `delete/1` for return values. See also `delete/3` and
  `find_and_delete/3`.
  """
  def delete(data, opts)

  def delete(queryable, id) when is_binary(id) or is_integer(id) do
    delete(queryable, id, [])
  end

  def delete(%{data: %{__meta__: %{schema: schema}}} = changeset, opts) do
    do_delete(changeset, schema, opts)
  end

  def delete(%{__meta__: %{schema: schema}} = schema_struct, opts) do
    do_delete(schema_struct, schema, opts)
  end

  def delete(records_or_changesets, opts) when is_list(records_or_changesets) do
    with {:ok, results} <-
           Enum.reduce_while(records_or_changesets, {:ok, []}, fn entry, {:ok, acc} ->
             case delete(entry, opts) do
               {:ok, result} -> {:cont, {:ok, [result | acc]}}
               {:error, reason} -> {:halt, {:error, reason}}
             end
           end) do
      {:ok, Enum.reverse(results)}
    end
  end

  @doc group: "CRUD"
  @doc """
  Deletes a record by id.

  Fetches the record with `find/3`, then deletes it.

  Returns `{:ok, struct}` or `{:error, reason}`.

  ## Arguments

    * `queryable` - the Ecto schema module.
    * `id` - the primary key value.
    * `opts` - forwarded to `c:Ecto.Repo.delete/2`.

  ## Examples

      {:ok, deleted} = EctoShorts.Actions.delete(EctoShorts.Schema.Post, 1)

  See also `delete/1`, `delete_all/3`, and `find_and_delete/3`.
  """
  def delete(queryable, id, opts) when is_integer(id) or is_binary(id) do
    with {:ok, record} <- find(queryable, %{id: id}, opts) do
      delete(record, opts)
    end
  end

  def delete(_queryable, %_{} = struct_or_changeset, opts) do
    delete(struct_or_changeset, opts)
  end

  @doc group: "CRUD"
  @doc """
  Returns a stream of records matching `params`.

  The stream must be consumed inside a transaction (see `transact/2`).

  ## Arguments

    * `queryable` - a schema module or queryable.
    * `params` - filter params (see `EctoShorts.CommonFilters`).
    * `opts` - forwarded to `c:Ecto.Repo.stream/2`.

  ## Examples

      EctoShorts.Actions.transact(fn ->
        EctoShorts.Schema.Post
        |> EctoShorts.Actions.stream(%{published: true})
        |> Stream.each(&process_post/1)
        |> Stream.run()
      end)

  See also `all/3`, `transact/2`, and `EctoShorts.CommonFilters`.
  """
  def stream(queryable, params \\ %{}, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).stream(opts)
  end

  @doc group: "CRUD"
  @doc """
  Runs an aggregate on filtered records.

  Delegates to `c:Ecto.Repo.aggregate/4` on the configured replica repo.

  ## Arguments

    * `queryable` - a schema module or queryable.
    * `params` - filter params (see `EctoShorts.CommonFilters`).
      Defaults to `%{}`.
    * `aggregate` - the aggregate function atom. Defaults to `:count`.
    * `key` - the field to aggregate over. Defaults to `:id`.
    * `opts` - forwarded to `c:Ecto.Repo.aggregate/4`.

  ## Examples

      count = EctoShorts.Actions.aggregate(EctoShorts.Schema.Post, %{published: true})
      total = EctoShorts.Actions.aggregate(EctoShorts.Schema.Post, %{}, :sum, :views)

  See also `all/3` and `exists?/3`.
  """
  def aggregate(queryable, params \\ %{}, aggregate \\ :count, key \\ :id, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.replica!(opts).aggregate(aggregate, key, opts)
  end

  @doc group: "CRUD"
  @doc since: "3.0.0"
  @doc """
  Finds a record matching `find_params`, or creates one with `create_params`.

  Returns `{:ok, struct}` in both cases, or `{:error, changeset}` when
  creation fails.

  ## Arguments

    * `queryable` - the Ecto schema module.
    * `find_params` - filter params for the lookup.
    * `create_params` - attributes for the new record if not found.
    * `opts` - shared options.

  ## Examples

      {:ok, post} = EctoShorts.Actions.find_and_create(
        EctoShorts.Schema.Post,
        %{title: "Hello"},
        %{title: "Hello", body: "World"}
      )

  See also `find_or_create/3`, `find_and_update/4`, and `create/3`.
  """
  def find_and_create(queryable, find_params, create_params, opts \\ []) do
    with {:error, _} <- find(queryable, find_params, opts) do
      create(queryable, create_params, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds a record matching `find_params` and updates it with `update_params`.

  Returns `{:ok, struct}`, `{:error, changeset}`, or
  `{:error, %ErrorMessage{code: :not_found}}`.

  ## Arguments

    * `source` - the Ecto schema module.
    * `find_params` - filter params for the lookup.
    * `update_params` - attributes to update on the found record.
    * `opts` - shared options.

  ## Examples

      {:ok, post} = EctoShorts.Actions.find_and_update(
        EctoShorts.Schema.Post,
        %{id: 1},
        %{title: "Updated"}
      )

  See also `update/4`, `find_and_upsert/4`, and `find/3`.
  """
  def find_and_update(source, find_params, update_params, opts \\ []) do
    with {:ok, record} <- find(source, find_params, opts) do
      update(source, record, update_params, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds a record matching `find_params` and updates it, or creates one.

  When found, updates with `upsert_params`. When not found, creates a
  record from `Map.merge(find_params, upsert_params)`.

  Returns `{:ok, struct}` or `{:error, changeset}`.

  ## Arguments

    * `source` - the Ecto schema module.
    * `find_params` - filter params for the lookup.
    * `upsert_params` - attributes for update or creation.
    * `opts` - shared options.

  ## Examples

      {:ok, post} = EctoShorts.Actions.find_and_upsert(
        EctoShorts.Schema.Post,
        %{title: "Hello"},
        %{body: "Updated body"}
      )

  See also `find_and_update/4`, `find_or_create/3`, and `create/3`.
  """
  def find_and_upsert(source, find_params, upsert_params, opts \\ []) do
    case find(source, find_params, opts) do
      {:ok, record} -> update(source, record, upsert_params, opts)
      {:error, _} -> create(source, Map.merge(find_params, upsert_params), opts)
    end
  end

  @doc group: "CRUD"
  @doc since: "3.0.0"
  @doc """
  Finds a record matching `find_params` and deletes it.

  Returns `{:ok, struct}` or `{:error, reason}`.

  ## Arguments

    * `source` - the Ecto schema module.
    * `find_params` - filter params for the lookup.
    * `opts` - shared options.

  ## Examples

      {:ok, deleted} = EctoShorts.Actions.find_and_delete(EctoShorts.Schema.Post, %{title: "Hello"})

  See also `delete/1`, `delete_all/3`, and `find/3`.
  """
  def find_and_delete(source, find_params, opts \\ []) do
    with {:ok, record} <- find(source, find_params, opts) do
      delete(record, opts)
    end
  end

  @doc group: "CRUD"
  @doc """
  Finds a record by the schema's query fields, or creates one.

  The lookup filters `params` to the schema's query fields. When no
  record matches, creates one from the full `params` map.

  Returns `{:ok, struct}` or `{:error, changeset}`.

  ## Arguments

    * `source` - the Ecto schema module.
    * `params` - attributes used for both the lookup and creation.
    * `opts` - shared options.

  ## Examples

      {:ok, post} = EctoShorts.Actions.find_or_create(
        EctoShorts.Schema.Post,
        %{title: "Hello", body: "World"}
      )

  See also `find_and_create/4`, `find_or_create_many/3`, and `create/3`.
  """
  def find_or_create(source, params, opts \\ []) do
    with {:error, _} <-
           find(
             source,
             Map.take(params, CommonSchema.get_query_fields(opts, source)),
             opts
           ) do
      source
      |> CommonSchema.get_schema_source()
      |> create(params, opts)
    end
  end

  @doc group: "Transaction"
  @doc since: "3.0.0"
  @doc """
  Runs `fun_or_multi` in a transaction.

  Delegates to `c:Ecto.Repo.transaction/2`.

  ## Arguments

    * `fun_or_multi` - a function or `Ecto.Multi`.
    * `opts` - forwarded to `c:Ecto.Repo.transaction/2`.

  ## Examples

      {:ok, _} = EctoShorts.Actions.transaction(fn ->
        EctoShorts.Actions.create(EctoShorts.Schema.Post, %{title: "Hello"})
      end)

  See also `transact/2` and `create_many/3`.
  """
  def transaction(fun_or_multi, opts \\ []) do
    Config.repo!(opts).transaction(fun_or_multi, opts)
  end

  @doc group: "Transaction"
  @doc since: "3.0.0"
  @doc """
  Runs a transaction and normalizes the return value.

  When given an `Ecto.Multi`, normalizes the multi response into
  `{:ok, [struct]}` or `{:error, reason}`. When given a 0- or 1-arity
  function, wraps it in a transaction. With `:strict` (default `true`),
  `{:error, reason}` triggers a rollback and `{:ok, value}` is
  unwrapped.

  ## Arguments

    * `fun_or_multi` - a function or `Ecto.Multi`.
    * `opts` - keyword list of options.

  ## Options

    * `:strict` (default: `true`) - roll back on `{:error, reason}`
      and unwrap `{:ok, value}`.

  See also `transaction/2` and `create_many/3`.
  """
  def transact(fun_or_multi, opts \\ [])

  def transact(%Ecto.Multi{} = multi, opts) do
    multi
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  def transact(fun, opts) when is_function(fun) do
    fn repo -> eval_transaction_fun(fun, repo, opts) end
    |> transaction(opts)
    |> normalize_transaction_response(opts)
  end

  @doc group: "Batch"
  @doc since: "3.0.0"
  @doc """
  Batches records by key(s) and cardinality.

  Returns a map keyed by batch key value (or a map of values for
  composite keys). Each value is a struct (`:one`) or list of structs
  (`:many`). Returns `%{}` when `params` is empty.

  Raises `ArgumentError` when `:one` cardinality finds multiple records
  for a single batch key.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `params` - a list of param maps to batch.
    * `batch_keys` - an atom or list of atoms. Defaults to `:id`.
    * `cardinality` - `:one` or `:many`. Defaults to `:many`.
    * `opts` - shared options.
  """
  def batch(schema, params, batch_keys \\ :id, cardinality \\ :many, opts \\ [])

  def batch(_schema, [], _batch_keys, _cardinality, _opts) do
    %{}
  end

  def batch(schema, params, batch_keys, cardinality, opts)
      when is_list(batch_keys) and cardinality in @cardinalities do
    batch_keys = Enum.uniq(batch_keys)

    case Batch.build_batch_params(schema, params, batch_keys, opts) do
      [] ->
        %{}

      batch_params ->
        schema
        |> CommonFilters.convert_params_to_filter(batch_params, opts)
        |> Config.repo!(opts).all(opts)
        |> Enum.group_by(&Map.take(&1, batch_keys))
        |> Batch.handle_batch_response(cardinality, batch_keys)
    end
  end

  def batch(schema, params, batch_key, cardinality, opts)
      when cardinality in @cardinalities do
    values =
      params
      |> Enum.map(&Batch.normalize_batch_key(&1, batch_key))
      |> Enum.uniq()

    if values === [] do
      %{}
    else
      schema
      |> CommonFilters.convert_params_to_filter(%{batch_key => values}, opts)
      |> Config.repo!(opts).all(opts)
      |> Enum.group_by(&Batch.normalize_batch_key(&1, batch_key))
      |> Batch.handle_batch_response(cardinality, batch_key)
    end
  end

  @doc group: "Batch"
  @doc since: "3.0.0"
  @doc """
  Batch-fetches records and zips them into the original entries.

  Looks up records by `keys` using `batch/5` with `:one` cardinality.
  Each matched record is merged into the corresponding entry.
  Unmatched entries are returned unchanged.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `entries` - a list of maps or keyword lists.
    * `keys` - an atom or list of atoms identifying the lookup fields.
    * `opts` - shared options.
  """
  def batch_preload(schema, entries, keys, opts \\ []) do
    {params_list, index_to_key} = Batch.extract_lookup_params(entries, keys)

    key_fields = Batch.normalize_key_fields(keys)

    fetched_records = batch(schema, params_list, key_fields, :one, opts)

    Enum.reduce(index_to_key, entries, fn {index, batch_key}, acc ->
      case Map.get(fetched_records, batch_key) do
        nil ->
          acc

        record ->
          current_entry = get_in(acc, [Access.at!(index)])
          updated_entry = Batch.zip_batch_result(current_entry, record)
          put_in(acc, [Access.at!(index)], updated_entry)
      end
    end)
  end

  @doc group: "Bulk"
  @doc since: "3.0.0"
  @doc """
  Inserts many records via `c:Ecto.Repo.insert_all/3`.

  Each entry is validated through the schema's `changeset/2` unless
  `validate: false` is set. When `:preload` is set, matching records
  are fetched with `batch_preload/4` before insertion.

  Returns `{:ok, {count, nil | [struct]}}` or
  `{:error, [changeset]}`.

  ## Arguments

    * `source` - the Ecto schema module.
    * `params_list` - a list of maps, keyword lists, structs,
      `{struct, params}` tuples, or changesets.
    * `opts` - keyword list of options.

  ## Options

    * `:preload` - atom or list of atoms for batch-preloading.
    * `:validate` - `false` to skip changeset validation.
    * `:on_conflict_replace` - `:none`, `:insert_keys` (default),
      or a list of field atoms.
    * `:placeholders` - a map of `{field, match_value}`.
    * `:on_placeholder_conflict` - `:nothing` (default),
      `:replace_all`, or `{:replace, [fields]}`.

  See `EctoShorts.CommonParams.convert_to_insert_params/3` for
  timestamp options.
  """
  def insert_all(source, params_list, opts \\ []) do
    params_list =
      if Keyword.has_key?(opts, :preload) do
        batch_preload(source, params_list, opts[:preload], opts)
      else
        params_list
      end

    with {:ok, inserts} <- CommonParams.convert_to_insert_params(source, params_list, opts) do
      on_conflict_options = CommonParams.build_on_conflict_options(source, inserts, opts)

      {:ok,
       Config.repo!(opts).insert_all(
         source,
         inserts,
         Keyword.merge(on_conflict_options, opts)
       )}
    end
  end

  @doc group: "Bulk"
  @doc since: "3.0.0"
  @doc """
  Updates all records matching `find_params`.

  Supports `:set`, `:inc`, `:push`, and `:pull` operations in
  `update_params`. Returns `{count, nil}`.

  ## Arguments

    * `source` - the Ecto schema module.
    * `find_params` - filter params for the query.
    * `update_params` - a map of update operations.
    * `opts` - forwarded to `c:Ecto.Repo.update_all/3`.

  See also `EctoShorts.CommonParams.convert_to_update_params/3`
  and `update_many/3`.
  """
  def update_all(source, find_params, update_params, opts \\ []) do
    updates =
      source
      |> CommonSchema.get_schema_source()
      |> CommonParams.convert_to_update_params(update_params, opts)

    source
    |> CommonFilters.convert_params_to_filter(find_params, opts)
    |> Config.repo!(opts).update_all(updates, opts)
  end

  @doc group: "Bulk"
  @doc since: "3.0.0"
  @doc """
  Deletes all records matching `params`.

  Returns `{count, nil}`.

  ## Arguments

    * `queryable` - the Ecto schema module or queryable.
    * `params` - filter params (see `EctoShorts.CommonFilters`).
      Defaults to `%{}`.
    * `opts` - forwarded to `c:Ecto.Repo.delete_all/2`.

  See also `delete_many/3` and `EctoShorts.CommonFilters`.
  """
  def delete_all(queryable, params \\ %{}, opts \\ []) do
    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).delete_all(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Creates many records in a single transaction.

  Each record is inserted individually inside an `Ecto.Multi`. Any
  failure rolls back the entire transaction.

  Returns `{:ok, [struct]}` or `{:error, reason}`.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `params_list` - a list of attribute maps.
    * `opts` - shared options.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.create_many(EctoShorts.Schema.Post, [
        %{title: "Post 1", body: "Body 1"},
        %{title: "Post 2", body: "Body 2"}
      ])

  See also `create/3`, `insert_all/3`, and `transact/2`.
  """
  def create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> Multi.build_create_many_multi(params_list, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Finds many records in a single transaction.

  Each lookup runs inside an `Ecto.Multi`. A `nil` result rolls back
  the transaction with a `:not_found` error.

  Returns `{:ok, [struct]}` or `{:error, reason}`.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `params_list` - a list of filter param maps.
    * `opts` - shared options.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.find_many(EctoShorts.Schema.Post, [%{id: 1}, %{id: 2}])

  See also `find/3`, `find_or_create_many/3`, and `transact/2`.
  """
  def find_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> Multi.build_find_many_multi(params_list, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Updates many records in a single transaction.

  Entries can be `{find_params, update_params}` tuples or maps with an
  `:id` key. Raises `ArgumentError` for unrecognized shapes.

  Returns `{:ok, [struct]}` or `{:error, reason}`.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `entries` - a list of `{find_params, update_params}` tuples or
      maps with an `:id` key.
    * `opts` - shared options.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.update_many(EctoShorts.Schema.Post, [
        {%{id: 1}, %{title: "Updated 1"}},
        {%{id: 2}, %{title: "Updated 2"}}
      ])

  See also `update/4`, `find_and_update/4`, and `update_all/4`.
  """
  def update_many(schema, entries, opts \\ []) when is_list(entries) do
    schema
    |> Multi.build_update_many_multi(entries, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Deletes many records in a single transaction.

  Entries can be structs, filter param maps, or raw id values.

  Returns `{:ok, [struct]}` or `{:error, reason}`.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `records` - a list of structs, param maps, or id values.
    * `opts` - shared options.

  ## Examples

      {:ok, deleted} = EctoShorts.Actions.delete_many(EctoShorts.Schema.Post, [post1, post2])

  See also `delete/1`, `delete_all/3`, and `transact/2`.
  """
  def delete_many(schema, records, opts \\ []) when is_list(records) do
    schema
    |> Multi.build_delete_many_multi(records, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Finds or creates many records in a single transaction.

  For each entry, finds a matching record or creates one from the
  same params.

  Returns `{:ok, [struct]}` or `{:error, reason}`.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `params_list` - a list of attribute maps.
    * `opts` - shared options.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.find_or_create_many(EctoShorts.Schema.Post, [
        %{title: "Post 1", body: "Body 1"},
        %{title: "Post 2", body: "Body 2"}
      ])

  See also `find_or_create/3`, `create_many/3`, and `find_many/3`.
  """
  def find_or_create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    schema
    |> Multi.build_find_or_create_multi(params_list, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  @doc group: "Multi"
  @doc since: "3.0.0"
  @doc """
  Finds and upserts many records in a single transaction.

  Entries can be `{find_params, upsert_params}` tuples or maps with an
  `:id` key. Raises `ArgumentError` for unrecognized shapes.

  Returns `{:ok, [struct]}` or `{:error, reason}`.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `entries` - a list of `{find_params, upsert_params}` tuples or
      maps with an `:id` key.
    * `opts` - shared options.

  ## Examples

      {:ok, posts} = EctoShorts.Actions.find_and_upsert_many(EctoShorts.Schema.Post, [
        {%{id: 1}, %{title: "Updated"}},
        {%{title: "New"}, %{body: "New body"}}
      ])

  See also `find_and_upsert/4`, `update_many/3`, and `find_or_create_many/3`.
  """
  def find_and_upsert_many(schema, entries, opts \\ []) when is_list(entries) do
    schema
    |> Multi.build_upsert_multi(entries, opts)
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  defp do_delete(schema_data, schema, opts) do
    with {:error, failed_changeset} <-
           schema
           |> CommonSchema.create_changeset(schema_data, opts)
           |> Config.repo!(opts).delete(opts) do
      {:error,
       Error.call(
         :conflict,
         "failed to delete record.",
         %{
           schema: schema,
           changeset: failed_changeset
         },
         opts
       )}
    end
  end

  defp put_param(enum, opts, key) do
    case Keyword.get(opts, key) do
      nil ->
        enum

      value ->
        if is_map(enum) do
          Map.put(enum, key, value)
        else
          enum ++ [{key, value}]
        end
    end
  end

  defp normalize_transaction_response(result, opts) do
    unwrap? = Keyword.get(opts, :strict, true)

    case {unwrap?, result} do
      {_, {:error, :error}} ->
        :error

      {_, {:ok, :ok}} ->
        :ok

      {true, {:ok, {:error, _} = error}} ->
        error

      {true, {:ok, {:ok, _} = response}} ->
        response

      {_, other} ->
        other
    end
  end

  defp eval_transaction_fun(fun, repo, opts) do
    response = call_transaction_fun(fun, repo)

    if Keyword.get(opts, :strict, true) do
      maybe_rollback(response, repo)
    else
      response
    end
  end

  defp call_transaction_fun(fun, repo) when is_function(fun, 1), do: fun.(repo)
  defp call_transaction_fun(fun, _repo) when is_function(fun, 0), do: fun.()

  defp maybe_rollback(:error, repo), do: repo.rollback(:error)
  defp maybe_rollback({:error, reason}, repo), do: repo.rollback(reason)
  defp maybe_rollback({:ok, value}, _repo), do: value
  defp maybe_rollback(term, _repo), do: term
end
