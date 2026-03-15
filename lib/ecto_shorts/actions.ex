defmodule EctoShorts.Actions do
  @moduledoc """
  Public data-operation boundary for the EctoShorts API.

  `EctoShorts.Actions` gives you a data-driven interface for reading,
  creating, updating, deleting, batching, and transacting records without
  having to write every `Ecto.Query` or changeset call by hand.

  The module owns the caller-facing action API. It delegates query building to
  `EctoShorts.CommonFilters`, changeset creation to schema-oriented helpers,
  and family-specific implementation details to the adjacent
  `EctoShorts.Actions.*` modules.

  Use this module when you want to:

  * work with a consistent public API over common `Ecto.Repo` operations
  * express reads as filter params instead of Ecto macros
  * batch related lookups by one or more keys
  * run multi-step write flows with rollback on failure
  * choose between single-record, bulk, and transactional helpers without
    switching to a different calling style

  ## Public families

  The API is organized into five groups:

  * **CRUD** - `preload/3`, `exists?/3`, `all/1-3`, `create/3`, `get/3`,
    `find/3`, `update/4`, `delete/1-3`, `stream/3`, `aggregate/5`, and the
    `find_*` wrappers
  * **Bulk** - `insert_all/3`, `update_all/4`, `delete_all/3`
  * **Multi** - `create_many/3`, `find_many/3`, `update_many/3`,
    `delete_many/3`, `find_or_create_many/3`, `find_and_upsert_many/3`
  * **Batch** - `batch/5`, `batch_find/4`
  * **Transaction** - `transaction/2`, `transact/2`

  ## Shared conventions

  A few rules apply across the module:

  * **Read helpers use `CommonFilters`.** Functions such as `exists?/3`,
    `all/3`, `find/3`, `stream/3`, and `aggregate/5` accept the public
    `EctoShorts.CommonFilters` language.
  * **Reads use `:replica`; writes use `:repo`.** `get/3`, `find/3`,
    `exists?/3`, and `aggregate/5` resolve the configured replica repo.
    `create/3`, `update/4`, `delete/1-3`, bulk helpers, and multi helpers
    resolve the configured write repo.
  * **`:preload` is shared across many boundaries.** `get/3`, `find/3`,
    `create/3`, `update/4`, the `find_*` wrappers, batch helpers, and the
    multi helpers all support post-operation preloading through `opts`.
  * **Bulk and Multi are different tradeoffs.** Bulk helpers execute one repo
    operation against a prepared query or insert set. Multi helpers compose
    several per-record operations inside an `Ecto.Multi` transaction.

  ## Choosing the right function

  | Use case | Preferred helper |
  | --- | --- |
  | Read one record by primary key and allow `nil` | `get/3` |
  | Read one record by filter params and get an explicit error on miss | `find/3` |
  | Read many records | `all/3` |
  | Check whether any row matches | `exists?/3` |
  | Run a count, sum, avg, min, or max | `aggregate/5` |
  | Create one record with changeset validation | `create/3` |
  | Update one record by id or struct | `update/4` |
  | Delete one record, a changeset, or a list of records | `delete/1-3` |
  | Bulk insert, update, or delete with repo-native row counts | `insert_all/3`, `update_all/4`, `delete_all/3` |
  | Run many per-record operations atomically | `create_many/3`, `find_many/3`, `update_many/3`, `delete_many/3`, `find_or_create_many/3`, `find_and_upsert_many/3` |
  | Reuse keyed lookups across many entries | `batch/5`, `batch_find/4` |
  | Wrap work in a raw transaction | `transaction/2` |
  | Wrap work in a normalized transaction API | `transact/2` |

  ## Return shapes

  The helpers intentionally return different shapes depending on what they do:

  * **Single-record CRUD helpers** usually return `{:ok, struct}` or
    `{:error, term()}`.
  * **`get/3`** returns `struct | nil`.
  * **`all/1-3`** returns a plain list.
  * **Bulk update/delete helpers** return `{count, nil}`.
  * **`insert_all/3`** returns `{:ok, {count, nil | rows}}` or
    `{:error, [changeset, ...]}`.
  * **Multi helpers** return `{:ok, list}` or `{:error, reason}` after
    rollback on the first failure.
  * **`transaction/2`** preserves the raw transaction shape.
  * **`transact/2`** normalizes function and `Ecto.Multi` transaction results.

  By default, caller-facing error payloads come from
  `EctoShorts.Actions.ErrorMessage`. You can replace that shape by configuring
  a different `EctoShorts.Actions.Error` implementation.

  ## Examples

      alias EctoShorts.Actions
      alias EctoShorts.Schema.Post

      {:ok, post} = Actions.create(Post, %{title: "Hello", body: "World"})

      {:ok, post} = Actions.find(Post, %{id: 1})

      posts = Actions.all(Post, %{published: true, order_by: [desc: :inserted_at]})

      {:ok, post} = Actions.update(Post, post, %{title: "Updated"})

      {:ok, deleted} = Actions.delete(post)

      grouped = Actions.batch(Post, [%{title: "Hello"}], :title, :one)

  ## Important behaviors

  * `find/3` returns `{:error, ...}` immediately when called with `%{}` and a
    non-query source. This prevents accidental "give me any row" lookups.
  * `stream/3` returns an enumerable that must be consumed inside a
    transaction.
  * `update/4` can apply optimistic locking either from `opts` or from a
    schema-level `optimistic_lock/0` callback.
  * `transaction/2` and `transact/2` are intentionally different:
    `transaction/2` preserves the repo transaction result, while `transact/2`
    reshapes it into the higher-level Actions contract.

  ## Shared options

  The exact supported options vary by function family, but these are the most
  important shared caller-facing keys:

  * `:repo` - write repo override
  * `:replica` - read repo override
  * `:changeset` - override the schema's default changeset callback
  * `:dynamic_adapter` - dynamic-expression adapter override used by
    `EctoShorts.CommonFilters`
  * `:error_module` - custom error payload adapter
  * `:preload` - post-operation preloads for the helpers that return structs
  * `:optimistic_lock` - lock field, `{field, incrementer}`, or `false` for
    update boundaries

  See `EctoShorts.CommonFilters`, `EctoShorts.Config`,
  `EctoShorts.CommonSchema`, and `EctoShorts.Actions.Error`.
  """

  @moduledoc groups: [
               %{title: "CRUD", description: "Single-record create, read, update, and delete operations."},
               %{title: "Bulk", description: "Multi-row operations without transactions."},
               %{title: "Multi", description: "Transactional multi-record operations using Ecto.Multi."},
               %{title: "Batch", description: "Keyed grouping and lookup helpers."},
               %{title: "Transaction", description: "Transaction wrappers."}
             ]

  alias Ecto.Changeset

  alias EctoShorts.Actions.Batch
  alias EctoShorts.Actions.Bulk
  alias EctoShorts.Actions.Error
  alias EctoShorts.Actions.Multi
  alias EctoShorts.Actions.Transaction

  alias EctoShorts.{
    Config,
    CommonFilters,
    CommonSchema
  }

  @typedoc """
  Query source accepted by the public read helpers.

  This is usually a schema module, an `{source, schema}` tuple, or a prebuilt
  `Ecto.Query.t()`.
  """
  @type queryable :: module() | {binary(), module()} | Ecto.Query.t()

  @typedoc """
  Public parameter container used by the Actions API.

  Read helpers interpret these values with `EctoShorts.CommonFilters`. Write
  helpers interpret them as attribute maps or keyword lists.
  """
  @type params :: map() | keyword()

  @typedoc """
  Keyword options accepted by Actions helpers.
  """
  @type opts :: keyword()

  @typedoc """
  Primary-key shape accepted by the id-based helpers.
  """
  @type id :: integer() | binary()

  @typedoc """
  Grouping shape used by `batch/5`.
  """
  @type cardinality :: :one | :many

  @cardinalities [:one, :many]

  @doc group: "CRUD"
  @doc since: "3.0.0"
  @doc """
  Preloads associations on the given struct or list of structs.

  Delegates to `c:Ecto.Repo.preload/3` on the configured replica repo.
  All options are forwarded directly to `c:Ecto.Repo.preload/3`.

  ## Arguments

    * `data` - a struct or list of structs to preload.
    * `preloads` - the associations to preload. Accepts the same shapes as
      `c:Ecto.Repo.preload/3`:
      * an atom - `:author`
      * a list of atoms - `[:author, :comments]`
      * a keyword list for nested preloads - `[author: :profile]`
      * an `{assoc, query}` tuple to preload with a custom query -
        `{:comments, from(c in Comment, where: c.approved == true)}`
    * `opts` - forwarded to `c:Ecto.Repo.preload/3`. Common options:
      * `:force` - reload even if already loaded.
      * `:in_parallel` - whether to run preloads in parallel.
      * `:prefix` - the query prefix.

  ## Examples

      post_with_author = EctoShorts.Actions.preload(post, :author)
      posts_with_tags  = EctoShorts.Actions.preload(posts, [:author, :comments])

      # Nested preload
      post = EctoShorts.Actions.preload(post, author: :profile)

      # Preload with a custom query
      post = EctoShorts.Actions.preload(post, {:comments, from(c in Comment, where: c.approved == true)})

  See `c:Ecto.Repo.preload/3` for the full list of supported options.
  See also `all/3` and `EctoShorts.CommonChanges.preload_change_assoc/3`.
  """
  @spec preload(struct() | list(term()), term(), opts) :: struct() | list(term())
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
  @spec exists?(queryable, params, opts) :: boolean()
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
  @spec all(queryable) :: list(term())
  def all(queryable) do
    all(queryable, %{}, [])
  end

  @doc group: "CRUD"
  @doc """
  Fetches all records using either a params map or the keyword shorthand form.

  When the second argument is a map it is used as filter params and
  forwarded to `all/3` with an empty opts list.

  When it is a keyword list, `:repo`, `:replica`, and
  `:dynamic_adapter` are extracted as options; every other key is
  treated as a filter param and passed to `all/3`.

  This shorthand is best when you want a filter-only keyword list such as
  `[published: true, limit: 10]`. Use `all/3` when you want to separate
  query params from runtime options explicitly.

  Raises `ArgumentError` when the second argument is neither a map
  nor a keyword list.

  ## Arguments

    * `queryable` - a schema module, `{source, schema}` tuple, or
      `Ecto.Query`.
    * `params` - a map of filter params, or a keyword list where
      `:repo`, `:replica`, and `:dynamic_adapter` are options and all
      other keys are filter params.

  ## Examples

      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, %{published: true})
      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, replica: MyApp.Repo)
      posts = EctoShorts.Actions.all(EctoShorts.Schema.Post, [published: true, limit: 10])

  See also `all/1`, `all/3`, and `find/3`.
  """
  @spec all(queryable, params | opts) :: list(term())
  def all(queryable, params) when is_map(params) and not is_struct(params) do
    all(queryable, params, [])
  end

  def all(queryable, opts) do
    if Keyword.keyword?(opts) do
      params = Keyword.drop(opts, [:repo, :replica, :dynamic_adapter])
      all(queryable, params, Keyword.take(opts, [:repo, :replica, :dynamic_adapter]))
    else
      raise ArgumentError, "Expected the options parameter to be a keyword list, got: #{inspect(opts)}"
    end
  end

  @doc group: "CRUD"
  @doc """
  Fetches all records matching `params`.

  This is the main list-read boundary in the module. It merges selected
  convenience options into `params`, builds an `Ecto.Query` with
  `EctoShorts.CommonFilters`, runs `c:Ecto.Repo.all/2`, and optionally
  post-preloads the returned structs.

  ## Arguments

    * `queryable` - a schema module, `{source, schema}` tuple, or
      `Ecto.Query`.
    * `params` - filter params (see `EctoShorts.CommonFilters`).
    * `opts` - keyword list of options.

  ## Options

  The following options are consumed before query building and are
  **not** forwarded to `c:Ecto.Repo.all/2`:

    * `:order_by` - merged into `params` before building the query.
    * `:group_by` - merged into `params` before building the query.
    * `:preload` - associations to preload on the returned structs. Accepts
      the same shapes as `preload/3`: an atom, list of atoms, keyword list
      for nested preloads, or `{assoc, query}` tuple. Applied after the
      query completes.

  All other options are forwarded to `c:Ecto.Repo.all/2`.

  ## Examples

      posts =
        EctoShorts.Actions.all(
          EctoShorts.Schema.Post,
          %{published: true},
          order_by: :title,
          preload: [:comments]
        )

  See also `find/3`, `stream/3`, and `EctoShorts.CommonFilters`.
  """
  @spec all(queryable, params, opts) :: list(term())
  def all(queryable, params, opts) do
    params =
      params
      |> put_param(opts, :order_by)
      |> put_param(opts, :group_by)

    queryable
    |> CommonFilters.convert_params_to_filter(params, opts)
    |> Config.repo!(opts).all(opts)
    |> maybe_preload(opts)
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

  ## Options

    * `:preload` - associations to preload on the created struct. Accepts
      the same shapes as `preload/3`. Applied after the insert completes.

  ## Examples

      iex> EctoShorts.Actions.create(EctoShorts.Schema.Post, %{title: "Hello", body: "World"}, repo: EctoShorts.Repo)
      {:ok, %EctoShorts.Schema.Post{title: "Hello", body: "World", ...}}

  See also `find/3`, `update/4`, and `EctoShorts.CommonChanges`.
  """
  @spec create(module(), params, opts) :: {:ok, struct()} | {:error, term()}
  def create(schema, params, opts \\ []) do
    schema
    |> CommonSchema.create_changeset(params, opts)
    |> Config.repo!(opts).insert(opts)
    |> handle_response_preload(opts)
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

  ## Options

    * `:preload` - associations to preload on the returned struct. Accepts
      the same shapes as `preload/3`. Returns `nil` unchanged when the
      record is not found.

  ## Examples

      post = EctoShorts.Actions.get(EctoShorts.Schema.Post, 1)

  See also `find/3` and `all/3`.
  """
  @spec get(queryable, id, opts) :: struct() | nil
  def get(queryable, id, opts \\ []) do
    Config.replica!(opts).get(queryable, id, opts)
    |> maybe_preload(opts)
  end

  @doc group: "CRUD"
  @doc """
  Finds a single record matching `params`.

  Returns `{:ok, struct}` when a record is found, or
  `{:error, %ErrorMessage{code: :not_found}}` otherwise. When `params`
  is an empty map and `queryable` is not an `Ecto.Query`, the error is
  returned immediately without querying.

  `find/3` uses `c:Ecto.Repo.one/2`, so callers should pass filters that
  identify at most one row.

  ## Arguments

    * `queryable` - a schema module or queryable.
    * `params` - filter params (see `EctoShorts.CommonFilters`).
    * `opts` - keyword list of options.

  ## Options

    * `:order_by` - merged into `params` before query building.
    * `:group_by` - merged into `params` before query building.
    * `:preload` - associations to preload on the found struct. Accepts
      the same shapes as `preload/3`. Applied after the record is located.

  ## Examples

      iex> EctoShorts.Actions.find(EctoShorts.Schema.Post, %{id: 1})
      {:ok, %EctoShorts.Schema.Post{id: 1, ...}}

      iex> EctoShorts.Actions.find(EctoShorts.Schema.Post, %{})
      {:error, %ErrorMessage{code: :not_found, message: "record not found."}}

  See also `all/3`, `create/3`, and `find_or_create/3`.
  """
  @spec find(queryable, params, opts) :: {:ok, struct()} | {:error, term()}
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
        {:ok, maybe_preload(record, opts)}
    end
  end

  @doc group: "CRUD"
  @doc """
  Updates a record by id or by struct.

  When `id_or_schema_struct` is an integer or binary, the record is
  fetched with `find/3` first. When it is a struct, the changeset is
  built and updated directly.

  Returns `{:ok, struct}`, `{:error, changeset}`,
  `{:error, %ErrorMessage{code: :not_found}}`, or
  `{:error, %ErrorMessage{code: :stale}}` when optimistic locking
  detects a concurrent modification.

  ## Arguments

    * `queryable` - the Ecto schema module.
    * `id_or_schema_struct` - a primary key value or an existing struct.
    * `params` - a map of attributes to update.
    * `opts` - forwarded to `c:Ecto.Repo.update/2`.

  ## Options

    * `:optimistic_lock` - an atom (lock field name), a
      `{field, incrementer}` tuple, or `false`. When not set,
      auto-detects by checking if the schema exports
      `optimistic_lock/0`. See the "Shared options" section for
      details.
    * `:preload` - associations to preload on the updated struct. Accepts
      the same shapes as `preload/3`. Applied after the update completes.

  ## Optimistic locking

  When optimistic locking is active, the changeset is piped through
  `Ecto.Changeset.optimistic_lock/3` before calling `c:Ecto.Repo.update/2`.
  If the record has been modified by another process since it was fetched,
  `Ecto.StaleEntryError` is rescued and converted to
  `{:error, %ErrorMessage{code: :stale}}`.

  To enable locking, either define `optimistic_lock/0` on your schema:

      defmodule MyApp.Post do
        def optimistic_lock, do: :lock_version
      end

  Or pass the option explicitly:

      Actions.update(Post, post, params, optimistic_lock: :lock_version)

  ## Examples

      {:ok, post} =
        EctoShorts.Actions.update(
          EctoShorts.Schema.Post,
          1,
          %{title: "New"},
          repo: EctoShorts.Repo
        )

      {:ok, post} = EctoShorts.Actions.update(EctoShorts.Schema.Post, post, %{title: "New"})

  See also `find_and_update/4`, `create/3`, and `EctoShorts.CommonChanges`.
  """
  @spec update(module(), id | struct(), params, opts) :: {:ok, struct()} | {:error, term()}
  def update(queryable, id_or_schema_struct, params, opts \\ [])

  def update(queryable, id, params, opts) when is_integer(id) or is_binary(id) do
    with {:ok, record} <- find(queryable, %{id: id}, opts) do
      update(queryable, record, params, opts)
    end
  end

  def update(queryable, schema_struct, params, opts) do
    changeset =
      queryable
      |> CommonSchema.create_changeset(schema_struct, params, opts)
      |> maybe_apply_optimistic_lock(queryable, opts)

    Config.repo!(opts).update(changeset, opts)
    |> handle_response_preload(opts)
  rescue
    Ecto.StaleEntryError ->
      {:error,
       Error.call(
         :stale,
         "record has been modified by another process.",
         %{
           schema: CommonSchema.get_schema(queryable),
           struct: schema_struct
         },
         opts
       )}
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
  @spec delete(struct() | Ecto.Changeset.t() | [struct() | Ecto.Changeset.t()]) ::
          {:ok, struct() | list(term())} | {:error, term()}
  def delete(data) do
    delete(data, [])
  end

  @doc group: "CRUD"
  @doc """
  Deletes a struct, changeset, or list of either with options.

  When `data` is a list, entries are deleted sequentially and the function
  stops on the first error. Earlier successful deletes are not rolled back.

  ## Arguments

    * `data` - a struct, `Ecto.Changeset`, or list of either.
    * `opts` - forwarded to `c:Ecto.Repo.delete/2`.

  See `delete/1` for return values. See also `delete/3` and
  `find_and_delete/3`.
  """
  @spec delete(queryable | struct() | Ecto.Changeset.t() | [struct() | Ecto.Changeset.t()], id | opts) ::
          {:ok, struct() | list(term())} | {:error, term()}
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
  @spec delete(queryable, id, opts) :: {:ok, struct()} | {:error, term()}
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

  Wraps `c:Ecto.Repo.stream/2` with filter support. The stream must be
  consumed inside a transaction (see `transact/2` or `transaction/2`).

  ## Arguments

  * `queryable` - a schema module or queryable.
  * `params` - filter params (see `EctoShorts.CommonFilters`).
  * `opts` - forwarded to `c:Ecto.Repo.stream/2`.

  ## Options

  * `:max_rows` (default: `500`) - the number of rows to fetch from the
    database per batch. Increase for throughput, decrease for memory.
  * `:repo` - the `Ecto.Repo` to use. Defaults to `EctoShorts.Config.repo/0`.

  ## Examples

      # Basic streaming inside a transaction
      EctoShorts.Actions.transact(fn ->
        EctoShorts.Schema.Post
        |> EctoShorts.Actions.stream(%{published: true})
        |> Stream.each(&process_post/1)
        |> Stream.run()
      end)

      # Custom chunk size for large datasets
      EctoShorts.Actions.transact(fn ->
        EctoShorts.Schema.Post
        |> EctoShorts.Actions.stream(%{}, max_rows: 1000)
        |> Stream.each(&process_post/1)
        |> Stream.run()
      end)

  See also `all/3`, `transact/2`, and `EctoShorts.CommonFilters`.
  """
  @spec stream(queryable, params, opts) :: Enumerable.t()
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
  @spec aggregate(queryable, params, atom(), atom(), opts) :: term()
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

  ## Options

    * `:preload` - associations to preload on the result struct. Accepts the
      same shapes as `preload/3`.

  See also `find_or_create/3`, `find_and_update/4`, and `create/3`.
  """
  @spec find_and_create(module(), params, params, opts) :: {:ok, struct()} | {:error, term()}
  def find_and_create(queryable, find_params, create_params, opts \\ []) do
    with {:error, _} <- find(queryable, find_params, Keyword.delete(opts, :preload)) do
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

  ## Options

    * `:preload` - associations to preload on the updated struct. Accepts the
      same shapes as `preload/3`.

  See also `update/4`, `find_and_upsert/4`, and `find/3`.
  """
  @spec find_and_update(module(), params, params, opts) :: {:ok, struct()} | {:error, term()}
  def find_and_update(source, find_params, update_params, opts \\ []) do
    with {:ok, record} <- find(source, find_params, Keyword.delete(opts, :preload)) do
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

  ## Options

    * `:preload` - associations to preload on the result struct. Accepts the
      same shapes as `preload/3`.

  See also `find_and_update/4`, `find_or_create/3`, and `create/3`.
  """
  @spec find_and_upsert(module(), params, params, opts) :: {:ok, struct()} | {:error, term()}
  def find_and_upsert(source, find_params, upsert_params, opts \\ []) do
    case find(source, find_params, Keyword.delete(opts, :preload)) do
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
  @spec find_and_delete(module(), params, opts) :: {:ok, struct()} | {:error, term()}
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

  ## Options

    * `:preload` - associations to preload on the result struct. Accepts the
      same shapes as `preload/3`. Applied once after the operation completes,
      regardless of whether a record was found or created.

  ## Notes

  `:preload` is intentionally withheld from the internal `find/3` call and
  applied to the final result instead. This ensures a single preload pass
  covers both the found path and the created path, rather than preloading
  during the lookup and then discarding that work on the create path.

  See also `find_and_create/4`, `find_or_create_many/3`, and `create/3`.
  """
  @spec find_or_create(module(), params, opts) :: {:ok, struct()} | {:error, term()}
  def find_or_create(source, params, opts \\ []) do
    result =
      with {:error, _} <-
             find(
               source,
               Map.take(params, CommonSchema.get_query_fields(opts, source)),
               Keyword.delete(opts, :preload)
             ) do
        source
        |> CommonSchema.get_schema_source()
        |> create(params, opts)
      end

    handle_response_preload(result, opts)
  end

  @doc group: "Transaction"
  @doc since: "3.0.0"
  @doc """
  Runs `fun_or_multi` in a transaction without further result normalization.

  This is the lower-level transaction helper in the Actions API. Use it when
  you want the repo transaction shape preserved. In particular, a function that
  returns `{:ok, value}` will produce `{:ok, {:ok, value}}`.

  ## Arguments

    * `fun_or_multi` - a function or `Ecto.Multi`.
    * `opts` - forwarded to `c:Ecto.Repo.transaction/2`.

  ## Examples

      {:ok, _} = EctoShorts.Actions.transaction(fn ->
        EctoShorts.Actions.create(EctoShorts.Schema.Post, %{title: "Hello"})
      end)

  See also `transact/2` and `create_many/3`.
  """
  @spec transaction((... -> term()) | Ecto.Multi.t(), opts) :: {:ok, term()} | {:error, term()}
  def transaction(fun_or_multi, opts \\ []) do
    Transaction.run_transaction(fun_or_multi, opts)
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

  This is the higher-level transaction boundary to prefer when you want your
  caller-facing code to stay inside the usual Actions success/error contract.

  ## Arguments

    * `fun_or_multi` - a function or `Ecto.Multi`.
    * `opts` - keyword list of options.

  ## Options

    * `:strict` (default: `true`) - roll back on `{:error, reason}`
      and unwrap `{:ok, value}`.

  See also `transaction/2` and `create_many/3`.
  """
  @spec transact((... -> term()) | Ecto.Multi.t(), opts) :: {:ok, term()} | {:error, term()}
  def transact(fun_or_multi, opts \\ [])

  def transact(%Ecto.Multi{} = multi, opts) do
    multi
    |> transaction(opts)
    |> Multi.handle_multi_response(opts)
  end

  def transact(fun, opts) when is_function(fun) do
    fn repo -> Transaction.eval_transaction_fun(fun, repo, opts) end
    |> transaction(opts)
    |> Transaction.normalize_transaction_response(opts)
  end

  @doc group: "Batch"
  @doc since: "3.0.0"
  @doc """
  Batches records by key(s) and cardinality.

  Returns a map keyed by batch key value (or a map of values for
  composite keys). Each value is a struct (`:one`) or list of structs
  (`:many`). Returns `%{}` when `params` is empty or when none of the
  provided entries contain the requested batch key(s).

  Raises `ArgumentError` when `:one` cardinality finds multiple records
  for a single batch key.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `params` - a list of param maps to batch.
    * `batch_keys` - an atom or list of atoms. Defaults to `:id`.
    * `cardinality` - `:one` or `:many`. Defaults to `:many`.
    * `opts` - shared options.

  ## Options

    * `:preload` - associations to preload on each value in the returned map.
      Accepts the same shapes as `preload/3`. Applied after grouping completes.

  ## Examples

      # Group posts by author_id (many per key)
      EctoShorts.Actions.batch(
        Post,
        [%{author_id: 1}, %{author_id: 2}],
        :author_id,
        :many
      )
      # %{1 => [%Post{...}], 2 => [%Post{...}]}

      # Fetch one post per id
      EctoShorts.Actions.batch(Post, [%{id: 1}, %{id: 2}], :id, :one)
      # %{1 => %Post{...}, 2 => %Post{...}}

      # Composite key batch
      EctoShorts.Actions.batch(PostTag, [%{post_id: 1, tag_id: 5}], [:post_id, :tag_id], :one)
      # %{%{post_id: 1, tag_id: 5} => %PostTag{...}}
  """
  @spec batch(module(), list(params()), atom() | list(atom()), cardinality, opts) :: map()
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
        |> Map.new(fn {k, v} -> {k, maybe_preload(v, opts)} end)
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
      |> Map.new(fn {k, v} -> {k, maybe_preload(v, opts)} end)
    end
  end

  @doc group: "Batch"
  @doc since: "3.0.0"
  @doc """
  Batch-fetches records and zips them into the original entries.

  Looks up records by `keys` using `batch/5` with `:one` cardinality.
  Each matched record is zipped into the corresponding entry while preserving
  the original list order.

  The live supported entry shapes include:

  * `{struct, params}` tuples, which are passed through unchanged
  * `{lookup_params, params}` tuples, which become `{resolved_struct, params}`
  * bare maps or keyword lists, which become `{resolved_struct, original_params}`
  * `nil`, which is preserved unchanged

  Unmatched entries are returned unchanged.

  ## Arguments

    * `schema` - the Ecto schema module.
    * `entries` - a list of maps or keyword lists.
    * `keys` - an atom or list of atoms identifying the lookup fields.
    * `opts` - shared options.

  ## Examples

      entries = [%{permalink: "existing", title: "Updated"}]
      [{post, ^entries |> hd()}] =
        EctoShorts.Actions.batch_find(EctoShorts.Schema.Post, entries, :permalink)
  """
  @spec batch_find(module(), [map()], atom() | list(atom()), opts) :: [map()]
  def batch_find(schema, entries, keys, opts \\ []) do
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

  This helper prepares the insert set first, then performs one
  `c:Ecto.Repo.insert_all/3` call. By default, each entry is validated through
  the schema's `changeset/2` before the repo call. Validation can be skipped
  with `validate: false`.

  The accepted entry shapes come from `EctoShorts.CommonParams` and include
  maps, keyword lists, structs, changesets, and `{struct, params}` tuples.

  When `:batch_find` is set, entries are first resolved through `batch_find/4`
  before the insert payload is built.

  Returns `{:ok, {count, nil | [struct]}}` or
  `{:error, [changeset]}`.

  ## Arguments

    * `source` - the Ecto schema module.
    * `params_list` - a list of maps, keyword lists, structs, `{struct, params}` tuples, or changesets.
    * `opts` - keyword list of options.

  ## Options

    * `:batch_find` - atom or list of atoms. Batch-fetches matching
      records before insertion using `batch_find/4`.
    * `:validate` - set to `false` to skip changeset validation.
    * `:on_conflict_replace` - controls conflict resolution:
      `:none` (insert or do nothing), `:insert_keys` (default, replace
      all non-primary-key fields), or a list of field atoms to replace.
    * `:on_conflict` - Ecto-native conflict action, forwarded directly
      to `c:Ecto.Repo.insert_all/3`. Overrides `:on_conflict_replace`
      when both are set.
    * `:conflict_target` - Ecto-native conflict target, forwarded
      directly to `c:Ecto.Repo.insert_all/3`.

  When at least one prepared insert contains all primary-key fields and the
  caller does not provide an explicit `:on_conflict`, the default conflict
  options are derived from the schema primary key and
  `:on_conflict_replace`.

  See `EctoShorts.CommonParams.convert_to_insert_params/3` for
  timestamp and validation options, and
  `EctoShorts.CommonParams.build_on_conflict_options/3` for conflict
  resolution details.
  """
  @spec insert_all(module() | {binary(), module()}, list(term()), opts()) ::
          {:ok, {non_neg_integer(), nil | list(term())}} | {:error, term()}
  def insert_all(source, params_list, opts \\ []) do
    params_list =
      if Keyword.has_key?(opts, :batch_find) do
        batch_find(source, params_list, opts[:batch_find], opts)
      else
        params_list
      end

    Bulk.insert_all(source, params_list, opts)
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
    * `update_params` - a map of update operations. Each value can be a plain
      value (`:set` implied), or a tagged tuple: `{:inc, n}`, `{:push, v}`,
      `{:pull, v}`. See `EctoShorts.CommonParams.convert_to_update_params/3`.
    * `opts` - forwarded to `c:Ecto.Repo.update_all/3`.

  ## Examples

      # Set title for all drafts
      EctoShorts.Actions.update_all(Post, %{published: false}, %{title: "Draft"})
      # {5, nil}

      # Increment views for a specific post
      EctoShorts.Actions.update_all(Post, %{id: 1}, %{views: {:inc, 1}})
      # {1, nil}

  See also `EctoShorts.CommonParams.convert_to_update_params/3`
  and `update_many/3`.
  """
  @spec update_all(module() | {binary(), module()}, params(), params(), opts()) :: {non_neg_integer(), nil}
  def update_all(source, find_params, update_params, opts \\ []) do
    Bulk.update_all(source, find_params, update_params, opts)
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

  ## Examples

      # Delete all unpublished posts
      EctoShorts.Actions.delete_all(Post, %{published: false})
      # {3, nil}

      # Delete all records
      EctoShorts.Actions.delete_all(Post)
      # {42, nil}

  See also `delete_many/3` and `EctoShorts.CommonFilters`.
  """
  @spec delete_all(queryable(), params(), opts()) :: {non_neg_integer(), nil}
  def delete_all(queryable, params \\ %{}, opts \\ []) do
    Bulk.delete_all(queryable, params, opts)
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

  ## Options

    * `:preload` - associations to preload on every struct in the result list.
      Accepts the same shapes as `preload/3`. Applied after the transaction.

  See also `create/3`, `insert_all/3`, and `transact/2`.
  """
  @spec create_many(module(), list(params()), opts()) :: {:ok, list(term())} | {:error, term()}
  def create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    run_multi(Multi.build_create_many_multi(schema, params_list, opts), opts)
    |> handle_response_preload(opts)
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

  ## Options

    * `:preload` - associations to preload on every struct in the result list.
      Accepts the same shapes as `preload/3`. Applied after the transaction.

  See also `find/3`, `find_or_create_many/3`, and `transact/2`.
  """
  @spec find_many(module(), list(params()), opts()) :: {:ok, list(term())} | {:error, term()}
  def find_many(schema, params_list, opts \\ []) when is_list(params_list) do
    run_multi(Multi.build_find_many_multi(schema, params_list, opts), opts)
    |> handle_response_preload(opts)
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

  ## Options

    * `:preload` - associations to preload on every struct in the result list.
      Accepts the same shapes as `preload/3`. Applied after the transaction.

  See also `update/4`, `find_and_update/4`, and `update_all/4`.
  """
  @spec update_many(module(), list(term()), opts()) :: {:ok, list(term())} | {:error, term()}
  def update_many(schema, entries, opts \\ []) when is_list(entries) do
    run_multi(Multi.build_update_many_multi(schema, entries, opts), opts)
    |> handle_response_preload(opts)
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

  ## Options

    * `:preload` - associations to preload on every struct in the result list.
      Accepts the same shapes as `preload/3`. Applied after the transaction.

  See also `delete/1`, `delete_all/3`, and `transact/2`.
  """
  @spec delete_many(module(), list(term()), opts()) :: {:ok, list(term())} | {:error, term()}
  def delete_many(schema, records, opts \\ []) when is_list(records) do
    run_multi(Multi.build_delete_many_multi(schema, records, opts), opts)
    |> handle_response_preload(opts)
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

  ## Options

    * `:preload` - associations to preload on every struct in the result list.
      Accepts the same shapes as `preload/3`. Applied after the transaction.

  See also `find_or_create/3`, `create_many/3`, and `find_many/3`.
  """
  @spec find_or_create_many(module(), list(params()), opts()) :: {:ok, list(term())} | {:error, term()}
  def find_or_create_many(schema, params_list, opts \\ []) when is_list(params_list) do
    run_multi(Multi.build_find_or_create_multi(schema, params_list, opts), opts)
    |> handle_response_preload(opts)
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

  ## Options

    * `:preload` - associations to preload on every struct in the result list.
      Accepts the same shapes as `preload/3`. Applied after the transaction.

  See also `find_and_upsert/4`, `update_many/3`, and `find_or_create_many/3`.
  """
  @spec find_and_upsert_many(module(), list(term()), opts()) :: {:ok, list(term())} | {:error, term()}
  def find_and_upsert_many(schema, entries, opts \\ []) when is_list(entries) do
    run_multi(Multi.build_upsert_multi(schema, entries, opts), opts)
    |> handle_response_preload(opts)
  end

  defp run_multi(multi, opts) do
    multi
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

  defp maybe_preload(nil, _opts), do: nil

  defp maybe_preload(data, opts) do
    case opts[:preload] do
      nil -> data
      [] -> data
      preloads -> preload(data, preloads, opts)
    end
  end

  defp handle_response_preload({:ok, value}, opts), do: {:ok, maybe_preload(value, opts)}
  defp handle_response_preload(other, _opts), do: other

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

  defp maybe_apply_optimistic_lock(changeset, queryable, opts) do
    case resolve_optimistic_lock(queryable, opts) do
      false ->
        changeset

      {field, incrementer} when is_atom(field) and is_function(incrementer, 1) ->
        Changeset.optimistic_lock(changeset, field, incrementer)

      field when is_atom(field) ->
        Changeset.optimistic_lock(changeset, field)
    end
  end

  defp resolve_optimistic_lock(queryable, opts) do
    case Keyword.fetch(opts, :optimistic_lock) do
      {:ok, value} ->
        value

      :error ->
        schema = CommonSchema.get_schema(queryable)

        if schema !== nil and function_exported?(schema, :optimistic_lock, 0) do
          schema.optimistic_lock()
        else
          false
        end
    end
  end
end
