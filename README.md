# EctoShorts

[![Hex version badge](https://img.shields.io/hexpm/v/ecto_shorts.svg)](https://hex.pm/packages/ecto_shorts)
[![Coveralls](https://github.com/MikaAK/ecto_shorts/actions/workflows/coveralls.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/coveralls.yml)
[![Credo](https://github.com/MikaAK/ecto_shorts/actions/workflows/credo.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/credo.yml)
[![Dialyzer](https://github.com/MikaAK/ecto_shorts/actions/workflows/dialyzer.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/dialyzer.yml)

EctoShorts is a library built on top of Ecto that helps you handle the database operations most applications need without repeatedly writing the same query and changeset code by hand. Instead of rebuilding those patterns each time, you describe them as data and let EctoShorts handle the common cases for you. The result is less boilerplate, greater consistency across your codebase, and a faster way to implement everyday tasks such as filtering, pagination, CRUD operations, bulk writes, and transactional multi-record workflows.

EctoShorts is organized into the following components:

  * `EctoShorts.Actions` — the primary entry point. It accepts a schema module and a params map, delegates to the supporting components to build and execute the query, and returns results in a consistent `{:ok, result}` / `{:error, reason}` shape.
  * `EctoShorts.CommonFilters` — translates a map or keyword list of filter params into an `Ecto.Query`. Schema field keys become `WHERE` conditions, while reserved keys such as `:limit`, `:order_by`, and `:preload` become the corresponding query operations. Use this directly when you need to build a query without executing it.
  * `EctoShorts.CommonChanges` — provides helpers for building `changeset/2` functions, including preloading and casting associations, trimming strings, applying conditional changes, and validating fields.
  * `EctoShorts.CommonSchema` — handles schema introspection by resolving field types, association metadata, and polymorphic source information at runtime.
  * `EctoShorts.CommonParams` — prepares parameter lists for `insert_all`, `update_all`, and `delete_all`, including timestamp injection and field filtering.
  * `EctoShorts.CommonQuery` — inspects query structure, including named bindings, positional bindings, sources, and prefixes.
  * `EctoShorts.DynamicBuilders` — builds `Ecto.Query.dynamic/2` expressions from data. The PostgreSQL adapter supports scalar comparisons, string matching, array operations, and more.
  * `EctoShorts.Testing` — provides test helpers for asserting against generated SQL and dynamic expressions.

In the typical case, you call `EctoShorts.Actions` from your context module and let it handle the rest. It builds an `Ecto.Query` from `EctoShorts.CommonFilters`, can pass that query through `EctoShorts.DynamicBuilders` when you need more complex expressions, and then executes it through your configured `Ecto.Repo`.

You usually configure your repo once in `config.exs`, and EctoShorts resolves it automatically. When you need to override that behavior, every `EctoShorts.Actions` function also allows you to pass `:repo` or `:replica` at call time.

## Overview

Ecto is a powerful tool, but in practice it often makes you write more code than the task should need, or write the same kind of code over and over. In many real applications, context modules slowly fill up with almost identical query pipelines: `from`, `where`, `order_by`, `limit`, `Repo.all`, repeated for different resources with only small changes. When you add conditional filters, the repetition grows even more. Each optional parameter often leads to another `maybe_filter_*` helper that only decides whether to add a clause or leave the query as it is. The same pattern shows up in changesets, bulk operations, and transactions. None of this is very hard, but over time it adds up to a lot of boilerplate that does not add much real value.

EctoShorts solves this using data. Instead of building every query step by step, you describe what you want with a map or keyword list and pass it to `EctoShorts.Actions`. `CommonFilters` then reads that data and builds the matching `Ecto.Query` for you. Schema field keys become `WHERE` clauses, and options like `:limit`, `:order_by`, `:offset`, and `:preload` are turned into the matching query parts. More advanced comparisons, such as `%{views: %{>: 100}}`, become expressions like `WHERE views > $1`.

When a filter is more complex than simple field matching, `DynamicBuilders` takes over and builds the right `Ecto.Query.dynamic/2` expressions. This lets EctoShorts support things like string matching, array checks, and PostgreSQL-specific operators while keeping the same data-based interface. After the query is built, `Actions` runs it through your configured `Ecto.Repo` and returns a consistent `{:ok, result}` or `{:error, reason}`.

The same idea also applies to write operations. `Actions.create/3` handles the call to `Post.changeset/2` and `Repo.insert/1` for you. `Actions.create_many/3` wraps multiple inserts in an `Ecto.Multi`, so the whole transaction is rolled back if any changeset fails. `Actions.insert_all/3` uses `CommonParams` to prepare the entries by adding timestamps and removing virtual fields before calling `Repo.insert_all/2`.

The result is code that is simpler and easier to maintain. Instead of spreading query and persistence logic across many helper functions, you describe what you want as data and let `Actions` handle it in a consistent way. That keeps your code focused on application behavior instead of repetitive setup work.

## Installation

Add EctoShorts to your dependencies:

    {:ecto_shorts, "~> 3.0"}

Configure a repo:

    # config/config.exs
    config :ecto_shorts, repo: MyApp.Repo

## Configuration

* `:repo` - The primary `Ecto.Repo` module used for write operations.
Required by most `EctoShorts.Actions` functions. Defaults to `nil`.

* `:replica` - A read-only `Ecto.Repo` module. Falls back to `:repo` when
not set. Used by read operations in `EctoShorts.Actions`. Defaults to `nil`.

* `:error_module` - A module implementing the `EctoShorts.Actions.Error`
behaviour. Used to construct error values returned by `EctoShorts.Actions`
functions. Defaults to `EctoShorts.Actions.Error`.

* `:dynamic_adapter` - A module implementing `EctoShorts.Dynamic`.
Auto-resolved to `EctoShorts.DynamicBuilders.Postgres` when the repo uses
`Ecto.Adapters.Postgres`. Defaults to resolved from the repo's adapter.

* `:query_provider` - A module that resolves provider-backed join and lock
expressions. Must export `resolve_query_expression/3` and return shapes
that match the calling filter contract. Defaults to `nil`.

* `:max_positional_bindings` - Controls how many positional query binding clauses
`EctoShorts.Generator` generates. Increase when your queries join more than
three tables. Defaults to `3`.

Example:

    # config/config.exs
    import Config

    config :ecto_shorts,
      repo: MyApp.Repo,
      replica: MyApp.Repo.Replica,
      error_module: MyApp.Error,
      dynamic_adapter: MyApp.DynamicAdapter,
      max_positional_bindings: 3

## Run the first examples

### Prerequisites

When you want to run these examples, you need the following:

* An `Ecto.Repo` module (for example `MyApp.Repo`) that is configured
  and started.
* An `Ecto.Schema` module (for example `MyApp.Post`).
* A `changeset/2` function on the schema for write operations.
  Override with the `:changeset` option if needed.

### Ecto in 60 seconds

If you are new to Ecto, these are the core building blocks:

* `Ecto.Repo` - where queries run (talks to the database).
* `Ecto.Schema` - what you query (table-backed structs).
* `Ecto.Query` - how you read data (composable query values).
* `Ecto.Changeset` - how you write data (cast and validate before
  insert or update).

EctoShorts must know which repo to use. Either configure it once:

    # config/config.exs
    config :ecto_shorts, repo: MyApp.Repo

Or pass `:repo` / `:replica` at call time (shown below).

### How it works

* `EctoShorts.Actions` is the entry point - it builds queries and executes them.

* Filter params are plain data:
  * Keys that match schema fields become `WHERE` conditions.
  * Reserved keys like `:limit` become query operations.

* Under the hood:
  * `EctoShorts.CommonFilters` turns params into an `Ecto.Query`.
  * The configured `Ecto.Repo` runs that query against the database.

### Examples

    alias EctoShorts.Actions

    # Create
    {:ok, post} = Actions.create(Post, %{title: "Hello", body: "World"})

    # Read
    posts = Actions.all(Post, %{published: true, limit: 10})
    {:ok, post} = Actions.find(Post, %{id: 1})

    # Update
    {:ok, post} = Actions.update(Post, post, %{title: "Updated"})

    # Delete
    {:ok, _} = Actions.delete(post)

### Troubleshooting

Common errors when running the examples above:

* **Repo not configured** - set `config :ecto_shorts, repo: MyApp.Repo` or pass `repo:` / `replica:` at call time.

* **Repo not started** - add the repo to your application supervisor.

* **Missing changeset** - add `changeset/2` to the schema module or use the `:changeset` option.

* **Unknown filter key** - verify the key matches a schema field or a supported query operation.

## Common workflows

This section shows how to combine EctoShorts functions for real features.

### Building a CRUD resource

A typical Phoenix context module using EctoShorts:

    defmodule MyApp.Blog do
      alias EctoShorts.Actions
      alias MyApp.Blog.Post

      def list_posts(params \\ %{}) do
        Actions.all(Post, Map.merge(%{order_by: [desc: :inserted_at]}, params))
      end

      def get_post(id) do
        Actions.find(Post, %{id: id})
      end

      def create_post(attrs) do
        Actions.create(Post, attrs)
      end

      def update_post(post, attrs) do
        Actions.update(Post, post, attrs)
      end

      def delete_post(post) do
        Actions.delete(post)
      end
    end

### Filtering and pagination

Use filter params to build complex queries:

    # Filter by multiple fields
    Actions.all(Post, %{
      published: true,
      author_id: 1,
      inserted_at: %{>=: ~U[2024-01-01 00:00:00Z]}
    })

    # Pagination
    Actions.all(Post, %{
      published: true,
      order_by: [desc: :inserted_at],
      limit: 20,
      offset: 40
    })

    # Preload associations
    Actions.all(Post, %{
      published: true,
      preload: [:author, :comments]
    })

See `EctoShorts.CommonFilters` for the complete filter language.

### Bulk operations

Use bulk functions when performance matters more than per-record validation:

    # Insert many records at once
    entries = [
      %{title: "Post 1", body: "Body 1"},
      %{title: "Post 2", body: "Body 2"}
    ]
    {:ok, {2, nil}} = Actions.insert_all(Post, entries)

    # Update many records
    {count, nil} = Actions.update_all(Post, %{draft: true}, %{set: %{published: true}})

    # Delete many records
    {count, nil} = Actions.delete_all(Post, %{published: false})

### Transactional operations

Use Multi functions when operations must succeed or fail together:

    # Create multiple records atomically
    {:ok, posts} = Actions.create_many(Post, [
      %{title: "Post 1"},
      %{title: "Post 2"}
    ])

    # If any fails, all are rolled back
    {:error, reason} = Actions.create_many(Post, [
      %{title: "Valid"},
      %{title: nil}  # Invalid - rolls back the first insert too
    ])

### Custom changesets

Override the default `changeset/2` function:

    # Use a different changeset function
    Actions.create(Post, attrs, changeset: &Post.admin_changeset/2)

    # Inline changeset logic
    Actions.create(Post, attrs, changeset: fn struct, params ->
      struct
      |> Ecto.Changeset.cast(params, [:title, :body])
      |> Ecto.Changeset.put_change(:source, "api")
    end)

## API Overview

### EctoShorts.Actions

The primary entry point for all database operations. Use this module
when you need to create, read, update, or delete records.

    alias EctoShorts.Actions

    # Single-record CRUD
    {:ok, post} = Actions.create(Post, %{title: "Hello"})
    {:ok, post} = Actions.find(Post, %{id: 1})
    {:ok, post} = Actions.update(Post, post, %{title: "Updated"})
    {:ok, _} = Actions.delete(post)

    # Bulk operations (no transactions)
    {:ok, {count, nil}} = Actions.insert_all(Post, list_of_maps)
    {count, nil} = Actions.update_all(Post, %{draft: true}, %{set: %{published: true}})
    {count, nil} = Actions.delete_all(Post, %{published: false})

    # Multi operations (transactional)
    {:ok, posts} = Actions.create_many(Post, list_of_maps)
    {:ok, posts} = Actions.find_many(Post, [%{id: 1}, %{id: 2}])

See `EctoShorts.Actions` for the complete API.

### EctoShorts.CommonFilters

Builds `Ecto.Query` structs from maps and keyword lists. Use this module
when you need to build a query without executing it.

    alias EctoShorts.CommonFilters

    # Build a query from params
    query = CommonFilters.convert_params_to_filter(Post, %{
      published: true,
      views: %{>: 100},
      order_by: [desc: :inserted_at],
      limit: 10
    })

    # Execute with your repo
    posts = MyApp.Repo.all(query)

The filter language supports:

* Field equality: `%{published: true}`
* Comparison operators: `%{views: %{>: 100}}`
* Logical operators: `%{or: [[published: true], [draft: true]]}`
* Query operations: `:limit`, `:offset`, `:order_by`, `:preload`, etc.
* Joins and associations
* Aggregates and grouping

See `EctoShorts.CommonFilters` for the complete filter language.

### EctoShorts.CommonChanges

Changeset helpers for building `changeset/2` functions. Use this module
when you need to preload associations, apply conditional changes, or
validate fields.

    defmodule MyApp.User do
      use Ecto.Schema
      import Ecto.Changeset
      alias EctoShorts.CommonChanges

      def changeset(user, attrs) do
        user
        |> cast(attrs, [:name, :email])
        |> validate_required([:name, :email])
        |> CommonChanges.preload_change_assoc(:address)
        |> CommonChanges.trim_string_change([:name, :email])
      end
    end

See `EctoShorts.CommonChanges` for the complete API.

### EctoShorts.CommonSchema

Schema introspection and changeset construction. Use this module when
you need to inspect schema fields, handle polymorphic sources, or build
changesets programmatically.

    alias EctoShorts.CommonSchema

    # Get schema fields
    fields = CommonSchema.get_schema_fields(Post)

    # Build a changeset
    changeset = CommonSchema.create_changeset(Post, %{title: "Hello"}, [])

See `EctoShorts.CommonSchema` for the complete API.

### EctoShorts.Testing

Test helpers for asserting on SQL, queries, and dynamic expressions.

    defmodule MyApp.BlogTest do
      use MyApp.DataCase
      import EctoShorts.Testing

      test "filters by published status" do
        query = CommonFilters.convert_params_to_filter(Post, %{published: true})

        assert_sql query, ~r/WHERE.*published = \$1/
      end
    end

See `EctoShorts.Testing` for the complete API.

## Architecture overview

EctoShorts modules collaborate in a layered architecture:

    ┌─────────────────────────────────────────────────────────┐
    │                    Your Application                     │
    │                  (Context Modules)                      │
    └─────────────────────────┬───────────────────────────────┘
                              │
                              ▼
    ┌─────────────────────────────────────────────────────────┐
    │                  EctoShorts.Actions                     │
    │            (Entry point for all operations)             │
    └─────────────────────────┬───────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
              ▼               ▼               ▼
    ┌─────────────────┐ ┌───────────┐ ┌─────────────────┐
    │  CommonFilters  │ │CommonParams│ │  CommonChanges  │
    │ (Query building)│ │(Bulk params)│ │(Changeset helpers)│
    └────────┬────────┘ └───────────┘ └─────────────────┘
             │
             ▼
    ┌─────────────────┐
    │    DynamicBuilders     │
    │(Dynamic exprs)  │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────────────────────────────────────────────┐
    │                      Ecto.Repo                          │
    │                     (Database)                          │
    └─────────────────────────────────────────────────────────┘

**Data flow:**

1. Your context module calls `EctoShorts.Actions` with a schema and params.
2. Actions delegates to `CommonFilters` to build an `Ecto.Query`.
3. CommonFilters uses `DynamicBuilders` to build dynamic expressions.
4. Actions executes the query through the configured `Ecto.Repo`.
5. Results are returned to your context module.

**Extensions:**

* `:changeset` option - override the default changeset function
* `:error_module` option - customize error formatting
* `:dynamic_adapter` option - customize dynamic expression building

## Migrating from raw Ecto

This section shows before/after comparisons for common patterns.

### Reading records

**Before (raw Ecto):**

    import Ecto.Query

    def list_published_posts(limit) do
      Post
      |> where([p], p.published == true)
      |> order_by([p], desc: p.inserted_at)
      |> limit(^limit)
      |> Repo.all()
    end

**After (EctoShorts):**

    def list_published_posts(limit) do
      Actions.all(Post, %{
        published: true,
        order_by: [desc: :inserted_at],
        limit: limit
      })
    end

### Creating records

**Before (raw Ecto):**

    def create_post(attrs) do
      %Post{}
      |> Post.changeset(attrs)
      |> Repo.insert()
    end

**After (EctoShorts):**

    def create_post(attrs) do
      Actions.create(Post, attrs)
    end

### Updating records

**Before (raw Ecto):**

    def update_post(post, attrs) do
      post
      |> Post.changeset(attrs)
      |> Repo.update()
    end

**After (EctoShorts):**

    def update_post(post, attrs) do
      Actions.update(Post, post, attrs)
    end

### Complex filtering

**Before (raw Ecto):**

    import Ecto.Query

    def search_posts(params) do
      Post
      |> maybe_filter_published(params[:published])
      |> maybe_filter_author(params[:author_id])
      |> maybe_filter_date(params[:after])
      |> order_by([p], desc: p.inserted_at)
      |> limit(^Map.get(params, :limit, 20))
      |> Repo.all()
    end

    defp maybe_filter_published(query, nil), do: query
    defp maybe_filter_published(query, published) do
      where(query, [p], p.published == ^published)
    end

    defp maybe_filter_author(query, nil), do: query
    defp maybe_filter_author(query, author_id) do
      where(query, [p], p.author_id == ^author_id)
    end

    defp maybe_filter_date(query, nil), do: query
    defp maybe_filter_date(query, after_date) do
      where(query, [p], p.inserted_at >= ^after_date)
    end

**After (EctoShorts):**

    def search_posts(params) do
      filters =
        params
        |> Map.take([:published, :author_id])
        |> maybe_put_date_filter(params[:after])
        |> Map.put(:order_by, [desc: :inserted_at])
        |> Map.put(:limit, Map.get(params, :limit, 20))

      Actions.all(Post, filters)
    end

    defp maybe_put_date_filter(filters, nil), do: filters
    defp maybe_put_date_filter(filters, after_date) do
      Map.put(filters, :inserted_at, %{>=: after_date})
    end

### Bulk operations

**Before (raw Ecto):**

    def publish_all_drafts do
      Post
      |> where([p], p.draft == true)
      |> Repo.update_all(set: [published: true, draft: false])
    end

**After (EctoShorts):**

    def publish_all_drafts do
      Actions.update_all(Post, %{draft: true}, %{set: %{published: true, draft: false}})
    end

## Configuration

    config :ecto_shorts,
      repo: MyApp.Repo,
      replica: MyApp.Repo.Replica,
      error_module: MyApp.CustomError,
      dynamic_adapter: MyApp.DynamicAdapter,
      max_positional_bindings: 3

* `:repo` - the default `Ecto.Repo` for write operations.

* `:replica` - the `Ecto.Repo` for read operations. Falls back to
  `:repo` when not set.

* `:error_module` - a module implementing the
  `EctoShorts.Actions.Error` behaviour for error formatting.

* `:dynamic_adapter` - a module implementing the
  `EctoShorts.Dynamic` behaviour for dynamic expressions.

* `:max_positional_bindings` - maximum query bindings before falling
  back to a subquery strategy. Defaults to `3`.

All keys are optional. Pass `:repo` and `:replica` at call time via
options on most `EctoShorts.Actions` functions.

See `EctoShorts.Config` for all configuration options.

## Edge cases and warnings

> **Empty params in find/3**
>
> Calling `Actions.find(Post, %{})` returns `{:error, :not_found}`
> immediately without querying the database. This prevents accidental
> fetches of arbitrary records.

> **Large batch sizes**
>
> `insert_all/3` and `update_all/4` execute a single SQL statement.
> Very large batches may exceed database limits. Consider chunking
> into smaller batches for thousands of records.

## Next steps

* API entry point: `EctoShorts.Actions`
* Filter language reference: `EctoShorts.CommonFilters`
* Changeset helpers: `EctoShorts.CommonChanges`
* Configuration: `EctoShorts.Config`
* Nesting and precedence rules: `guides/RULES.md`
* Worked examples: `guides/WORKED_EXAMPLES.md`
