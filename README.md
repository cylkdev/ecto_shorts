# EctoShorts

[![Hex version badge](https://img.shields.io/hexpm/v/ecto_shorts.svg)](https://hex.pm/packages/ecto_shorts)
[![Coveralls](https://github.com/MikaAK/ecto_shorts/actions/workflows/coveralls.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/coveralls.yml)
[![Credo](https://github.com/MikaAK/ecto_shorts/actions/workflows/credo.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/credo.yml)
[![Dialyzer](https://github.com/MikaAK/ecto_shorts/actions/workflows/dialyzer.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/dialyzer.yml)

EctoShorts builds on Ecto to provide data-driven CRUD, query building, and
changeset helpers. Pass plain maps and keyword lists instead of composing
`Ecto.Query` structs by hand.

The library is split into the following modules:

  * `EctoShorts.Actions` - CRUD, batch, bulk, `Ecto.Multi`, and transaction
    operations. This is the primary entry point.

  * `EctoShorts.CommonFilters` - Builds `Ecto.Query` structs from maps and
    keyword lists. Supports polymorphic sources and schemaless queries.

  * `EctoShorts.CommonChanges` - Changeset helpers for association preloading,
    conditional `put_*` operations, and field validation.

  * `EctoShorts.CommonSchema` - Schema introspection, polymorphic source
    handling, and changeset construction.

  * `EctoShorts.CommonParams` - Parameter helpers for `c:Ecto.Repo.insert_all/3`,
    `c:Ecto.Repo.update_all/3`, and `c:Ecto.Repo.delete_all/2`, including
    timestamps, placeholders, and validation.

  * `EctoShorts.CommonQuery` - Query introspection, binding and source
    resolution.

  * `EctoShorts.Compiler` - Dynamic function clause generation.

  * `EctoShorts.Dynamics` - `Ecto.Query.dynamic/2` expression builders.

  * `EctoShorts.Testing` - Test helpers for asserting on SQL, queries, and
    dynamic expressions.
	
## Installation

Add EctoShorts to your dependencies:

    {:ecto_shorts, "~> 3.0"}

Configure a repo:

    # config/config.exs
    config :ecto_shorts, repo: MyApp.Repo

## Getting started

### Prerequisites

The examples below require:

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

  * `EctoShorts.Actions` is the entry point - it builds queries and
    executes them.
  * Filter params are plain data:
    * Keys that match schema fields become `WHERE` conditions.
    * Reserved keys like `:limit` become query operations.
  * Under the hood:
    * `EctoShorts.CommonFilters` turns params into an `Ecto.Query`.
    * The configured `Ecto.Repo` runs that query against the database.

### Examples

    # Create (write operations use :repo)
    {:ok, post} =
      EctoShorts.Actions.create(MyApp.Post, %{title: "Hello"}, repo: MyApp.Repo)

    # List (read operations use :replica, falling back to :repo)
    posts =
      EctoShorts.Actions.all(MyApp.Post, %{published: true, limit: 10}, replica: MyApp.Repo)

    # Find one
    {:ok, post} =
      EctoShorts.Actions.find(MyApp.Post, %{id: 1}, replica: MyApp.Repo)

    # Update
    {:ok, post} =
      EctoShorts.Actions.update(MyApp.Post, post, %{title: "Updated"}, repo: MyApp.Repo)

    # Delete
    {:ok, _} = EctoShorts.Actions.delete(post, repo: MyApp.Repo)

### Troubleshooting

Common errors when running the examples above:

  * **Repo not configured** - set `config :ecto_shorts, repo: MyApp.Repo`
    or pass `repo:` / `replica:` at call time.
  * **Repo not started** - add the repo to your application supervisor.
  * **Missing changeset** - add `changeset/2` to the schema module or
    pass `changeset:` in options.
  * **Unknown filter key** - verify the key matches a schema field or a
    supported query operation.

### Next steps

  * API entry point: `EctoShorts.Actions`.
  * Filter language reference: `EctoShorts.CommonFilters`.
  * Nesting and precedence rules: `guides/RULES.md`.
  * Worked examples: `guides/WORKED_EXAMPLES.md`.

## Configuration

    config :ecto_shorts,
      repo: MyApp.Repo,
      replica: MyApp.Repo.Replica,
      error_module: MyApp.CustomError,
      dynamic_adapter: MyApp.DynamicAdapter,
      max_binding_positions: 3

  * `:repo` - the default `Ecto.Repo` for write operations.

  * `:replica` - the `Ecto.Repo` for read operations. Falls back to
    `:repo`.

  * `:error_module` - a module implementing the
    `EctoShorts.Actions.Error` behaviour for error formatting.

  * `:dynamic_adapter` - a module implementing the
    `EctoShorts.Dynamics.Adapter` behaviour for dynamic expressions.

  * `:max_binding_positions` - maximum query bindings before falling
    back to a subquery strategy. Defaults to `3`.

All keys are optional. Pass `:repo` and `:replica` at call time via
options on most `EctoShorts.Actions` functions.
