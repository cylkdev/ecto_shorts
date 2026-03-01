# EctoShorts

[![Hex version badge](https://img.shields.io/hexpm/v/ecto_shorts.svg)](https://hex.pm/packages/ecto_shorts)
[![Coveralls](https://github.com/MikaAK/ecto_shorts/actions/workflows/coveralls.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/coveralls.yml)
[![Credo](https://github.com/MikaAK/ecto_shorts/actions/workflows/credo.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/credo.yml)
[![Dialyzer](https://github.com/MikaAK/ecto_shorts/actions/workflows/dialyzer.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/dialyzer.yml)

EctoShorts is a standardized, data-driven API library that simplifies common
Ecto database operations by providing a concise filtering language for query
composition, CRUD actions, and changeset helpers, allowing developers to write
shorter, more readable code when working with Ecto queries and database
operations through modules like Actions, CommonFilters, CommonChanges, and
SchemaHelpers.

The library abstracts away Ecto's complexity with a clean, intuitive interface
that handles the heavy lifting behind the scenes, while comprehensive
documentation with practical examples ensures developers can quickly understand
and confidently implement database operations without getting bogged down in
implementation details.

The API is split into the following components:

  * `EctoShorts.Actions` - Provides functions for executing database operations,
    including CRUD, batch, bulk, `Ecto.Multi`, and transaction workflows. This
    is the primary API module for most callers.

  * `EctoShorts.CommonFilters` - Provides a filtering language for building
    queries from data. It supports polymorphic sources and schemaless operations.

  * `EctoShorts.CommonChanges` - Provides helpers for working with `Ecto.Changeset`,
    including association preloading, conditional `put_*` operations, and field
    validation.

  * `EctoShorts.CommonSchema` - Provides helpers for schema introspection,
    polymorphic source handling, and changeset construction.

  * `EctoShorts.CommonParams` - Provides parameter and option helpers for
    `m:Ecto.Repo.insert_all/3`, `m:Ecto.Repo.update_all/3`, and
    `m:Ecto.Repo.delete_all/2`, including timestamp handling, placeholders, and
    validation.

  * `EctoShorts.CommonQuery` - Provides query introspection helpers, including
    binding and source resolution.

  * `EctoShorts.Compiler` - Provides helpers for generating function clauses
    dynamically.

  * `EctoShorts.Dynamics` - Provides helpers for building `Ecto.Query.dynamic/2`
    expressions.

  * `EctoShorts.Testing` - Provides test helpers for asserting on SQL, queries,
    and dynamic expressions.

## Special Thank you

EctoShorts was built on the shoulders of giants, and owes a debt of gratitude
to the Ecto team and the Ecto community for their hard work and dedication to
the project.

## Installation

Add EctoShorts to your dependencies:

    {:ecto_shorts, "~> 3.0"}

Configure a repo:

    	# config/config.exs
	config :ecto_shorts, repo: MyApp.Repo

## Getting Started

Once you have completed the installation steps, you can start using the API.

### Prerequisites

Before the examples below will work, your application must have:

	* An `Ecto.Repo` module (for example `MyApp.Repo`) that is configured and started.
	* An `Ecto.Schema` module (for example `MyApp.Post`).
	* A changeset function for write operations.
	  * By default, `EctoShorts.Actions` will call your schema's `changeset/2`.
	  * You can override this by passing the `:changeset` option.

### Ecto in 60 seconds

If you are new to Ecto, these are the core building blocks used by EctoShorts:

	* `Ecto.Repo` - where queries run (it talks to your database).
	* `Ecto.Schema` - what you query (your table-backed structs).
	* `Ecto.Query` - how you read data (a composable query value).
	* `Ecto.Changeset` - how you write data (cast/validate before insert/update).

EctoShorts must also know which repo to use:

	* Either configure it once:

		# config/config.exs
		config :ecto_shorts, repo: MyApp.Repo

	* Or pass `:repo` / `:replica` at runtime (shown below).

### What is happening in these examples

	* `EctoShorts.Actions` is the entry point that builds queries and executes them.
	* Filter params are plain data:
	  * Keys that match schema fields become `where` conditions.
	  * Special keys like `limit` become query operations.
	* Under the hood:
	  * `EctoShorts.CommonFilters` turns params into an `Ecto.Query`.
	  * Your `Ecto.Repo` runs that query against the database.

### Examples

	# Create a record (write operations use `:repo`)
	{:ok, post} =
	  EctoShorts.Actions.create(MyApp.Post, %{title: "Hello"}, repo: MyApp.Repo)

	# List records (read operations use `:replica`, falling back to `:repo`)
	# `published` becomes a WHERE condition, and `limit` becomes a query operation.
	posts =
	  EctoShorts.Actions.all(MyApp.Post, %{published: true, limit: 10}, replica: MyApp.Repo)

	# Retrieve a record
	# Returns `{:ok, struct}` or `{:error, reason}`.
	{:ok, post} =
	  EctoShorts.Actions.find(MyApp.Post, %{id: 1}, replica: MyApp.Repo)

	# Update a record
	{:ok, post} =
	  EctoShorts.Actions.update(MyApp.Post, post, %{title: "Updated"}, repo: MyApp.Repo)

	# Delete a record
	{:ok, _} = EctoShorts.Actions.delete(post, repo: MyApp.Repo)

### Troubleshooting

If you hit an error while trying the examples above, these are the most common causes:

	* Repo not configured.
	  * Fix: set `config :ecto_shorts, repo: MyApp.Repo` or pass `repo:`/`replica:` at runtime.
	* Repo not started.
	  * Fix: start your repo under your application supervisor.
	* Missing changeset.
	  * Fix: add `changeset/2` to your schema module or pass `changeset:` in options.
	* A filter key does not match a schema field (or is not a supported query operation).
	  * Fix: double-check the field name, or move the key under an explicit query operation.

### Next steps

	* Read the API entry point docs: `EctoShorts.Actions`.
	* Learn the filter language: `EctoShorts.CommonFilters`.
	* See nesting and precedence rules: `guides/RULES.md`.
	* Browse example filter shapes: `guides/WORKED_EXAMPLES.md`.

## Configuration

Configure the library in your application config:

    config :ecto_shorts,
      repo: MyApp.Repo,
      replica: MyApp.Repo.Replica,
      error_module: MyApp.CustomError,
      dynamic_adapter: MyApp.DynamicAdapter,
      max_binding_positings: 3

Options:

  * `:repo` - The default `Ecto.Repo` for write operations.

  * `:replica` - The `Ecto.Repo` for read operations. Defaults to the value of
    `:repo`.

  * `:error_module` - A module implementing the `EctoShorts.Actions.Error`
    behaviour. This module formats error messages.

  * `:dynamic_adapter` - A module implementing the `EctoShorts.Dynamics.Adapter`
    behaviour. This module builds dynamic expressions.

  * `:max_binding_positings` - The maximum number of query bindings allowed
    before EctoShorts falls back to a subquery strategy. Defaults to `3`.

All configuration keys are optional. You can also pass `:repo` and `:replica` at
runtime via options on most `EctoShorts.Actions` functions.
