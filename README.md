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

Once you have completed the installation steps, you can start using the API:

    # create a record
    {:ok, post} = EctoShorts.Actions.create(MyApp.Post, %{title: "Hello"})

    # list records
    posts = EctoShorts.Actions.all(MyApp.Post, %{published: true, limit: 10})

    # retrieve a record
    {:ok, post} = EctoShorts.Actions.find(MyApp.Post, %{id: 1})

    # update a record
    {:ok, post} = EctoShorts.Actions.update(MyApp.Post, post, %{title: "Updated"})

    # delete a record
    {:ok, _} = EctoShorts.Actions.delete(post)

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
