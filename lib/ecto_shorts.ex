defmodule EctoShorts do
  @moduledoc """
  Provides a standardized, data-driven API for common Ecto operations.

  EctoShorts wraps frequent Ecto workflows behind a consistent API. It covers CRUD,
  batch and bulk operations, `Ecto.Multi` and transactions, query composition, and
  changeset building.

  EctoShorts is split into these components:

    * `EctoShorts.Actions` — Database operations, including CRUD, batch, bulk,
      multi, and transaction workflows. This is the main entry point for most
      callers.

    * `EctoShorts.CommonFilters` — Data-driven query composition. Supports
      polymorphic sources and schemaless operations.

    * `EctoShorts.CommonChanges` — Changeset helpers for association preloading,
      conditional puts, and field validation.

    * `EctoShorts.CommonSchema` — Schema introspection, polymorphic source
      handling, and changeset creation.

    * `EctoShorts.CommonParams` — Builds parameters and options for
      `Ecto.Repo.insert_all/2`, `Ecto.Repo.update_all/2`, and
      `Ecto.Repo.delete_all/2`, including timestamps, placeholders, and
      validation.

    * `EctoShorts.CommonQuery` — Query introspection for binding and source
      resolution.

    * `EctoShorts.Compiler` — Support for dynamic function clause generation.

    * `EctoShorts.Dynamics` — Support for building dynamic expressions.

    * `EctoShorts.Testing` — Helpers for asserting on SQL, queries, and dynamic
      expressions.

  ## Getting started

  Add EctoShorts to your dependencies:

      {:ecto_shorts, "~> 3.0"}

  Configure a repo:

      # config/config.exs
      config :ecto_shorts, repo: MyApp.Repo

  Now you can start using the API:

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

    * `:repo` — The default `Ecto.Repo` for write operations.

    * `:replica` — The `Ecto.Repo` for read operations. Defaults to the value of
      `:repo`.

    * `:error_module` — A module implementing the `EctoShorts.Actions.Error`
      behaviour. This module formats error messages.

    * `:dynamic_adapter` — A module implementing the `EctoShorts.Dynamics.Adapter`
      behaviour. This module builds dynamic expressions.

    * `:max_binding_positings` — The maximum number of query bindings allowed
      before EctoShorts falls back to a subquery strategy. Defaults to `3`.

  All configuration keys are optional. You can also pass `:repo` and `:replica` at
  runtime via options on most `EctoShorts.Actions` functions.

  See `EctoShorts.Config` for the full configuration reference.
  """
end
