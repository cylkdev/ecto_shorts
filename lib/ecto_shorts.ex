defmodule EctoShorts do
  @moduledoc """
  Provides a standardized, data-driven API for common Ecto operations.

  Use this library when you want to build Ecto queries from plain maps and
  keyword lists, perform CRUD operations with a single function call, or
  manage bulk and transactional workflows without writing repetitive
  boilerplate.

  ## Public Entry Points

  Start with one of these modules depending on what you need:

    * `EctoShorts.Actions` — CRUD, batch, bulk, multi, and transaction
      operations. This is the main interface most callers use.
    * `EctoShorts.CommonFilters` — converts a map or keyword list of
      filter params into an `Ecto.Query`.
    * `EctoShorts.CommonChanges` — changeset helpers for preloading
      associations, conditional puts, and field validations.
    * `EctoShorts.CommonSchema` — schema introspection, polymorphic
      source handling, and changeset creation.
    * `EctoShorts.CommonParams` — prepares data for `insert_all` and
      `update_all` with timestamp, placeholder, and validation support.
    * `EctoShorts.CommonQuery` — query introspection for bindings and
      source resolution.
    * `EctoShorts.Testing` — test assertion helpers for queries and
      dynamic expressions.

  ## Configuration

  Configure the library in your application config:

      # config/config.exs
      config :ecto_shorts,
        repo: MyApp.Repo,
        replica: MyApp.Repo.Replica,
        error_module: MyApp.CustomError,
        dynamic_adapter: MyApp.DynamicAdapter,
        max_query_bindings: 10

  All configuration keys are optional. `:repo` and `:replica` can also be
  passed at runtime via the `:repo` and `:replica` options on most
  `EctoShorts.Actions` functions.
  """
end
