defmodule EctoShorts.Adapter.QueryProvider do
  @moduledoc """
  Public API for resolving dynamic query expressions from a query provider module.

  A query provider is a module that knows how to turn a named expression key and
  its parameters into an Ecto query fragment — such as a subquery, a lock clause,
  or a window definition. This allows callers to supply named, reusable query
  logic without hard-coding it inside the filter layer.

  The active provider is selected by:

    1. The `:query_provider` option in the filter params or call-site opts.
    2. `EctoShorts.Config.query_provider/0` (configured in application env).

  ## Implementing a query provider

  Define a module with a `resolve_query_expression/4` callback:

      defmodule MyApp.QueryProvider do
        def resolve_query_expression(selected_binding, expression_key, expression_params, opts) do
          case expression_key do
            :active_users ->
              {:ok, from(u in "users", where: u.active == true)}

            _ ->
              {:error, :unsupported_fragment_key}
          end
        end
      end

  Then configure it globally:

      # config/config.exs
      config :ecto_shorts, query_provider: MyApp.QueryProvider

  Or pass it at runtime:

      EctoShorts.CommonFilters.convert_params_to_filter(
        Post,
        %{join: %{name: :active_users}},
        query_provider: MyApp.QueryProvider
      )

  ## Return values

  `resolve_query_expression/4` must return one of:

    * `{:ok, Ecto.Query.t()}` — a subquery or fragment query.
    * `{:ok, function}` — a unary function `(Ecto.Query.t() -> Ecto.Query.t())` applied to the current query.
    * `{:ok, keyword()}` — a keyword list of query options (e.g. for window definitions).
    * `{:error, reason}` — an error tuple; the filter layer will log a warning and skip the expression.
    * `nil` — treated as "no expression"; the filter layer skips the expression silently.
  """

  @doc """
  Delegates `resolve_query_expression/4` to the given provider `module`.

  This is the dispatch entry point used internally by filter modules such as
  `EctoShorts.CommonFilters.Join` and `EctoShorts.CommonFilters.Lock`.
  """
  def resolve_query_expression(module, selected_binding, expression_key, expression_params, opts) do
    module.resolve_query_expression(selected_binding, expression_key, expression_params, opts)
  end
end
