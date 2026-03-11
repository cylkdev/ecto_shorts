defmodule EctoShorts.QueryProvider do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Provides an API for injecting compiled query expressions into runtime expressions.

  Use this module when you need to inject custom SQL fragments, database
  functions, or complex expressions into queries built by
  `EctoShorts.CommonFilters`. Fragment providers let you extend the filter
  language with database-specific features without modifying the core library.

  ## When to use fragments

  Use fragment providers when you need to:

  * **Call database-specific functions** - use PostgreSQL functions like
    `to_tsvector`, MySQL JSON functions, or database-specific operators.
  * **Implement custom join sources** - join against views, materialized views,
    or complex subqueries defined as fragments.
  * **Add custom lock expressions** - implement row-level locking strategies
    specific to your database.
  * **Extend the filter language** - add custom filter keys that map to
    database-specific expressions.

  ## Add fragment-backed filters

  By default, fragment resolution is disabled. Enable it by configuring a
  fragment provider:

      config :ecto_shorts,
        query_provider: MyApp.CustomFragments

  Define your custom provider:

      defmodule MyApp.CustomFragments do
        import Ecto.Query

        def build_fragment_expression(selected_binding, expression_key, expression_params) do
          case expression_key do
            :active_users ->
              {:ok, from(u in fragment("SELECT * FROM users WHERE active = true"), select: u)}

            :for_update ->
              {:ok, fn query -> from(q in query, lock: "FOR UPDATE") end}

            _ ->
              nil
          end
        end
      end

  Use the fragment in a filter:

      EctoShorts.CommonFilters.convert_params_to_filter(
        Post,
        %{join: [fragment: [source: %{name: :active_users, values: %{}}, as: :users, on: true]]}
      )

  ## Configuration

  ### Application-level configuration

  Set the default fragment provider in your config:

      config :ecto_shorts,
        query_provider: MyApp.CustomFragments

  This provider is used for all queries unless overridden.

  ### Per-call configuration

  Override the provider for a specific query:

      EctoShorts.CommonFilters.convert_params_to_filter(
        Post,
        %{...},
        query_provider: MyApp.SpecialFragments
      )

  ### Default provider

  When no provider is configured, `EctoShorts.CommonFilters.QueryProviders.NoOp`
  is used. This provider returns `nil` for all expressions, effectively
  disabling fragment support.

  ## Custom provider implementation

  A fragment provider module must export `build_fragment_expression/3`:

      defmodule MyApp.CustomFragments do
        def build_fragment_expression(selected_binding, expression_key, expression_params) do
          # Return a shape that matches the calling filter contract.
        end
      end

  ### Parameters

  * `selected_binding` - the binding selector (for example, `{:as, :post}`,
    `{:at, 1}`, `:first`, `:last`).
  * `expression_key` - an atom identifying which fragment to build (for
    example, `:active_users`, `:for_update`).
  * `expression_params` - a map or keyword list of parameters for the fragment.

  ### Return value

  Return one of:

  * `{:ok, source}` for fragment-backed joins, where `source` is a joinable Ecto
    source value such as `from(... in fragment(...), select: ...)`.
  * `{:ok, query_builder_fun}` for lock builders that transform an `Ecto.Query.t/0`.
  * `{:ok, other}` for other callers that document their own expected shape.
  * `{:error, reason}` when the key is recognized but cannot be built.
  * `nil` when the expression key is not recognized.

  ## Common use cases

  ### Use case 1: Database functions

  Call PostgreSQL full-text search functions:

      defmodule MyApp.CustomFragments do
        import Ecto.Query

        def build_fragment_expression(_selected_binding, :search, %{query: search_query}) do
          fragment("to_tsvector('english', title || ' ' || body) @@ plainto_tsquery(?)", ^search_query)
        end

        def build_fragment_expression(_selected_binding, _key, _params), do: nil
      end

  Use in a filter:

      EctoShorts.CommonFilters.convert_params_to_filter(
        Post,
        %{where: %{dynamic: build_fragment_expression(:first, :search, %{query: "elixir"})}}
      )

  ### Use case 2: Custom join sources

  Join against a fragment-backed source:

      defmodule MyApp.CustomFragments do
        import Ecto.Query

        def build_fragment_expression(_selected_binding, :active_users, _params) do
          {:ok, from(u in fragment("SELECT * FROM active_users_mv"), select: u)}
        end

        def build_fragment_expression(_selected_binding, _key, _params), do: nil
      end

  Use in a join:

      EctoShorts.CommonFilters.convert_params_to_filter(
        Post,
        %{join: [fragment: [source: %{name: :active_users, values: %{}}, as: :users, on: true]]}
      )

  ### Use case 3: Row-level locking

  Implement custom lock expressions:

      defmodule MyApp.CustomFragments do
        import Ecto.Query

        def build_fragment_expression(_selected_binding, :for_update, _params) do
          {:ok, fn query -> from(q in query, lock: "FOR UPDATE") end}
        end

        def build_fragment_expression(_selected_binding, :for_share, _params) do
          {:ok, fn query -> from(q in query, lock: "FOR SHARE") end}
        end

        def build_fragment_expression(_selected_binding, _key, _params), do: nil
      end

  Use in a lock:

      EctoShorts.CommonFilters.convert_params_to_filter(
        Post,
        %{lock: %{name: :for_update}}
      )

      EctoShorts.CommonFilters.convert_params_to_filter(
        Post,
        %{lock: %{name: :for_update_with_clause, values: [clause: "SKIP LOCKED"]}}
      )

  ## Fragment expression examples

  ### Simple fragment

      fragment("NOW()")

  ### Fragment with parameters

      fragment("age > ?", ^18)

  ### Fragment with multiple parameters

      fragment("? BETWEEN ? AND ?", ^field, ^min, ^max)

  ### Fragment with keyword list

      fragment("jsonb_extract_path_text(?, ?)", field(:data), ^"key")

  ## Troubleshooting

  **Problem:** Fragment provider raises "Expected ... to have a build_fragment_expression/3 function".

  **Solution:** Add a `build_fragment_expression/3` function to your provider
  module. The function must accept three arguments and return `{:ok, value}`,
  `{:error, reason}`, or `nil` according to the caller's contract.

  **Problem:** Fragment is not being called.

  **Solution:** Verify the fragment provider is configured correctly. Check
  that the `:query_provider` config points to the correct module.

  **Problem:** Fragment returns `nil` but should return an expression.

  **Solution:** Check that the `expression_key` matches the key you are
  handling in your provider. Use `IO.inspect/2` to see what key is being passed.

  **Problem:** Fragment raises at runtime.

  **Solution:** Verify the provider returns the correct shape for the caller.
  For join sources, build the fragment inside an Ecto query such as
  `from(... in fragment(...), select: ...)` and return it as `{:ok, source}`.

  See also `EctoShorts.Config.query_provider/0`, `EctoShorts.CommonFilters`,
  and `Ecto.Query.API.fragment/1`.
  """

  alias EctoShorts.Config

  @default_adapter EctoShorts.CommonFilters.QueryProviders.NoOp

  @doc false
  def build_fragment_expression(selected_binding, expression_key, expression_params, opts \\ []) do
    query_provider =
      Keyword.get(opts, :query_provider, Config.query_provider()) ||
        @default_adapter

    unless Code.ensure_loaded?(query_provider) and
             function_exported?(query_provider, :build_fragment_expression, 3) do
      raise ArgumentError,
            "Expected expression resolver module to have a build_fragment_expression/3 function, got: #{inspect(query_provider)}"
    end

    query_provider.build_fragment_expression(selected_binding, expression_key, expression_params)
  end
end
