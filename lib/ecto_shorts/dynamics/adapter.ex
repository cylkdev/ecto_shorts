defmodule EctoShorts.Dynamics.Adapter do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Defines the behaviour for pluggable dynamic expression adapters.

  `EctoShorts.Dynamics` delegates all expression construction to a
  configured adapter. Implement this behaviour when the built-in
  `EctoShorts.Dynamics.Adapters.Postgres` adapter does not cover your
  database's operator set, or when you need to override how individual
  filter keys are translated to dynamic expressions.

  ## Implementing an adapter

  A minimal adapter must export two callbacks: `operators/0` and
  `build_dynamic/4`.

      defmodule MyApp.DynamicAdapter do
        @behaviour EctoShorts.Dynamics.Adapter

        @operators [:ids, :before, :after]

        @impl true
        def operators, do: @operators

        @impl true
        def build_dynamic(source, binding_selector, key, expr) do
          import Ecto.Query, only: [dynamic: 2]

          case key do
            :ids -> dynamic([{^binding_selector, r}], r.id in ^expr)
            :before -> dynamic([{^binding_selector, r}], r.inserted_at < ^expr)
            :after -> dynamic([{^binding_selector, r}], r.inserted_at > ^expr)
            _ -> dynamic([{^binding_selector, r}], field(r, ^key) == ^expr)
          end
        end
      end

  ## Registering the adapter

  Configure the adapter globally in your application config:

      # config/config.exs
      config :ecto_shorts, dynamic_adapter: MyApp.DynamicAdapter

  Or pass it at runtime via the `:dynamic_adapter` option on
  `EctoShorts.CommonFilters.convert_params_to_filter/3`.

  See also `EctoShorts.Dynamics` and `EctoShorts.Config`.
  """

  @doc """
  Returns the list of special operator atoms this adapter handles.

  Called by `EctoShorts.Dynamics.convert_to_dynamic/4` before dispatching
  each filter key to the adapter. Keys that appear in the returned list are
  routed directly to `build_dynamic/4` regardless of the schema field type.
  Keys not in this list may be handled by a fallback mechanism (such as
  scalar or array expression builders in the Postgres adapter).

  ## Return value

  A list of atoms, for example `[:ids, :before, :after]`. Return an empty
  list if your adapter handles all keys uniformly in `build_dynamic/4`.

  ## Example implementation

      @operators [:ids, :before, :after]

      @impl true
      def operators, do: @operators

  See also `build_dynamic/4`.
  """
  @callback operators() :: [atom()]

  @doc """
  Builds a dynamic expression for the given filter key and value.

  Called by `EctoShorts.Dynamics.convert_to_dynamic/4` once per filter
  key-value pair after boolean operators (`:and`, `:or`) have been
  unwrapped and helper expressions (`:datetime_add`, `:date_add`) have
  been applied.

  ## Arguments

  * `source` - the Ecto queryable or `{table_name, schema}` tuple that the
    query is built from. Use this to introspect schema field types when
    deciding which expression to generate.
  * `binding_selector` - the named or positional binding atom used to
    reference the correct query binding in the generated `dynamic/2`
    expression (for example `:post` or `nil` for the root binding).
  * `key` - the filter field atom (for example `:title`, `:inserted_at`,
    or a special operator like `:ids`).
  * `expr` - the filter value exactly as provided by the caller (for
    example a string, integer, list, or range).

  ## Return value

  A dynamic expression (the result of `Ecto.Query.dynamic/2`).

  ## Example implementation

      @impl true
      def build_dynamic(_source, binding_selector, key, expr) do
        import Ecto.Query, only: [dynamic: 2]

        case key do
          :ids -> dynamic([{^binding_selector, r}], r.id in ^expr)
          _ -> dynamic([{^binding_selector, r}], field(r, ^key) == ^expr)
        end
      end

  See also `operators/0` and `EctoShorts.Dynamics`.
  """
  @callback build_dynamic(
              source :: term(),
              binding_selector :: term(),
              key :: atom(),
              expr :: term()
            ) :: Ecto.Query.dynamic_expr()
end
