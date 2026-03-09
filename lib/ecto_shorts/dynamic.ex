# defmodule EctoShorts.Dynamic do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Defines the behaviour for pluggable dynamic expression adapters.

#   Use this module when building custom adapters that translate filter
#   parameters into Ecto dynamic expressions. An adapter implements two
#   callbacks: `operators/0` to list special operators, and `build_dynamic/4`
#   to construct the dynamic expressions.

#   ## When to implement a custom adapter

#   Implement a custom adapter when you need to:

#   * **Support database-specific operators** - add PostgreSQL array operators,
#     MySQL JSON functions, or other database-specific features.
#   * **Override default behaviour** - change how certain filter keys are
#     translated to dynamic expressions.
#   * **Add custom filter operators** - implement domain-specific filters like
#     `:before`, `:after`, `:ids`, or `:search`.
#   * **Integrate with existing code** - adapt the filter language to match
#     your application's conventions.

#   ## Adapter architecture

#   The adapter follows this flow:

#   1. **Receive filter parameters** - `EctoShorts.Dynamics` receives filter
#      params from `CommonFilters`.
#   2. **Check operators list** - calls `operators/0` to determine which keys
#      are special operators.
#   3. **Dispatch to adapter** - calls `build_dynamic/4` for each filter
#      key-value pair.
#   4. **Return dynamic expression** - the adapter returns an Ecto dynamic
#      expression.
#   5. **Combine expressions** - `Dynamics` combines all expressions with AND
#      logic.

#   ## Implementing an adapter

#   A minimal adapter must export two callbacks: `operators/0` and
#   `build_dynamic/4`.

#       defmodule MyApp.DynamicAdapter do
#         @behaviour EctoShorts.Dynamic

#         @operators [:ids, :before, :after]

#         @impl true
#         def operators, do: @operators

#         @impl true
#         def build_dynamic(source, binding_selector, key, expr) do
#           import Ecto.Query, only: [dynamic: 2]

#           case key do
#             :ids -> dynamic([{^binding_selector, r}], r.id in ^expr)
#             :before -> dynamic([{^binding_selector, r}], r.inserted_at < ^expr)
#             :after -> dynamic([{^binding_selector, r}], r.inserted_at > ^expr)
#             _ -> dynamic([{^binding_selector, r}], field(r, ^key) == ^expr)
#           end
#         end
#       end

#   ## Registering the adapter

#   Configure the adapter globally in your application config:

#       # config/config.exs
#       config :ecto_shorts, dynamic_adapter: MyApp.DynamicAdapter

#   Or pass it at runtime via the `:dynamic_adapter` option on
#   `EctoShorts.CommonFilters.convert_params_to_filter/3`:

#       EctoShorts.CommonFilters.convert_params_to_filter(
#         Post,
#         %{title: "Hello"},
#         dynamic_adapter: MyApp.CustomAdapter
#       )

#   ## Complete adapter example

#   An adapter that handles multiple operators with type checking:

#       defmodule MyApp.DynamicAdapter do
#         @behaviour EctoShorts.Dynamic

#         import Ecto.Query, only: [dynamic: 2]

#         @operators [:ids, :before, :after, :search, :published]

#         @impl true
#         def operators, do: @operators

#         @impl true
#         def build_dynamic(_source, binding_selector, key, expr) do
#           case key do
#             :ids when is_list(expr) ->
#               dynamic([{^binding_selector, r}], r.id in ^expr)

#             :before when is_struct(expr, DateTime) ->
#               dynamic([{^binding_selector, r}], r.inserted_at < ^expr)

#             :after when is_struct(expr, DateTime) ->
#               dynamic([{^binding_selector, r}], r.inserted_at > ^expr)

#             :search when is_binary(expr) ->
#               pattern = "%\#{expr}%"
#               dynamic([{^binding_selector, r}],
#                 ilike(r.title, ^pattern) or ilike(r.body, ^pattern)
#               )

#             :published when is_boolean(expr) ->
#               dynamic([{^binding_selector, r}], r.published == ^expr)

#             _ ->
#               build_field_expression(binding_selector, key, expr)
#           end
#         end

#         defp build_field_expression(binding_selector, key, expr) when is_map(expr) do
#           # Handle operator map: %{>: 10, <: 100}
#           Enum.reduce(expr, true, fn {op, val}, acc ->
#             condition = build_operator_expression(binding_selector, key, op, val)
#             dynamic([{^binding_selector, r}], ^acc and ^condition)
#           end)
#         end

#         defp build_field_expression(binding_selector, key, expr) do
#           # Default equality
#           dynamic([{^binding_selector, r}], field(r, ^key) == ^expr)
#         end

#         defp build_operator_expression(binding_selector, key, :>, val) do
#           dynamic([{^binding_selector, r}], field(r, ^key) > ^val)
#         end

#         defp build_operator_expression(binding_selector, key, :<, val) do
#           dynamic([{^binding_selector, r}], field(r, ^key) < ^val)
#         end

#         defp build_operator_expression(binding_selector, key, :>=, val) do
#           dynamic([{^binding_selector, r}], field(r, ^key) >= ^val)
#         end

#         defp build_operator_expression(binding_selector, key, :<=, val) do
#           dynamic([{^binding_selector, r}], field(r, ^key) <= ^val)
#         end

#         defp build_operator_expression(binding_selector, key, :in, vals) when is_list(vals) do
#           dynamic([{^binding_selector, r}], field(r, ^key) in ^vals)
#         end
#       end

#   ## Testing your adapter

#   Test your adapter by verifying the generated SQL:

#       defmodule MyApp.DynamicAdapterTest do
#         use ExUnit.Case
#         use EctoShorts.Testing, repo: MyApp.Repo

#         test "ids operator generates IN clause" do
#           query = EctoShorts.CommonFilters.convert_params_to_filter(
#             Post,
#             %{ids: [1, 2, 3]},
#             dynamic_adapter: MyApp.DynamicAdapter
#           )

#           expected = from p in Post, where: p.id in ^[1, 2, 3]
#           assert_sql(expected, query)
#         end

#         test "search operator generates ILIKE clause" do
#           query = EctoShorts.CommonFilters.convert_params_to_filter(
#             Post,
#             %{search: "elixir"},
#             dynamic_adapter: MyApp.DynamicAdapter
#           )

#           {sql, _} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)
#           assert sql =~ "ILIKE"
#         end
#       end

#   ## Common patterns

#   ### Pattern 1: Type-based dispatch

#   Route to different implementations based on value type:

#       def build_dynamic(_source, binding_selector, key, expr) when is_list(expr) do
#         dynamic([{^binding_selector, r}], field(r, ^key) in ^expr)
#       end

#       def build_dynamic(_source, binding_selector, key, expr) when is_map(expr) do
#         # Handle operator map
#         build_operator_map(binding_selector, key, expr)
#       end

#       def build_dynamic(_source, binding_selector, key, expr) do
#         # Default equality
#         dynamic([{^binding_selector, r}], field(r, ^key) == ^expr)
#       end

#   ### Pattern 2: Schema introspection

#   Use the source parameter to check field types:

#       def build_dynamic(source, binding_selector, key, expr) do
#         case EctoShorts.CommonSchema.get_schema_reflection(source, :type, key) do
#           {:array, _} ->
#             build_array_expression(binding_selector, key, expr)

#           :string ->
#             build_string_expression(binding_selector, key, expr)

#           _ ->
#             build_default_expression(binding_selector, key, expr)
#         end
#       end

#   ### Pattern 3: Operator delegation

#   Delegate to specialized modules for complex operators:

#       def build_dynamic(source, binding_selector, key, expr) do
#         cond do
#           key in @custom_operators ->
#             CustomOperators.build_dynamic(binding_selector, key, expr)

#           match?({:array, _}, get_field_type(source, key)) ->
#             ArrayOperators.build_dynamic(binding_selector, key, expr)

#           true ->
#             ScalarOperators.build_dynamic(binding_selector, key, expr)
#         end
#       end

#   ### Pattern 4: Binding selector handling

#   Handle different binding selector types:

#       defp build_binding_list(nil), do: [quote(do: r)]
#       defp build_binding_list({:as, alias}), do: [quote(do: {^alias, r})]
#       defp build_binding_list({:at, pos}), do: build_positional_binding(pos)

#       defp build_positional_binding(1), do: [quote(do: r)]
#       defp build_positional_binding(2), do: [quote(do: _), quote(do: r)]
#       defp build_positional_binding(n) do
#         List.duplicate(quote(do: _), n - 1) ++ [quote(do: r)]
#       end

#   ## Troubleshooting

#   **Problem:** Adapter is not being called.

#   **Solution:** Verify the adapter is configured correctly. Check that
#   `:dynamic_adapter` points to the correct module in your config or options.

#   **Problem:** Generated SQL is incorrect.

#   **Solution:** Use `Ecto.Adapters.SQL.to_sql/3` to inspect the generated SQL.
#   Verify that your dynamic expressions use the correct Ecto query syntax.

#   **Problem:** Operator is not recognized.

#   **Solution:** Add the operator to the list returned by `operators/0`. Only
#   operators in this list are routed directly to `build_dynamic/4`.

#   **Problem:** Type errors at runtime.

#   **Solution:** Add guards to your `build_dynamic/4` clauses to validate value
#   types before building expressions.

#   See also `EctoShorts.Dynamics`, `EctoShorts.Dynamics.Postgres`,
#   `EctoShorts.Config`, and `EctoShorts.CommonFilters`.
#   """

#   @doc """
#   Returns the list of special operator atoms this adapter handles.

#   Called by `EctoShorts.Dynamics.convert_to_dynamic/4` before dispatching
#   each filter key to the adapter. Keys that appear in the returned list are
#   routed directly to `build_dynamic/4` regardless of the schema field type.
#   Keys not in this list may be handled by a fallback mechanism (such as
#   scalar or array expression builders in the Postgres adapter).

#   ## Return value

#   A list of atoms, for example `[:ids, :before, :after]`. Return an empty
#   list if your adapter handles all keys uniformly in `build_dynamic/4`.

#   ## Example implementation

#       @operators [:ids, :before, :after]

#       @impl true
#       def operators, do: @operators

#   See also `build_dynamic/4` and `operator?/1`.
#   """
#   @callback operators() :: list(atom())

#   @doc """
#   Checks if the given key is a special operator handled by this adapter.

#   Called to determine whether a filter key should be routed to the adapter's
#   special operator handling logic or to the default field expression logic.

#   ## Arguments

#   * `key` - the filter field atom to check (for example `:ids`, `:before`,
#     or `:title`).

#   ## Return value

#   Returns `true` if the key is in the adapter's operator list, `false`
#   otherwise.

#   ## Example implementation

#       @operators [:ids, :before, :after]

#       @impl true
#       def operator?(key), do: key in @operators

#   See also `operators/0`.
#   """
#   @callback operator?(key :: atom()) :: boolean()

#   @doc """
#   Builds a dynamic expression for the given filter key and value.

#   Called by `EctoShorts.Dynamics.convert_to_dynamic/4` once per filter
#   key-value pair after boolean operators (`:and`, `:or`) have been
#   unwrapped and helper expressions (`:datetime_add`, `:date_add`) have
#   been applied.

#   ## Arguments

#   * `source` - the Ecto queryable or `{table_name, schema}` tuple that the
#     query is built from. Use this to introspect schema field types when
#     deciding which expression to generate.
#   * `binding_selector` - the named or positional binding atom used to
#     reference the correct query binding in the generated `dynamic/2`
#     expression (for example `:post` or `nil` for the root binding).
#   * `key` - the filter field atom (for example `:title`, `:inserted_at`,
#     or a special operator like `:ids`).
#   * `expr` - the filter value exactly as provided by the caller (for
#     example a string, integer, list, or range).

#   ## Return value

#   A dynamic expression (the result of `Ecto.Query.dynamic/2`).

#   ## Example implementation

#       @impl true
#       def build_dynamic(_source, binding_selector, key, expr) do
#         import Ecto.Query, only: [dynamic: 2]

#         case key do
#           :ids -> dynamic([{^binding_selector, r}], r.id in ^expr)
#           _ -> dynamic([{^binding_selector, r}], field(r, ^key) == ^expr)
#         end
#       end

#   See also `operators/0` and `EctoShorts.Dynamics`.
#   """
#   @callback build_dynamic(
#               source :: term(),
#               binding_selector :: term(),
#               key :: atom(),
#               expr :: term()
#             ) :: Ecto.Query.dynamic_expr()
# end
