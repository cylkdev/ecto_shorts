# defmodule EctoShorts.CommonFilters.Filter do
#   alias EctoShorts.Adapters.Postgres

#   def build_query(
#         filter,
#         source,
#         query,
#         binding_selector,
#         key,
#         value,
#         opts
#       ) do
#     Postgres.build_dynamic(source, binding_selector, key, value, opts)
#   end
# end

# # defmodule EctoShorts.CommonFilters.Filter do
# #   @moduledoc since: "3.0.0"
# #   @moduledoc """
# #   Default query builder for WHERE clauses, set operations, and query-level filters.

# #   Handles schema field filters (converting them to dynamic WHERE expressions),
# #   custom filters (`:ids`, `:before`, `:after`, `:start_date`, `:end_date`,
# #   `:exists`), boolean operators (`:and`, `:or`), raw `:dynamic` expressions,
# #   and query-level operations like `:limit`, `:offset`, `:lock`, `:exclude`,
# #   `:union`, `:intersect`, `:except`, `:first`, `:last`, `:put_query_prefix`,
# #   `:recursive_ctes`, and `:reverse_order`.
# #   """

# #   alias EctoShorts.CommonFilters
# #   alias EctoShorts.CommonSchema
# #   alias EctoShorts.Dynamics
# #   alias EctoShorts.Logger
# #   alias EctoShorts.QueryProvider

# #   alias Ecto.Query
# #   require Ecto.Query

# #   @logger_prefix "EctoShorts.CommonFilters.Filter"

# #   @query_filters [
# #     :except,
# #     :except_all,
# #     :exclude,
# #     :first,
# #     :intersect,
# #     :intersect_all,
# #     :union,
# #     :union_all,
# #     :last,
# #     :lock,
# #     :limit,
# #     :offset,
# #     :put_query_prefix,
# #     :recursive_ctes,
# #     :reverse_order
# #   ]

# #   @set_operations [:except, :except_all, :intersect, :intersect_all, :union, :union_all]

# #   @doc "Applies the given filter to the query."
# #   def build(
# #         _schema_source,
# #         filter,
# #         query,
# #         _binding_selector,
# #         {:dynamic, value},
# #         _opts
# #       )
# #       when filter in [:where, :or_where] do
# #     if is_struct(value, Ecto.Query.DynamicExpr) do
# #       apply_where_expr(filter, query, value)
# #     else
# #       Logger.warning(
# #         @logger_prefix,
# #         "Expected :dynamic payload to be an Ecto.Query.DynamicExpr, got: #{inspect(value)}"
# #       )

# #       query
# #     end
# #   end

# #   def build(schema_source, filter, query, binding_selector, value, opts)
# #       when filter in @query_filters do
# #     apply_expr(schema_source, filter, query, binding_selector, value, opts)
# #   end

# #   def build(schema_source, filter, query, binding_selector, {key, value}, opts) do
# #     dyn = Dynamics.convert_to_dynamic(schema_source, binding_selector, {key, value}, opts)
# #     apply_where_expr(filter, query, dyn)
# #   end

# #   def build(_schema, _filter, query, _binding_selector, term, _opts) do
# #     Logger.warning(
# #       @logger_prefix,
# #       "Expected params to be a map or keyword list, got: #{inspect(term)}"
# #     )

# #     query
# #   end

# #   defp apply_expr(_schema_source, :exclude, query, _binding_selector, entries, _opts) do
# #     entries
# #     |> List.wrap()
# #     |> Enum.reduce(query, fn filter, q2 -> Query.exclude(q2, filter) end)
# #   end

# #   defp apply_expr(schema_source, op, query, _binding_selector, value, opts)
# #        when op in @set_operations do
# #     expr = to_query(schema_source, value, opts)
# #     apply_set_operation(op, query, expr)
# #   end

# #   for op <- [:except, :except_all, :intersect, :intersect_all, :union, :union_all] do
# #     defp apply_set_operation(unquote(op), query, expr) do
# #       Query.unquote(op)(query, ^expr)
# #     end
# #   end

# #   defp apply_expr(schema_source, :first, query, binding_selector, limit, opts) do
# #     apply_expr(schema_source, :limit, query, binding_selector, limit, opts)
# #   end

# #   defp apply_expr(schema_source, :last, query, binding_selector, entries, opts)
# #        when is_map(entries) or is_list(entries) do
# #     Enum.reduce(entries, query, fn entry, q ->
# #       apply_expr(schema_source, :last, q, binding_selector, entry, opts)
# #     end)
# #   end

# #   defp apply_expr(
# #          schema_source,
# #          :last,
# #          query,
# #          _binding_selector,
# #          {sort_key, limit},
# #          _opts
# #        ) do
# #     # Applies `:last` pagination by selecting the last `limit` rows
# #     # according to `sort_keys`.
# #     #
# #     # First, any existing `order_by` is removed and the query is ordered
# #     # in descending order by `sort_keys` so the last rows come first;
# #     # then `limit` is applied and the result is wrapped in a subquery.
# #     #
# #     # Finally, the outer query is ordered in ascending order by the same
# #     # `sort_keys` so the returned rows are presented in the expected order.
# #     # This reduces over `sort_keys` to support composite primary keys
# #     # (multi-field ordering).

# #     sort_keys =
# #       if is_nil(sort_key) do
# #         List.wrap(CommonSchema.get_schema_reflection(schema_source, :primary_key) || :id)
# #       else
# #         List.wrap(sort_key)
# #       end

# #     subquery =
# #       sort_keys
# #       |> Enum.reduce(Query.exclude(query, :order_by), &Query.order_by(&2, desc: ^&1))
# #       |> Query.from(limit: ^limit)
# #       |> Query.subquery()

# #     Enum.reduce(sort_keys, subquery, &Query.order_by(&2, asc: ^&1))
# #   end

# #   defp apply_expr(schema_source, :last, query, binding_selector, limit, opts) do
# #     apply_expr(schema_source, :last, query, binding_selector, {nil, limit}, opts)
# #   end

# #   defp apply_expr(_schema_source, :lock, query, _binding_selector, value, _opts)
# #        when is_function(value, 1) do
# #     value.(query)
# #   end

# #   defp apply_expr(schema_source, :lock, query, binding_selector, params, opts)
# #        when is_map(params) and not is_struct(params) do
# #     apply_expr(schema_source, :lock, query, binding_selector, Map.to_list(params), opts)
# #   end

# #   defp apply_expr(_schema_source, :lock, query, binding_selector, params, opts)
# #        when is_list(params) do
# #     if Keyword.keyword?(params) do
# #       apply_lock_from_resolver(query, binding_selector, params, opts)
# #     else
# #       Logger.warning(
# #         @logger_prefix,
# #         "Expected :lock params to be a unary function or a keyword/map resolver payload, got: #{inspect(params)}"
# #       )

# #       query
# #     end
# #   end

# #   defp apply_expr(_schema_source, :lock, query, _binding_selector, value, _opts) do
# #     Logger.warning(
# #       @logger_prefix,
# #       "Expected :lock params to be a unary function or a keyword/map resolver payload, got: #{inspect(value)}"
# #     )

# #     query
# #   end

# #   defp apply_expr(_schema_source, :limit, query, _binding_selector, value, _opts) do
# #     Query.limit(query, ^value)
# #   end

# #   defp apply_expr(_schema_source, :offset, query, _binding_selector, value, _opts) do
# #     Query.offset(query, ^value)
# #   end

# #   defp apply_expr(_schema_source, :put_query_prefix, query, _binding_selector, value, _opts)
# #        when is_binary(value) do
# #     Query.put_query_prefix(query, value)
# #   end

# #   defp apply_expr(_schema_source, :put_query_prefix, query, _binding_selector, value, _opts) do
# #     Logger.warning(
# #       @logger_prefix,
# #       "Expected :put_query_prefix value to be a string, got: #{inspect(value)}"
# #     )

# #     query
# #   end

# #   defp apply_expr(
# #          _schema_source,
# #          :recursive_ctes,
# #          query,
# #          _binding_selector,
# #          value,
# #          _opts
# #        )
# #        when is_boolean(value) do
# #     Query.recursive_ctes(query, value)
# #   end

# #   defp apply_expr(
# #          _schema_source,
# #          :recursive_ctes,
# #          query,
# #          _binding_selector,
# #          value,
# #          _opts
# #        ) do
# #     Logger.warning(
# #       @logger_prefix,
# #       "Expected :recursive_ctes value to be a boolean, got: #{inspect(value)}"
# #     )

# #     query
# #   end

# #   defp apply_expr(
# #          _schema_source,
# #          :reverse_order,
# #          query,
# #          _binding_selector,
# #          _value,
# #          _opts
# #        ) do
# #     Query.reverse_order(query)
# #   end

# #   defp apply_where_expr(_, query, nil) do
# #     query
# #   end

# #   defp apply_where_expr(:or_where, query, dyn) do
# #     Query.or_where(query, ^dyn)
# #   end

# #   defp apply_where_expr(:where, query, dyn) do
# #     Query.where(query, ^dyn)
# #   end

# #   defp apply_lock_from_resolver(query, binding_selector, params, opts) do
# #     params =
# #       if is_map(params) and not is_struct(params) do
# #         Map.to_list(params)
# #       else
# #         params
# #       end

# #     lock_name = Keyword.get(params, :name)
# #     lock_values = Keyword.get(params, :values, [])

# #     if is_nil(lock_name) do
# #       Logger.warning(
# #         @logger_prefix,
# #         "Expected :lock resolver payload to have a :name key, got: #{inspect(params)}"
# #       )

# #       query
# #     else
# #       case QueryProvider.build_fragment_expression(binding_selector, lock_name, lock_values, opts) do
# #         {:ok, lock_builder} when is_function(lock_builder, 1) ->
# #           lock_builder.(query)

# #         {:ok, other} ->
# #           Logger.warning(
# #             @logger_prefix,
# #             "Expected :lock resolver to return {:ok, (Ecto.Query.t() -> Ecto.Query.t())}, got: #{inspect(other)}"
# #           )

# #           query

# #         {:error, reason} ->
# #           Logger.warning(
# #             @logger_prefix,
# #             "Lock expression callback returned error for key #{inspect(lock_name)}: #{inspect(reason)}"
# #           )

# #           query

# #         other ->
# #           Logger.warning(
# #             @logger_prefix,
# #             "Expected :lock resolver callback to return {:ok, query_builder_fun} | {:error, reason}, got: #{inspect(other)}"
# #           )

# #           query
# #       end
# #     end
# #   end

# #   defp to_query(schema_source, value, opts) do
# #     if is_struct(value, Ecto.Query) do
# #       value
# #     else
# #       CommonFilters.convert_params_to_filter(schema_source, value, opts)
# #     end
# #   end
# # end
