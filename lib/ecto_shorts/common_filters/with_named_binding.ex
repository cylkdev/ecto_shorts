# defmodule EctoShorts.CommonFilters.WithNamedBinding do
#   @moduledoc since: "3.0.0"
#   @moduledoc """
#   Builds `:with_named_binding` expressions from data-driven params.

#   Accepts keyword lists of `{key, params}` entries. Each
#   entry calls `Ecto.Query.with_named_binding/3` with a callback that
#   converts `params` to filters via `CommonFilters.convert_params_to_filter/3`.
#   """

#   alias Ecto.Query
#   alias EctoShorts.CommonFilters
#   alias EctoShorts.Logger

#   require Ecto.Query

#   @logger_prefix "EctoShorts.CommonFilters.WithNamedBinding"

#   @doc false
#   def build(_schema_source, :with_named_binding, query, _selected_binding, params, opts) do
#     reduce_entries(query, params, opts)
#   end

#   defp reduce_entries(query, params, opts) when is_map(params) and not is_struct(params) do
#     reduce_entries(query, Map.to_list(params), opts)
#   end

#   defp reduce_entries(query, params, opts) when is_list(params) do
#     if Keyword.keyword?(params) do
#       Enum.reduce(params, query, fn {key, params}, query_acc ->
#         apply_entry(query_acc, key, params, opts)
#       end)
#     else
#       Enum.reduce(params, query, fn
#         {key, params}, query_acc ->
#           apply_entry(query_acc, key, params, opts)

#         other, query_acc ->
#           Logger.warning(
#             @logger_prefix,
#             "Expected :with_named_binding params to be a map or keyword list, got: #{inspect(other)}"
#           )

#           query_acc
#       end)
#     end
#   end

#   defp reduce_entries(query, value, _opts) do
#     Logger.warning(
#       @logger_prefix,
#       "Expected :with_named_binding params to be a map or keyword list, got: #{inspect(value)}"
#     )

#     query
#   end

#   defp apply_entry(query, key, params, opts)
#        when is_atom(key) and is_map(params) and not is_struct(params) do
#     apply_entry(query, key, Map.to_list(params), opts)
#   end

#   defp apply_entry(query, key, params, opts)
#        when is_atom(key) and is_list(params) do
#     cond do
#       not Keyword.keyword?(params) ->
#         warn_invalid_params(key, params)
#         query

#       Query.has_named_binding?(query, key) ->
#         query

#       true ->
#         new_query = CommonFilters.convert_params_to_filter(query, params, opts)

#         if Query.has_named_binding?(new_query, key) do
#           new_query
#         else
#           Logger.warning(
#             @logger_prefix,
#             "callback function for with_named_binding/3 should create a named binding for key #{inspect(key)}"
#           )

#           query
#         end
#     end
#   end

#   defp apply_entry(query, key, params, _opts) when is_atom(key) do
#     warn_invalid_params(key, params)

#     query
#   end

#   defp apply_entry(query, key, _params, _opts) do
#     Logger.warning(
#       @logger_prefix,
#       "Expected :with_named_binding key to be an atom, got: #{inspect(key)}"
#     )

#     query
#   end

#   defp warn_invalid_params(key, params) do
#     Logger.warning(
#       @logger_prefix,
#       "Expected :with_named_binding params for #{inspect(key)} to be a map or keyword list, got: #{inspect(params)}"
#     )
#   end
# end

defmodule EctoShorts.CommonFilters.WithNamedBinding do
  alias Ecto.Query
  alias EctoShorts.CommonFilters
  alias EctoShorts.Logger

  @logger_prefix "EctoShorts.CommonFilters.WithNamedBinding"

  def build_query(:with_named_binding, _source, query, _selected_binding, params, opts) do
    reduce_entries(query, params, opts)
  end

  defp reduce_entries(query, params, opts) when is_map(params) and not is_struct(params) do
    reduce_entries(query, Map.to_list(params), opts)
  end

  defp reduce_entries(query, params, opts) when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {key, params}, query_acc ->
        apply_entry(query_acc, key, params, opts)
      end)
    else
      Enum.reduce(params, query, fn
        {key, params}, query_acc ->
          apply_entry(query_acc, key, params, opts)

        other, query_acc ->
          Logger.warning(
            @logger_prefix,
            "Expected :with_named_binding params to be a map or keyword list, got: #{inspect(other)}"
          )

          query_acc
      end)
    end
  end

  defp reduce_entries(query, value, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_named_binding params to be a map or keyword list, got: #{inspect(value)}"
    )

    query
  end

  defp apply_entry(query, key, params, opts)
       when is_atom(key) and is_map(params) and not is_struct(params) do
    apply_entry(query, key, Map.to_list(params), opts)
  end

  defp apply_entry(query, key, params, opts)
       when is_atom(key) and is_list(params) do
    cond do
      not Keyword.keyword?(params) ->
        warn_invalid_params(key, params)
        query

      Query.has_named_binding?(query, key) ->
        query

      true ->
        new_query = CommonFilters.convert_params_to_filter(query, params, opts)

        if Query.has_named_binding?(new_query, key) do
          new_query
        else
          Logger.warning(
            @logger_prefix,
            "callback function for with_named_binding/3 should create a named binding for key #{inspect(key)}"
          )

          query
        end
    end
  end

  defp apply_entry(query, key, params, _opts) when is_atom(key) do
    warn_invalid_params(key, params)

    query
  end

  defp apply_entry(query, key, _params, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_named_binding key to be an atom, got: #{inspect(key)}"
    )

    query
  end

  defp warn_invalid_params(key, params) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_named_binding params for #{inspect(key)} to be a map or keyword list, got: #{inspect(params)}"
    )
  end
end
