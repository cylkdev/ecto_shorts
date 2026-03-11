defmodule EctoShorts.CommonFilters.WithNamedBinding do
  alias Ecto.Query
  alias EctoShorts.CommonFilters
  alias EctoShorts.Utils

  @logger_prefix "EctoShorts.CommonFilters.WithNamedBinding"

  def build_query(:with_named_binding, _source, query, _selected_binding, term, opts) do
    params = Utils.normalize_input(term)

    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn
        {key, value}, query_acc ->
          apply_entry(query_acc, key, value, opts)

        other, query_acc ->
          EctoShorts.Logger.warning(
            @logger_prefix,
            "Expected :with_named_binding params to be a map or keyword list, got: #{inspect(other)}"
          )

          query_acc
      end)
    else
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected :with_named_binding params to be a map or keyword list, got: #{inspect(term)}"
      )

      query
    end
  end

  defp apply_entry(query, key, params, opts) do
    cond do
      not is_atom(key) ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Expected :with_named_binding key to be an atom, got: #{inspect(key)}"
        )

        query

      Query.has_named_binding?(query, key) ->
        query

      true ->
        new_query = CommonFilters.convert_params_to_filter(query, params, opts)

        if Query.has_named_binding?(new_query, key) do
          new_query
        else
          EctoShorts.Logger.warning(
            @logger_prefix,
            "callback function for with_named_binding/3 should create a named binding for key #{inspect(key)}"
          )

          query
        end
    end
  end
end
