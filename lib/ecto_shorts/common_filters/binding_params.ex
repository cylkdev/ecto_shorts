defmodule EctoShorts.CommonFilters.BindingParams do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Normalizes `:bind` params into `{binding_selector, value}` tuples.

  When a filter params map contains a `:bind` key, this module unpacks
  flat bind entries and normalizes each one into a
  `{binding_selector, value}` tuple that callers reduce over to apply
  filters or query operations to the correct binding.

  Each bind entry is a flat map (or keyword list) containing an `:as`
  or `:at` key that identifies the binding target. All other keys in
  the entry are treated as filters or query operations.

  ## Binding keys

  * `:as` - named binding. The value is an atom alias
    (e.g. `%{as: :post, published: true}`).

  * `:at` - positional binding. The value is an integer position, or
    one of the atoms `:first` / `:last`.
    * An integer targets the binding at that position
      (e.g. `%{at: 2, published: true}`).
    * `:first` targets the root `from` binding (position 1).
    * `:last` targets the highest positional binding in the query
      (last join, or `from` if no joins).

  ## Multiple bindings

  Pass a list of flat maps to target multiple bindings:

      %{bind: [%{as: :post, published: true}, %{as: :author, first_name: "John"}]}
  """

  alias EctoShorts.CommonQuery
  alias EctoShorts.Logger

  @logger_prefix "EctoShorts.CommonFilters.BindingParams"

  defp resolve_at_target(:first, _query), do: {:at, 1}
  defp resolve_at_target(:last, query), do: {:at, CommonQuery.query_binding_count(query)}
  defp resolve_at_target(index, _query) when is_integer(index), do: {:at, index}

  defp resolve_at_target(other, _query) do
    Logger.warning(
      @logger_prefix,
      "Expected :at value to be an integer, :first, or :last, got: #{inspect(other)}"
    )

    :error
  end

  @doc false
  def normalize_bind_params(bind_params, query \\ nil)

  def normalize_bind_params(bind_params, query)
      when is_map(bind_params) and not is_struct(bind_params) do
    [normalize_flat_entry(bind_params, query)]
  end

  def normalize_bind_params(bind_params, query) when is_list(bind_params) do
    if Keyword.keyword?(bind_params) and
         (Keyword.has_key?(bind_params, :as) or Keyword.has_key?(bind_params, :at)) do
      [normalize_flat_entry(Map.new(bind_params), query)]
    else
      Enum.flat_map(bind_params, fn
        entry when is_map(entry) and not is_struct(entry) ->
          [normalize_flat_entry(entry, query)]

        entry when is_list(entry) ->
          if Keyword.keyword?(entry) do
            [normalize_flat_entry(Map.new(entry), query)]
          else
            Logger.warning(
              @logger_prefix,
              "Expected :bind entry to be a map or keyword list, got: #{inspect(entry)}"
            )

            []
          end

        entry ->
          Logger.warning(
            @logger_prefix,
            "Expected :bind entry to be a map, got: #{inspect(entry)}"
          )

          []
      end)
    end
  end

  def normalize_bind_params(bind_params, _query) do
    Logger.warning(
      @logger_prefix,
      "Expected :bind payload to be a map or list of maps, got: #{inspect(bind_params)}"
    )

    []
  end

  defp normalize_flat_entry(entry, query) do
    cond do
      Map.has_key?(entry, :as) ->
        {bind_alias, rest} = Map.pop(entry, :as)
        value = extract_bind_value(rest)
        {{:as, bind_alias}, value}

      Map.has_key?(entry, :at) ->
        {bind_target, rest} = Map.pop(entry, :at)
        value = extract_bind_value(rest)

        case resolve_at_target(bind_target, query) do
          :error -> {{:as, nil}, []}
          binding_selector -> {binding_selector, value}
        end

      true ->
        Logger.warning(
          @logger_prefix,
          "Expected :bind entry to have an :as or :at key, got: #{inspect(entry)}"
        )

        {{:as, nil}, []}
    end
  end

  defp extract_bind_value(rest) when is_map(rest) do
    if Map.has_key?(rest, :value) do
      Map.get(rest, :value)
    else
      Map.to_list(rest)
    end
  end

  defp extract_bind_value(rest), do: rest
end
