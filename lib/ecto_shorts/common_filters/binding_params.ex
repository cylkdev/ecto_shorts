defmodule EctoShorts.CommonFilters.BindingParams do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Routes `:bind` params to the correct binding selector.

  When a filter params map contains a `:bind` key, this module unpacks
  flat bind entries and dispatches each one back to
  `CommonFilters.create_schema_filter/6` with the appropriate binding
  selector.

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

  alias EctoShorts.CommonFilters
  alias EctoShorts.CommonQuery
  alias EctoShorts.Config
  alias EctoShorts.Logger

  @logger_prefix "EctoShorts.CommonFilters.BindingParams"

  def build_binding_params(
        schema_source,
        query,
        _binding_selector,
        filter_op,
        bind_params,
        opts
      )
      when is_map(bind_params) and not is_struct(bind_params) do
    apply_flat_bind_entry(schema_source, query, bind_params, filter_op, opts)
  end

  def build_binding_params(
        schema_source,
        query,
        _binding_selector,
        filter_op,
        bind_params,
        opts
      )
      when is_list(bind_params) do
    if Keyword.keyword?(bind_params) and
         (Keyword.has_key?(bind_params, :as) or Keyword.has_key?(bind_params, :at)) do
      apply_flat_bind_entry(schema_source, query, Map.new(bind_params), filter_op, opts)
    else
      Enum.reduce(bind_params, query, fn
        entry, query_acc when is_map(entry) and not is_struct(entry) ->
          apply_flat_bind_entry(schema_source, query_acc, entry, filter_op, opts)

        entry, query_acc when is_list(entry) ->
          if Keyword.keyword?(entry) do
            apply_flat_bind_entry(schema_source, query_acc, Map.new(entry), filter_op, opts)
          else
            Logger.warning(
              @logger_prefix,
              "Expected :bind entry to be a map or keyword list, got: #{inspect(entry)}"
            )

            query_acc
          end

        entry, query_acc ->
          Logger.warning(
            @logger_prefix,
            "Expected :bind entry to be a map, got: #{inspect(entry)}"
          )

          query_acc
      end)
    end
  end

  def build_binding_params(
        _schema_source,
        query,
        _binding_selector,
        _filter_op,
        bind_params,
        _opts
      ) do
    Logger.warning(
      @logger_prefix,
      "Expected :bind payload to be a map or list of maps, got: #{inspect(bind_params)}"
    )

    query
  end

  defp apply_flat_bind_entry(schema_source, query, entry, filter_op, opts) do
    entry_kw = if is_map(entry), do: Map.to_list(entry), else: entry

    cond do
      Keyword.has_key?(entry_kw, :as) ->
        {bind_alias, filters} = Keyword.pop(entry_kw, :as)
        apply_as_binding(schema_source, query, bind_alias, filters, filter_op, opts)

      Keyword.has_key?(entry_kw, :at) ->
        {bind_target, filters} = Keyword.pop(entry_kw, :at)

        case resolve_at_target(bind_target, query) do
          :error -> query
          binding_selector -> apply_at_binding(schema_source, query, binding_selector, filters, filter_op, opts)
        end

      true ->
        Logger.warning(
          @logger_prefix,
          "Expected :bind entry to have an :as or :at key, got: #{inspect(entry)}"
        )

        query
    end
  end

  defp apply_as_binding(schema_source, query, bind_alias, filters, filter_op, opts)
       when is_atom(bind_alias) and not is_nil(bind_alias) do
    CommonFilters.create_schema_filter(
      schema_source,
      query,
      {:as, bind_alias},
      filter_op,
      filters,
      opts
    )
  end

  defp apply_as_binding(_schema_source, query, bind_alias, _filters, _filter_op, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :as value to be a non-nil atom, got: #{inspect(bind_alias)}"
    )

    query
  end

  defp apply_at_binding(schema_source, query, {:at, bind_index} = binding_selector, filters, filter_op, opts)
       when is_integer(bind_index) do
    max = Config.max_binding_positions()

    if bind_index > max do
      Logger.warning(
        @logger_prefix,
        "Binding position #{bind_index} exceeds the configured :max_binding_positions (#{max}). " <>
          "Increase :max_binding_positions in your config to support more positional bindings."
      )

      query
    else
      CommonFilters.create_schema_filter(
        schema_source,
        query,
        binding_selector,
        filter_op,
        filters,
        opts
      )
    end
  end

  defp apply_at_binding(_schema_source, query, binding_selector, _filters, _filter_op, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :at value to be an integer, :first, or :last, got: #{inspect(elem(binding_selector, 1))}"
    )

    query
  end

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
