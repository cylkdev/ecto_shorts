defmodule EctoShorts.CommonFilters.BindingParams do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Routes `:bind` params to the correct binding selector.

  When a filter params map contains a `:bind` key, this module unpacks the
  nested `{:as, target}` or `{:at, target}` selectors and dispatches each
  scoped param set back to `CommonFilters.create_schema_filter/6` with the
  appropriate binding selector.

  ## Binding modes

  * `:as` - named binding. Wraps `{alias, params}` pairs.
  * `:at` - positional binding. Wraps `{position, params}` pairs.
  * `:first` - flat mode. Targets the root `from` binding (position 1).
    Wraps filter params directly.
  * `:last` - flat mode. Targets the highest positional binding in the
    query (last join, or `from` if no joins). Wraps filter params directly.
  """

  alias EctoShorts.CommonFilters
  alias EctoShorts.CommonQuery
  alias EctoShorts.Config
  alias EctoShorts.Logger

  @logger_prefix "EctoShorts.CommonFilters.BindingParams"
  @binding_selector_modes [:as, :at, :first, :last]
  @flat_binding_modes [:first, :last]

  def build_binding_params(
        schema_source,
        query,
        binding_selector,
        filter_op,
        bind_params,
        opts
      )
      when is_map(bind_params) and not is_struct(bind_params) do
    build_binding_params(
      schema_source,
      query,
      binding_selector,
      filter_op,
      Map.to_list(bind_params),
      opts
    )
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
    if Keyword.keyword?(bind_params) do
      Enum.reduce(bind_params, query, fn
        {binding_mode, scoped_params}, query_acc ->
          reduce_scoped_bind_params(
            schema_source,
            query_acc,
            binding_mode,
            scoped_params,
            filter_op,
            opts
          )

        entry, query_acc ->
          Logger.warning(
            @logger_prefix,
            "Expected :bind entries to be {mode, params} tuples, got: #{inspect(entry)}"
          )

          query_acc
      end)
    else
      Logger.warning(
        @logger_prefix,
        "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
      )

      query
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
      "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
    )

    query
  end

  defp reduce_scoped_bind_params(
         schema_source,
         query,
         binding_mode,
         scoped_params,
         filter_op,
         opts
       ) do
    cond do
      binding_mode in @flat_binding_modes ->
        reduce_flat_bind_params(schema_source, query, binding_mode, scoped_params, filter_op, opts)

      binding_mode in @binding_selector_modes ->
        case scoped_params do
          params when is_map(params) and not is_struct(params) ->
            reduce_scoped_entries(schema_source, query, binding_mode, Map.to_list(params), filter_op, opts)

          params when is_list(params) ->
            reduce_scoped_entries(schema_source, query, binding_mode, params, filter_op, opts)

          params ->
            Logger.warning(
              @logger_prefix,
              "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(params)}"
            )

            query
        end

      true ->
        Logger.warning(
          @logger_prefix,
          "Expected :bind keys to be one of #{inspect(@binding_selector_modes)}, got: #{inspect(binding_mode)}"
        )

        query
    end
  end

  defp reduce_flat_bind_params(schema_source, query, binding_mode, scoped_params, filter_op, opts) do
    case scoped_params do
      params when is_map(params) and not is_struct(params) ->
        binding_selector = resolve_flat_binding(binding_mode, query)

        CommonFilters.create_schema_filter(
          schema_source,
          query,
          binding_selector,
          filter_op,
          Map.to_list(params),
          opts
        )

      params when is_list(params) ->
        binding_selector = resolve_flat_binding(binding_mode, query)

        CommonFilters.create_schema_filter(
          schema_source,
          query,
          binding_selector,
          filter_op,
          params,
          opts
        )

      params ->
        Logger.warning(
          @logger_prefix,
          "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(params)}"
        )

        query
    end
  end

  defp resolve_flat_binding(:first, _query), do: {:at, 1}
  defp resolve_flat_binding(:last, query), do: {:at, CommonQuery.query_binding_count(query)}

  defp reduce_scoped_entries(schema_source, query, binding_mode, entries, filter_op, opts) do
    Enum.reduce(entries, query, fn
      {binding_target, params}, query_acc ->
        reduce_binding_params(
          schema_source,
          query_acc,
          {binding_mode, binding_target},
          filter_op,
          params,
          opts
        )

      entry, query_acc ->
        Logger.warning(
          @logger_prefix,
          "Expected :bind -> #{inspect(binding_mode)} entries to be {target, params} tuples, got: #{inspect(entry)}"
        )

        query_acc
    end)
  end

  @doc false
  def normalize_bind_params(bind_params, query \\ nil)

  def normalize_bind_params(bind_params, query)
      when is_map(bind_params) and not is_struct(bind_params) do
    normalize_bind_params(Map.to_list(bind_params), query)
  end

  def normalize_bind_params(bind_params, query) when is_list(bind_params) do
    if Keyword.keyword?(bind_params) do
      Enum.flat_map(bind_params, fn
        {binding_mode, scoped_params} when binding_mode in @flat_binding_modes ->
          normalize_flat_entries(binding_mode, scoped_params, query)

        {binding_mode, scoped_params} when binding_mode in @binding_selector_modes ->
          normalize_scoped_entries(binding_mode, scoped_params)

        {binding_mode, _scoped_params} ->
          Logger.warning(
            @logger_prefix,
            "Expected :bind keys to be one of #{inspect(@binding_selector_modes)}, got: #{inspect(binding_mode)}"
          )

          []
      end)
    else
      Logger.warning(
        @logger_prefix,
        "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
      )

      []
    end
  end

  def normalize_bind_params(bind_params, _query) do
    Logger.warning(
      @logger_prefix,
      "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
    )

    []
  end

  defp reduce_binding_params(
         schema_source,
         query,
         {binding_mode, binding_target},
         filter,
         params,
         opts
       ) do
    case {binding_mode, binding_target} do
      {:as, bind_alias} when is_atom(bind_alias) ->
        CommonFilters.create_schema_filter(
          schema_source,
          query,
          {:as, bind_alias},
          filter,
          params,
          opts
        )

      {:at, bind_index} when is_integer(bind_index) ->
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
            {:at, bind_index},
            filter,
            params,
            opts
          )
        end

      binding_selector ->
        Logger.warning(
          @logger_prefix,
          "Expected binding selector to be one of {:as, atom()} or {:at, integer()}, got: #{inspect(binding_selector)}"
        )

        query
    end
  end

  defp normalize_scoped_entries(binding_mode, scoped_params) do
    case scoped_params do
      value when is_map(value) and not is_struct(value) ->
        normalize_entries(binding_mode, Map.to_list(value))

      value when is_list(value) ->
        normalize_entries(binding_mode, value)

      value ->
        Logger.warning(
          @logger_prefix,
          "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(value)}"
        )

        []
    end
  end

  defp normalize_entries(binding_mode, entries) do
    Enum.flat_map(entries, fn
      {binding_target, next_value} ->
        [{{binding_mode, binding_target}, next_value}]

      entry ->
        Logger.warning(
          @logger_prefix,
          "Expected :bind -> #{inspect(binding_mode)} entries to be {target, params} tuples, got: #{inspect(entry)}"
        )

        []
    end)
  end

  defp normalize_flat_entries(binding_mode, scoped_params, query) do
    case scoped_params do
      params when is_map(params) and not is_struct(params) ->
        binding_selector = resolve_flat_binding(binding_mode, query)
        [{binding_selector, Map.to_list(params)}]

      params when is_list(params) ->
        binding_selector = resolve_flat_binding(binding_mode, query)
        [{binding_selector, params}]

      params ->
        Logger.warning(
          @logger_prefix,
          "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(params)}"
        )

        []
    end
  end
end
