defmodule EctoShorts.CommonFilters.BindingParams do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Routes `:bind` params to the correct binding selector.

  When a filter params map contains a `:bind` key, this module unpacks the
  nested `{:as, target}` or `{:at, target}` selectors and dispatches each
  scoped param set back to `CommonFilters.reduce_filter_params/6` with the
  appropriate binding selector. Also provides `reduce_submodule_bind_params/3`
  for use by other query builder submodules (e.g., OrderBy, GroupBy, Windows).
  """

  alias EctoShorts.CommonFilters
  alias EctoShorts.Logger

  @logger_prefix "EctoShorts.CommonFilters.BindingParams"
  @binding_selector_modes [:as, :at]

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
    if binding_mode in @binding_selector_modes do
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
    else
      Logger.warning(
        @logger_prefix,
        "Expected :bind keys to be one of #{inspect(@binding_selector_modes)}, got: #{inspect(binding_mode)}"
      )

      query
    end
  end

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
  def normalize_bind_params(bind_params)
      when is_map(bind_params) and not is_struct(bind_params) do
    normalize_bind_params(Map.to_list(bind_params))
  end

  def normalize_bind_params(bind_params) when is_list(bind_params) do
    if Keyword.keyword?(bind_params) do
      Enum.flat_map(bind_params, fn
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

  def normalize_bind_params(bind_params) do
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
        CommonFilters.create_schema_filter(
          schema_source,
          query,
          {:at, bind_index},
          filter,
          params,
          opts
        )

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
end
