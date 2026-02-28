defmodule EctoShorts.CommonFilters.BindParams do
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

  @binding_selector_modes [:as, :at]

  def reduce_bind_params(
        schema_source,
        query,
        binding_selector,
        filter_op,
        bind_params,
        opts
      )
      when is_map(bind_params) and not is_struct(bind_params) do
    reduce_bind_params(
      schema_source,
      query,
      binding_selector,
      filter_op,
      Map.to_list(bind_params),
      opts
    )
  end

  def reduce_bind_params(
        schema_source,
        query,
        _binding_selector,
        filter_op,
        bind_params,
        opts
      )
      when is_list(bind_params) do
    unless Keyword.keyword?(bind_params) do
      raise ArgumentError,
            "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
    end

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

      entry, _query_acc ->
        raise ArgumentError,
              "Expected :bind entries to be {mode, params} tuples, got: #{inspect(entry)}"
    end)
  end

  def reduce_bind_params(
        _schema_source,
        _query,
        _binding_selector,
        _filter_op,
        bind_params,
        _opts
      ) do
    raise ArgumentError,
          "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
  end

  defp reduce_scoped_bind_params(
         schema_source,
         query,
         binding_mode,
         scoped_params,
         filter_op,
         opts
       ) do
    unless binding_mode in @binding_selector_modes do
      raise ArgumentError,
            "Expected :bind keys to be one of #{inspect(@binding_selector_modes)}, got: #{inspect(binding_mode)}"
    end

    scoped_entries =
      case scoped_params do
        params when is_map(params) and not is_struct(params) ->
          Map.to_list(params)

        params when is_list(params) ->
          params

        params ->
          raise ArgumentError,
                "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(params)}"
      end

    Enum.reduce(scoped_entries, query, fn
      {binding_target, params}, query_acc ->
        reduce_binding_params(
          schema_source,
          query_acc,
          {binding_mode, binding_target},
          filter_op,
          params,
          opts
        )

      entry, _query_acc ->
        raise ArgumentError,
              "Expected :bind -> #{inspect(binding_mode)} entries to be {target, params} tuples, got: #{inspect(entry)}"
    end)
  end

  @doc false
  def reduce_submodule_bind_params(query, bind_params, callback)
      when is_map(bind_params) and not is_struct(bind_params) do
    reduce_submodule_bind_params(query, Map.to_list(bind_params), callback)
  end

  def reduce_submodule_bind_params(query, bind_params, callback) when is_list(bind_params) do
    if Keyword.keyword?(bind_params) do
      Enum.reduce(bind_params, query, fn
        {binding_mode, scoped_params}, query_acc when binding_mode in @binding_selector_modes ->
          scoped_params =
            case scoped_params do
              value when is_map(value) and not is_struct(value) ->
                Map.to_list(value)

              value when is_list(value) ->
                value

              value ->
                raise ArgumentError,
                      "Expected :bind -> #{inspect(binding_mode)} payload to be a map or keyword list, got: #{inspect(value)}"
            end

          Enum.reduce(scoped_params, query_acc, fn
            {binding_target, next_value}, q ->
              callback.(q, {binding_mode, binding_target}, next_value)

            entry, _q ->
              raise ArgumentError,
                    "Expected :bind -> #{inspect(binding_mode)} entries to be {target, params} tuples, got: #{inspect(entry)}"
          end)

        {binding_mode, _scoped_params}, _query_acc ->
          raise ArgumentError,
                "Expected :bind keys to be one of #{inspect(@binding_selector_modes)}, got: #{inspect(binding_mode)}"
      end)
    else
      raise ArgumentError,
            "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
    end
  end

  def reduce_submodule_bind_params(_query, bind_params, _callback) do
    raise ArgumentError,
          "Expected :bind payload to be a keyword list or map, got: #{inspect(bind_params)}"
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
        raise ArgumentError,
              "Expected binding selector to be one of {:as, atom()} or {:at, integer()}, got: #{inspect(binding_selector)}"
    end
  end
end
