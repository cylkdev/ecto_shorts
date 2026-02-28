defmodule EctoShorts.CommonFilters.WithNamedBinding do
  @moduledoc """
  Builds `:with_named_binding` expressions from data-driven params.

  Accepts keyword lists of `{binding_key, binding_params}` entries. Each
  entry calls `Ecto.Query.with_named_binding/3` with a callback that
  converts `binding_params` to filters via `CommonFilters.convert_params_to_filter/3`.
  """

  alias Ecto.Query
  alias EctoShorts.CommonFilters
  alias EctoShorts.Logger

  require Ecto.Query

  @logger_prefix "EctoShorts.CommonFilters.WithNamedBinding"

  @doc false
  def build(_schema_source, :with_named_binding, query, _binding_selector, params, opts) do
    reduce_entries(query, params, opts)
  end

  defp reduce_entries(query, params, opts) when is_map(params) and not is_struct(params) do
    reduce_entries(query, Map.to_list(params), opts)
  end

  defp reduce_entries(query, params, opts) when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {binding_key, binding_params}, query_acc ->
        apply_entry(query_acc, binding_key, binding_params, opts)
      end)
    else
      Enum.reduce(params, query, fn entry, query_acc ->
        reduce_entries(query_acc, entry, opts)
      end)
    end
  end

  defp reduce_entries(query, {binding_key, binding_params}, opts) do
    apply_entry(query, binding_key, binding_params, opts)
  end

  defp reduce_entries(query, value, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_named_binding params to be a map or keyword list, got: #{inspect(value)}"
    )

    query
  end

  defp apply_entry(query, binding_key, binding_params, opts)
       when is_atom(binding_key) and is_map(binding_params) and not is_struct(binding_params) do
    apply_entry(query, binding_key, Map.to_list(binding_params), opts)
  end

  defp apply_entry(query, binding_key, binding_params, opts)
       when is_atom(binding_key) and is_list(binding_params) do
    if Keyword.keyword?(binding_params) do
      Query.with_named_binding(query, binding_key, fn query_acc ->
        CommonFilters.convert_params_to_filter(query_acc, binding_params, opts)
      end)
    else
      warn_invalid_binding_params(binding_key, binding_params)
      query
    end
  end

  defp apply_entry(query, binding_key, binding_params, _opts) when is_atom(binding_key) do
    warn_invalid_binding_params(binding_key, binding_params)

    query
  end

  defp apply_entry(query, binding_key, _binding_params, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_named_binding key to be an atom, got: #{inspect(binding_key)}"
    )

    query
  end

  defp warn_invalid_binding_params(binding_key, binding_params) do
    Logger.warning(
      @logger_prefix,
      "Expected :with_named_binding params for #{inspect(binding_key)} to be a map or keyword list, got: #{inspect(binding_params)}"
    )
  end
end
