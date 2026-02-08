defmodule EctoShorts.CommonFilters.Preload do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @logger_prefix "EctoShorts.CommonFilters.Preload"

  @doc "Builds preload expressions for plain and binding-aware preload params."
  def build(_source, _filter_op, query, _binding_selector, term, _opts) do
    term
    |> normalize_preload_term()
    |> split_preload_entries()
    |> apply_preloads(query)
  end

  defp normalize_preload_term(term) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> normalize_preload_term()
  end

  defp normalize_preload_term(term) when is_list(term) do
    Enum.map(term, fn
      {key, value} -> {key, normalize_preload_term(value)}
      value -> normalize_preload_term(value)
    end)
  end

  defp normalize_preload_term(term), do: term

  defp split_preload_entries(term) do
    term
    |> List.wrap()
    |> Enum.reduce({[], []}, fn entry, {plain_entries, binding_entries} ->
      case split_preload_entry(entry) do
        {:plain, value} ->
          {[value | plain_entries], binding_entries}

        {:binding_aware, assoc, binding_selector, nested_payload} ->
          {plain_entries, [{assoc, binding_selector, nested_payload} | binding_entries]}

        :skip ->
          {plain_entries, binding_entries}
      end
    end)
    |> then(fn {plain_entries, binding_entries} ->
      {Enum.reverse(plain_entries), Enum.reverse(binding_entries)}
    end)
  end

  defp split_preload_entry({assoc, payload}) when is_atom(assoc) do
    case binding_aware_assoc_payload(payload) do
      {:ok, binding_selector, nested_payload} ->
        {:binding_aware, assoc, binding_selector, nested_payload}

      :not_binding_aware ->
        {:plain, {assoc, payload}}

      :skip ->
        :skip
    end
  end

  defp split_preload_entry({assoc, _payload}) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected preload association key to be an atom, got: #{inspect(assoc)}"
    )

    :skip
  end

  defp split_preload_entry(value) when is_atom(value) do
    {:plain, value}
  end

  defp split_preload_entry(value) when is_list(value) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected preload list entries to be atoms or {assoc, value} tuples, got: #{inspect(value)}"
    )

    :skip
  end

  defp split_preload_entry(value) do
    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected preload params to be list/map/atom entries, got: #{inspect(value)}"
    )

    :skip
  end

  defp binding_aware_assoc_payload(payload) when is_map(payload) and not is_struct(payload) do
    binding_aware_assoc_payload(Map.to_list(payload))
  end

  defp binding_aware_assoc_payload(payload) when is_list(payload) do
    if Keyword.keyword?(payload) and Keyword.has_key?(payload, :binding) do
      binding_options = Keyword.get(payload, :binding)
      nested_payload = Keyword.delete(payload, :binding)

      case parse_binding_selector(binding_options) do
        {:ok, binding_selector} ->
          nested_payload =
            case nested_payload do
              [] -> nil
              entries -> entries
            end

          {:ok, binding_selector, nested_payload}

        :error ->
          :skip
      end
    else
      :not_binding_aware
    end
  end

  defp binding_aware_assoc_payload(_payload), do: :not_binding_aware

  defp parse_binding_selector(binding_options) when is_map(binding_options) do
    parse_binding_selector(Map.to_list(binding_options))
  end

  defp parse_binding_selector(binding_options) when is_list(binding_options) do
    if Keyword.keyword?(binding_options) do
      has_as? = Keyword.has_key?(binding_options, :as)
      has_at? = Keyword.has_key?(binding_options, :at)

      cond do
        has_as? and has_at? ->
          warn_invalid_binding_selector(binding_options)
          :error

        has_as? ->
          case Keyword.get(binding_options, :as) do
            alias_name when is_atom(alias_name) ->
              {:ok, {:as, alias_name}}

            value ->
              warn_invalid_binding_selector(binding_options, value)
              :error
          end

        has_at? ->
          case Keyword.get(binding_options, :at) do
            index when is_integer(index) ->
              {:ok, {:at, index}}

            value ->
              warn_invalid_binding_selector(binding_options, value)
              :error
          end

        true ->
          warn_invalid_binding_selector(binding_options)
          :error
      end
    else
      warn_invalid_binding_selector(binding_options)
      :error
    end
  end

  defp parse_binding_selector(binding_options) do
    warn_invalid_binding_selector(binding_options)
    :error
  end

  defp warn_invalid_binding_selector(binding_options, value \\ nil) do
    suffix =
      if is_nil(value) do
        ""
      else
        ", invalid selector value: #{inspect(value)}"
      end

    EctoShorts.Logger.warning(
      @logger_prefix,
      "Expected :binding to be [as: atom()] or [at: integer()], got: #{inspect(binding_options)}#{suffix}"
    )
  end

  defp apply_preloads({plain_entries, binding_entries}, query) do
    query
    |> apply_plain_preloads(plain_entries)
    |> apply_binding_preloads(binding_entries)
  end

  defp apply_plain_preloads(query, []), do: query

  defp apply_plain_preloads(query, entries) do
    Query.preload(query, ^entries)
  end

  defp apply_binding_preloads(query, []), do: query

  defp apply_binding_preloads(query, entries) do
    Enum.reduce(entries, query, fn {assoc, binding_selector, nested_payload}, query_acc ->
      apply_binding_preload(query_acc, binding_selector, assoc, nested_payload)
    end)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp apply_binding_preload(
             query,
             unquote(quoted_binding_head),
             assoc,
             nil
           )
           when is_atom(assoc) do
        Query.preload(
          query,
          [unquote_splicing(quoted_binding_body)],
          [{^assoc, unquote(target_binding_var)}]
        )
      end

      defp apply_binding_preload(
             query,
             unquote(quoted_binding_head),
             assoc,
             nested_payload
           )
           when is_atom(assoc) do
        if is_list(nested_payload) and Keyword.keyword?(nested_payload) do
          Enum.reduce(nested_payload, query, fn nested_entry, query_acc ->
            Query.preload(
              query_acc,
              [unquote_splicing(quoted_binding_body)],
              [{^assoc, {unquote(target_binding_var), ^nested_entry}}]
            )
          end)
        else
          Query.preload(
            query,
            [unquote_splicing(quoted_binding_body)],
            [{^assoc, {unquote(target_binding_var), ^nested_payload}}]
          )
        end
      end
  end
end
