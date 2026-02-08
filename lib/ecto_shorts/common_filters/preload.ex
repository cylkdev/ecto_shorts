defmodule EctoShorts.CommonFilters.Preload do
  @moduledoc false

  alias Ecto.Query
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  def build(_source, _filter_op, query, _binding_selector, term, _opts) do
    term
    |> normalize()
    |> List.wrap()
    |> Enum.reduce({query, []}, &process_entry/2)
    |> then(fn {q, bindings} ->
      Enum.reduce(Enum.reverse(bindings), q, fn {assoc, selector, nested}, acc ->
        apply_binding(acc, selector, assoc, nested)
      end)
    end)
  end

  defp process_entry({assoc, payload}, {q, bindings}) when is_atom(assoc) and is_list(payload) do
    if Keyword.keyword?(payload) and Keyword.has_key?(payload, :binding) do
      case parse_binding_selector(Keyword.get(payload, :binding)) do
        {:ok, selector} ->
          nested =
            case Keyword.delete(payload, :binding) do
              [] -> nil
              value -> value
            end

          {q, [{assoc, selector, nested} | bindings]}

        :error ->
          {q, bindings}
      end
    else
      {Query.preload(q, [{^assoc, ^payload}]), bindings}
    end
  end

  defp process_entry({assoc, payload}, {q, bindings}) when is_atom(assoc) do
    {Query.preload(q, [{^assoc, ^payload}]), bindings}
  end

  defp process_entry(atom, {q, bindings}) when is_atom(atom) do
    {Query.preload(q, [^atom]), bindings}
  end

  defp process_entry(_, acc), do: acc

  defp normalize(term) when is_map(term) and not is_struct(term),
    do: Enum.map(Map.to_list(term), fn {k, v} -> {k, normalize(v)} end)

  defp normalize(term) when is_list(term),
    do:
      Enum.map(term, fn
        {k, v} -> {k, normalize(v)}
        v -> v
      end)

  defp normalize(term), do: term

  defp parse_binding_selector(binding_options) when is_map(binding_options) do
    parse_binding_selector(Map.to_list(binding_options))
  end

  defp parse_binding_selector(binding_options) when is_list(binding_options) do
    cond do
      Keyword.keyword?(binding_options) and Keyword.has_key?(binding_options, :as) and
          not Keyword.has_key?(binding_options, :at) ->
        case Keyword.get(binding_options, :as) do
          alias_name when is_atom(alias_name) -> {:ok, {:as, alias_name}}
          _ -> :error
        end

      Keyword.keyword?(binding_options) and Keyword.has_key?(binding_options, :at) and
          not Keyword.has_key?(binding_options, :as) ->
        case Keyword.get(binding_options, :at) do
          index when is_integer(index) -> {:ok, {:at, index}}
          _ -> :error
        end

      true ->
        :error
    end
  end

  defp parse_binding_selector(_binding_options), do: :error

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp apply_binding(query, unquote(quoted_binding_head), assoc, nil) do
        Query.preload(query, [unquote_splicing(quoted_binding_body)], [
          {^assoc, unquote(target_binding_var)}
        ])
      end

      defp apply_binding(query, unquote(quoted_binding_head), assoc, nested) do
        if is_list(nested) and Keyword.keyword?(nested) do
          Enum.reduce(nested, query, fn nested_entry, query_acc ->
            Query.preload(query_acc, [unquote_splicing(quoted_binding_body)], [
              {^assoc, {unquote(target_binding_var), ^nested_entry}}
            ])
          end)
        else
          Query.preload(query, [unquote_splicing(quoted_binding_body)], [
            {^assoc, {unquote(target_binding_var), ^nested}}
          ])
        end
      end
  end
end
