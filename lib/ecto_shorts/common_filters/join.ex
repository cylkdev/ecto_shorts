defmodule EctoShorts.CommonFilters.Join do
  @moduledoc false
  alias EctoShorts.Compiler
  alias EctoShorts.Dynamics

  alias Ecto.Query

  require EctoShorts.Compiler
  @logger_prefix "EctoShorts.CommonFilters.Join"

  require Ecto.Query

  Compiler.query_binding_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      @doc false
      def build(
            schema,
            _filter_op,
            query,
            binding_selector = unquote(quoted_binding_head),
            {:association, key, params},
            opts
          ) do
        qual = params[:qualifier] || :inner
        as = params[:as]
        on = normalize_on(schema, binding_selector, params[:on], opts)

        Query.join(
          query,
          qual,
          [unquote_splicing(quoted_binding_body)],
          a in assoc(unquote(target_binding_var), ^key),
          as: ^as,
          on: ^on
        )
      end
  end

  defp normalize_on(schema, binding_selector, on_param, opts) do
    case on_param do
      true ->
        true

      nil ->
        true

      list when is_list(list) ->
        if Keyword.keyword?(list) do
          Dynamics.convert_to_dynamic(schema, binding_selector, list, opts)
        else
          EctoShorts.Logger.error(
            @logger_prefix,
            "Expected :on to be a keyword list, got: #{inspect(list)}"
          )

          true
        end

      on_params when is_map(on_params) ->
        Dynamics.convert_to_dynamic(schema, binding_selector, on_params, opts)

      term ->
        EctoShorts.Logger.error(
          @logger_prefix,
          "Expected :on to be a keyword list, map, or true, got: #{inspect(term)}"
        )

        true
    end
  end
end
