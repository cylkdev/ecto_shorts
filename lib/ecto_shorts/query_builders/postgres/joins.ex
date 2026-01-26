defmodule EctoShorts.QueryBuilder.Joins do
  @moduledoc false
  alias EctoShorts.QueryBuilder.Dynamics

  alias Ecto.Query

  @logger_prefix "EctoShorts.QueryBuilder.Joins"

  require Ecto.Query

  {target_binding_var, binding_patterns} =
    EctoShorts.QueryBuilder.BindingHelpers.query_var_and_binding_heads()

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    def build_query(schema, query, unquote(quoted_binding_head), {:association, key, params}) do
      qual = params[:qualifier] || :inner
      as = params[:as]

      on =
        case params[:on] do
          true ->
            true

          nil ->
            true

          list when is_list(list) ->
            if Keyword.keyword?(list) do
              Dynamics.build_dynamic(schema, unquote(quoted_binding_head), list)
            else
              EctoShorts.Logger.error(
                @logger_prefix,
                "Expected :on to be a keyword list, got: #{inspect(list)}"
              )

              true
            end

          on_params when is_map(on_params) ->
            Dynamics.build_dynamic(schema, unquote(quoted_binding_head), on_params)

          term ->
            EctoShorts.Logger.error(
              @logger_prefix,
              "Expected :on to be a keyword list, map, or true, got: #{inspect(term)}"
            )

            true
        end

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
end
