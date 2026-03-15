defmodule EctoShorts.Dynamics.Adapters.Postgres.CommonExpr do
  alias Ecto.Query
  alias EctoShorts.QueryBindings

  require Ecto.Query

  @operators [
    :ids,
    :before,
    :after,
    :until,
    :since,
    :exists,
    :start_date,
    :end_date,
    :since_date,
    :until_date
  ]

  def operators, do: @operators

  {target_binding_var, binding_patterns} = QueryBindings.query_binding_contracts(__MODULE__)

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    def dynamic_expr(unquote(quoted_binding_head), operator, negated, term, _opts) do
      expr =
        case operator do
          :ids ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), :id) in ^term
            )

          :after ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), :id) > ^term
            )

          :before ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), :id) < ^term
            )

          :since ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), :id) >= ^term
            )

          :until ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), :id) <= ^term
            )

          op when op in [:start_date, :since_date] ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), :inserted_at) >= ^term
            )

          op when op in [:end_date, :until_date] ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              field(unquote(target_binding_var), :inserted_at) <= ^term
            )

          :exists ->
            Query.dynamic(
              [unquote_splicing(quoted_binding_body)],
              exists(term)
            )

          _ ->
            nil
        end

      if negated === :not and not is_nil(expr) do
        Query.dynamic([unquote_splicing(quoted_binding_body)], not (^expr))
      else
        expr
      end
    end
  end

  def dynamic_expr(_selected_binding, _operator, _negated, _term, _opts), do: nil
end
