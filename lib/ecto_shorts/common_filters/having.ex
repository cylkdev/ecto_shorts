defmodule EctoShorts.CommonFilters.Having do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds `:having` and `:or_having` expressions from data-driven params.

  Converts filter params into dynamic expressions and applies them as
  `Ecto.Query.having/3` or `Ecto.Query.or_having/3` clauses on an existing
  `Ecto.Query`. Used internally by `EctoShorts.CommonFilters` when it
  encounters the `:having` or `:or_having` filter keys.

  ## Key concepts

  ### `:having` vs `:or_having`

  * `:having` appends an `AND HAVING` clause to the query.
  * `:or_having` appends an `OR HAVING` clause.

  ### Accepted param shapes

  Params can be any of the following:

  * A keyword list of `{aggregate_field, value}` pairs:

        [count_comments: 5]

  * A plain map:

        %{count_comments: 5}

  * A nested boolean operator `{:and, [...]}` or `{:or, [...]}`:

        {or: [%{count_comments: 1}, %{count_comments: 2}]}

  * A raw dynamic expression (passed through unchanged):

        import Ecto.Query
        dynamic([p], count(p.id) > 10)

  Unknown param shapes are logged as warnings and the query is returned
  unchanged.

  ## Examples

      import Ecto.Query

      query =
        from p in EctoShorts.Schema.Post,
          group_by: p.author_id,
          select: {p.author_id, count(p.id)}

      # Add a HAVING clause using data-driven params
      EctoShorts.CommonFilters.convert_params_to_filter(
        EctoShorts.Schema.Post,
        %{having: [count_comments: [gt: 3]]},
        repo: EctoShorts.Repo
      )

  See also `EctoShorts.CommonFilters`, `EctoShorts.Dynamics`, and
  `Ecto.Query.having/3`.
  """

  alias Ecto.Query
  alias EctoShorts.Compiler
  alias EctoShorts.Dynamics

  require Ecto.Query
  require EctoShorts.Compiler

  @doc false
  def build(schema_source, filter_op, query, binding_selector, params, opts)
      when filter_op in [:having, :or_having] do
    dyn = Dynamics.convert_to_dynamic(schema_source, binding_selector, params, opts)
    apply_having_clause(filter_op, query, binding_selector, dyn)
  end

  defp apply_having_clause(:having, query, binding_selector, dyn) do
    apply_having_expr(query, binding_selector, dyn)
  end

  defp apply_having_clause(:or_having, query, binding_selector, dyn) do
    apply_or_having_expr(query, binding_selector, dyn)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, _target_binding_var, _binding_patterns ->
      defp apply_having_expr(query, unquote(quoted_binding_head), nil) do
        query
      end

      defp apply_having_expr(query, unquote(quoted_binding_head), dyn) do
        Query.having(query, [unquote_splicing(quoted_binding_body)], ^dyn)
      end

      defp apply_or_having_expr(query, unquote(quoted_binding_head), nil) do
        query
      end

      defp apply_or_having_expr(query, unquote(quoted_binding_head), dyn) do
        Query.or_having(query, [unquote_splicing(quoted_binding_body)], ^dyn)
      end
  end
end
