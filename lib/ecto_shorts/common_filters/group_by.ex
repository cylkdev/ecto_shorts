defmodule EctoShorts.CommonFilters.GroupBy do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds `:group_by` expressions from data-driven params.

  Accepts single field atoms, lists of fields, and dynamic expressions.
  Supports binding-scoped params via the `:bind` key.
  """

  alias Ecto.Query
  alias EctoShorts.CommonFilters.BindParams
  alias EctoShorts.Compiler

  require Ecto.Query
  require EctoShorts.Compiler

  @binding_selector_key :bind

  @doc false
  def build(_schema_source, :group_by, query, binding_selector, params, _opts) do
    reduce_group_by(query, binding_selector, params)
  end

  defp reduce_group_by(query, binding_selector, {key, params})
       when is_map(params) and not is_struct(params) do
    reduce_group_by(query, binding_selector, {key, Map.to_list(params)})
  end

  defp reduce_group_by(query, binding_selector, {@binding_selector_key, bind_params}) do
    reduce_group_by_bind(query, binding_selector, bind_params)
  end

  defp reduce_group_by(query, binding_selector, {key, value}) do
    apply_group_by_expr(query, binding_selector, {key, value})
  end

  defp reduce_group_by(query, binding_selector, params)
       when is_map(params) and not is_struct(params) do
    reduce_group_by(query, binding_selector, Map.to_list(params))
  end

  defp reduce_group_by(query, binding_selector, entries) when is_list(entries) do
    if Keyword.keyword?(entries) do
      case Enum.split_with(entries, fn {k, _} -> k === @binding_selector_key end) do
        {[], group_entries} ->
          Enum.reduce(group_entries, query, fn {key, value}, q ->
            reduce_group_by(q, binding_selector, {key, value})
          end)

        {bind_entries, []} ->
          Enum.reduce(bind_entries, query, fn entry, query_acc ->
            reduce_group_by(query_acc, binding_selector, entry)
          end)

        {bind_entries, group_entries} ->
          query_with_group =
            Enum.reduce(group_entries, query, fn {key, value}, q ->
              reduce_group_by(q, binding_selector, {key, value})
            end)

          Enum.reduce(bind_entries, query_with_group, fn entry, query_acc ->
            reduce_group_by(query_acc, binding_selector, entry)
          end)
      end
    else
      apply_group_by_expr(query, binding_selector, entries)
    end
  end

  defp reduce_group_by(query, binding_selector, expr) do
    apply_group_by_expr(query, binding_selector, expr)
  end

  defp reduce_group_by_bind(query, _binding_selector, bind_params) do
    BindParams.reduce_submodule_bind_params(query, bind_params, fn q, {mode, target}, value ->
      reduce_group_by(q, {mode, target}, value)
    end)
  end

  Compiler.define_clauses do
    quoted_binding_head, quoted_binding_body, target_binding_var, _binding_patterns ->
      defp apply_group_by_expr(query, unquote(quoted_binding_head), field_name)
           when is_atom(field_name) do
        Query.group_by(
          query,
          [unquote_splicing(quoted_binding_body)],
          field(unquote(target_binding_var), ^field_name)
        )
      end

      defp apply_group_by_expr(query, unquote(quoted_binding_head), entries)
           when is_list(entries) do
        group_by_exprs =
          Enum.map(entries, fn
            field_name when is_atom(field_name) ->
              Query.dynamic(
                [unquote_splicing(quoted_binding_body)],
                field(unquote(target_binding_var), ^field_name)
              )

            %Ecto.Query.DynamicExpr{} = dynamic_expr ->
              dynamic_expr

            other ->
              other
          end)

        Query.group_by(query, ^group_by_exprs)
      end
  end

  defp apply_group_by_expr(query, _binding_selector, expr) do
    Query.group_by(query, ^expr)
  end
end
