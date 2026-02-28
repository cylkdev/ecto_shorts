defmodule EctoShorts.CommonFilters.Having do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds `:having` and `:or_having` expressions from data-driven params.

  Converts filter params into dynamic expressions and applies them as
  `Ecto.Query.having/3` or `Ecto.Query.or_having/3` clauses. Supports
  nested boolean operators, keyword lists, maps, and raw
  `Ecto.Query.DynamicExpr` values.
  """

  alias Ecto.Query
  alias EctoShorts.Compiler
  alias EctoShorts.Dynamics
  alias EctoShorts.Logger

  require Ecto.Query
  require EctoShorts.Compiler

  @logger_prefix "EctoShorts.CommonFilters.Having"

  @boolean_operators [:and, :or]

  @doc false
  def build(schema_source, filter_op, query, binding_selector, params, opts)
      when filter_op in [:having, :or_having] do
    reduce_having(filter_op, schema_source, query, binding_selector, params, opts)
  end

  defp reduce_having(
         filter_op,
         _schema_source,
         query,
         binding_selector,
         %Ecto.Query.DynamicExpr{} = dyn,
         _opts
       ) do
    reduce_having_expr(filter_op, query, binding_selector, dyn)
  end

  defp reduce_having(filter_op, schema_source, query, binding_selector, params, opts)
       when is_map(params) and not is_struct(params) do
    reduce_having(filter_op, schema_source, query, binding_selector, Map.to_list(params), opts)
  end

  defp reduce_having(filter_op, schema_source, query, binding_selector, params, opts)
       when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn entry, query_acc ->
        reduce_having(filter_op, schema_source, query_acc, binding_selector, entry, opts)
      end)
    else
      apply_dynamic_having(filter_op, schema_source, query, binding_selector, params, opts)
    end
  end

  defp reduce_having(
         filter_op,
         schema_source,
         query,
         binding_selector,
         {boolean_operator, params},
         opts
       )
       when boolean_operator in @boolean_operators do
    if is_map(params) and not is_struct(params) do
      reduce_having(
        filter_op,
        schema_source,
        query,
        binding_selector,
        {boolean_operator, Map.to_list(params)},
        opts
      )
    else
      apply_dynamic_having(
        filter_op,
        schema_source,
        query,
        binding_selector,
        {boolean_operator, params},
        opts
      )
    end
  end

  defp reduce_having(filter_op, schema_source, query, binding_selector, {key, value}, opts)
       when is_map(value) and not is_struct(value) do
    reduce_having(
      filter_op,
      schema_source,
      query,
      binding_selector,
      {key, Map.to_list(value)},
      opts
    )
  end

  defp reduce_having(filter_op, schema_source, query, binding_selector, {key, value}, opts)
       when is_list(value) do
    if Keyword.keyword?(value) do
      Enum.reduce(value, query, fn {key2, value2}, query_acc ->
        reduce_having(
          filter_op,
          schema_source,
          query_acc,
          binding_selector,
          {key, {key2, value2}},
          opts
        )
      end)
    else
      apply_dynamic_having(filter_op, schema_source, query, binding_selector, {key, value}, opts)
    end
  end

  defp reduce_having(filter_op, schema_source, query, binding_selector, {key, value}, opts) do
    apply_dynamic_having(filter_op, schema_source, query, binding_selector, {key, value}, opts)
  end

  defp reduce_having(filter_op, _schema_source, query, _binding_selector, params, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected params for #{filter_op} to be a map or keyword list, got: #{inspect(params)}"
    )

    query
  end

  defp apply_dynamic_having(filter_op, schema_source, query, binding_selector, params, opts) do
    dyn = Dynamics.convert_to_dynamic(schema_source, binding_selector, params, opts)
    reduce_having_expr(filter_op, query, binding_selector, dyn)
  end

  defp reduce_having_expr(:having, query, binding_selector, dyn) do
    apply_having_expr(query, binding_selector, dyn)
  end

  defp reduce_having_expr(:or_having, query, binding_selector, dyn) do
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
