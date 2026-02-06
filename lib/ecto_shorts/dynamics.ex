defmodule EctoShorts.Dynamics do
  @moduledoc false

  alias EctoShorts.CommonSchema
  alias EctoShorts.Config
  alias EctoShorts.Dynamics.Postgres

  import Ecto.Query, only: [dynamic: 2]

  @logger_prefix "EctoShorts.Dynamics"

  @equal :==
  @boolean_operators [:and, :or]

  def convert_to_dynamic(source, binding_selector, args, opts \\ []) do
    expression_adapter = expression_adapter!(opts)
    source = CommonSchema.normalize_source(source)

    if is_map(args) or is_list(args) do
      Enum.reduce(args, nil, fn entry, dyn_acc ->
        reduce_dynamic_params(expression_adapter, source, dyn_acc, binding_selector, entry)
      end)
    else
      reduce_dynamic_params(expression_adapter, source, nil, binding_selector, args)
    end
  end

  defp reduce_dynamic_params(expression_adapter, source, dyn_l, binding_selector, args)
       when is_map(args) or is_list(args) do
    Enum.reduce(args, dyn_l, fn entry, dyn_acc ->
      reduce_dynamic_params(expression_adapter, source, dyn_acc, binding_selector, entry)
    end)
  end

  defp reduce_dynamic_params(expression_adapter, source, dyn_l, binding_selector, {key, value})
       when key in @boolean_operators do
    if is_map(value) and not is_struct(value) do
      reduce_dynamic_params(
        expression_adapter,
        source,
        dyn_l,
        binding_selector,
        {key, Map.to_list(value)}
      )
    else
      reduce_merge_dynamic_predicates(
        expression_adapter,
        source,
        dyn_l,
        binding_selector,
        key,
        value
      )
    end
  end

  defp reduce_dynamic_params(expression_adapter, source, dyn_l, binding_selector, {key, value}) do
    cond do
      key in expression_adapter.operators() ->
        value
        |> normalize_expr_params()
        |> Enum.reduce(dyn_l, fn item, dyn_acc ->
          dyn_r = expression_adapter.build_dynamic_expression(source, binding_selector, key, item)
          merge_dynamic(dyn_acc, :and, dyn_r)
        end)

      source_has_schema?(source) ->
        build_schema_dynamic(expression_adapter, source, dyn_l, binding_selector, {key, value})

      true ->
        value
        |> normalize_expr_params()
        |> Enum.reduce(dyn_l, fn item, dyn_acc ->
          dyn_r = expression_adapter.build_dynamic_expression(source, binding_selector, key, item)
          merge_dynamic(dyn_acc, :and, dyn_r)
        end)
    end
  end

  # Reduces `entries` into a single boolean `dynamic/2` expression.
  #
  # Each entry is first converted into an independent predicate
  # (starting from `nil`), and then combined with the accumulator
  # using `bool_op` (`:and` or `:or`).
  #
  # Returns the combined `dynamic()` expression (or the first built
  # predicate when the accumulator is `nil`).
  #
  # ## Examples
  #
  #     # bool_op:  :or
  #     # input:    [dyn1, dyn2]
  #     # output:   dyn1 or dyn2
  #
  #     # bool_op:  :and
  #     # input:    [dyn1, dyn2]
  #     # output:   dyn1 and dyn2
  #
  # ## Nesting
  #
  # This reducer can build “outer” groups (across a list of entries),
  # and it can also participate in “inner” groups when entries contain
  # nested boolean structures (since entries are built via `reduce_dynamic_params/4`,
  # which may call back into this reducer for nested `:and` / `:or`).
  defp reduce_merge_dynamic_predicates(
         expression_adapter,
         source,
         dyn_l,
         binding_selector,
         bool_op,
         entries
       ) do
    Enum.reduce(entries, dyn_l, fn entry, dyn_acc ->
      dyn_r =
        cond do
          is_map(entry) and not is_struct(entry) ->
            reduce_dynamic_params(
              expression_adapter,
              source,
              nil,
              binding_selector,
              Map.to_list(entry)
            )

          Keyword.keyword?(entry) ->
            reduce_dynamic_params(expression_adapter, source, nil, binding_selector, entry)

          true ->
            reduce_dynamic_params(expression_adapter, source, nil, binding_selector, entry)
        end

      merge_dynamic(dyn_acc, bool_op, dyn_r)
    end)
  end

  defp build_schema_dynamic(expression_adapter, source, dyn_l, binding_selector, {key, value}) do
    schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

    if is_list(schema_fields) and key not in schema_fields do
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
      )

      dyn_l
    else
      build_schema_dynamic_value(expression_adapter, source, dyn_l, binding_selector, key, value)
    end
  end

  defp build_schema_dynamic_value(
         expression_adapter,
         source,
         dyn_l,
         binding_selector,
         key,
         value
       )
       when is_map(value) and not is_struct(value) do
    build_schema_dynamic(
      expression_adapter,
      source,
      dyn_l,
      binding_selector,
      {key, Map.to_list(value)}
    )
  end

  defp build_schema_dynamic_value(expression_adapter, source, dyn_l, binding_selector, key, value)
       when is_list(value) do
    if Keyword.keyword?(value) do
      Enum.reduce(value, dyn_l, fn entry, dyn_acc ->
        build_schema_dynamic(expression_adapter, source, dyn_acc, binding_selector, {key, entry})
      end)
    else
      build_dynamic_field_expr(expression_adapter, source, dyn_l, binding_selector, key, value)
    end
  end

  defp build_schema_dynamic_value(expression_adapter, source, dyn_l, binding_selector, key, value) do
    build_dynamic_field_expr(expression_adapter, source, dyn_l, binding_selector, key, value)
  end

  # NOTE: `{bool_op, values}` can mean two different things:
  #
  # 1) Same-field comparisons (apply to `key`):
  #
  #    %{published: %{or: %{==: true, ==: false}}}
  #    -> build predicates like: published == true OR published == false
  #
  # 2) Composite predicates (each entry is its own field map/keyword list):
  #
  #    %{id: %{or: [%{published: true, views: 20}, %{published: false, views: 10}]}}
  #    -> each list item is a full "AND group" across multiple fields
  #
  # If we always wrap entries as `{key, entry}` (ex: `{id, %{published: true}}`),
  # the inner field tuples (`{:published, true}`) can accidentally get treated as
  # values for `:id`, which will likely crash due to there being no matching clause.
  #
  # (In practice, this will crash in the adapter expression builder when no
  # matching `dynamic_field_expr/3` clause exists for the unexpected shape.)
  #
  # So: for composite entries we pass `values` through unchanged and let the
  # existing reducer recurse into each field; for same-field comparisons we
  # wrap them as `{key, comparison}`.
  defp build_dynamic_field_expr(
         expression_adapter,
         source,
         dyn_l,
         binding_selector,
         key,
         {bool_op, values}
       )
       when bool_op in @boolean_operators and is_list(values) do
    entries =
      if composite_predicate_entries?(values) do
        values
      else
        Enum.map(values, &{key, &1})
      end

    dyn_r =
      reduce_merge_dynamic_predicates(
        expression_adapter,
        source,
        nil,
        binding_selector,
        bool_op,
        entries
      )

    merge_dynamic(dyn_l, :and, dyn_r)
  end

  defp build_dynamic_field_expr(
         expression_adapter,
         source,
         dyn_l,
         binding_selector,
         key,
         {op, value}
       ) do
    value
    |> normalize_expr_params()
    |> Enum.reduce(dyn_l, fn item, dyn_acc ->
      dyn_r =
        expression_adapter.build_dynamic_expression(source, binding_selector, key, {op, item})

      merge_dynamic(dyn_acc, :and, dyn_r)
    end)
  end

  defp build_dynamic_field_expr(expression_adapter, source, dyn_l, binding_selector, key, value) do
    build_dynamic_field_expr(
      expression_adapter,
      source,
      dyn_l,
      binding_selector,
      key,
      {@equal, value}
    )
  end

  defp merge_dynamic(nil, _, dyn_right) do
    dyn_right
  end

  defp merge_dynamic(dyn_l, :and, dyn_r) do
    dynamic([], ^dyn_l and ^dyn_r)
  end

  defp merge_dynamic(dyn_l, :or, dyn_r) do
    dynamic([], ^dyn_l or ^dyn_r)
  end

  defp composite_predicate_entries?(values) when is_list(values) do
    case values do
      [entry | _] -> (is_map(entry) and not is_struct(entry)) or Keyword.keyword?(entry)
      [] -> false
    end
  end

  defp source_has_schema?({_, schema}) when is_atom(schema) and not is_nil(schema), do: true
  defp source_has_schema?(_), do: false

  defp normalize_expr_params(term) do
    term
    |> do_normalize_expr_params([])
    |> Enum.reverse()
  end

  defp do_normalize_expr_params(term, acc) when is_map(term) and not is_struct(term) do
    term
    |> Map.to_list()
    |> do_normalize_expr_params(acc)
  end

  defp do_normalize_expr_params([], acc), do: acc

  defp do_normalize_expr_params(list, acc) when is_list(list) do
    if flatten?(list) do
      Enum.reduce(list, acc, fn entry, acc_inner ->
        do_normalize_expr_params(entry, acc_inner)
      end)
    else
      [list | acc]
    end
  end

  defp do_normalize_expr_params({k, v}, acc) when is_map(v) and not is_struct(v) do
    do_normalize_expr_params({k, Map.to_list(v)}, acc)
  end

  defp do_normalize_expr_params({k, v}, acc) when is_list(v) do
    if flatten?(v) do
      v
      |> normalize_expr_params()
      |> Enum.map(&{k, &1})
      |> do_normalize_expr_params(acc)
    else
      [{k, v} | acc]
    end
  end

  defp do_normalize_expr_params(v, acc) do
    [v | acc]
  end

  defp flatten?(list) do
    Keyword.keyword?(list) or Enum.any?(list, &is_map/1)
  end

  defp expression_adapter!(opts) do
    case Keyword.get(opts, :expression_adapter) do
      nil ->
        repo = Keyword.get(opts, :repo) || Config.repo()

        if is_nil(repo) do
          raise ArgumentError, """
          Missing :repo option for dynamic expression adapter detection.

          Expected one of the following:

            * Pass `repo: MyApp.Repo`
            * Pass `expression_adapter: MyApp.DynamicExpressionAdapter`
            * Configure a default repo:

                config :ecto_shorts, :repo, MyApp.Repo
          """
        end

        unless is_atom(repo) and function_exported?(repo, :__adapter__, 0) do
          raise ArgumentError,
                "Expected :repo to be an Ecto.Repo module that exports __adapter__/0, got: #{inspect(repo)}"
        end

        adapter = repo.__adapter__()

        case adapter do
          Ecto.Adapters.Postgres ->
            Postgres

          other ->
            raise ArgumentError, """
            Unsupported Ecto repo adapter: #{inspect(other)} (repo: #{inspect(repo)}).

            EctoShorts currently supports dynamic expressions for Postgres only.

            Use an Ecto SQL adapter with Postgres, or provide a custom dynamic expression adapter module via:

                expression_adapter: MyApp.DynamicExpressionAdapter
            """
        end

      module ->
        validate_expression_adapter!(module)
        module
    end
  end

  defp validate_expression_adapter!(module) do
    unless is_atom(module) do
      raise ArgumentError,
            "Expected :expression_adapter to be a module, got: #{inspect(module)}"
    end

    case Code.ensure_compiled(module) do
      {:module, _} ->
        :ok

      {:error, reason} ->
        raise ArgumentError,
              "Expected :expression_adapter to be a compiled module, got: #{inspect(module)} (#{inspect(reason)})"
    end

    unless function_exported?(module, :operators, 0) do
      raise ArgumentError,
            "Expected :expression_adapter #{inspect(module)} to export operators/0"
    end

    unless function_exported?(module, :build_dynamic_expression, 4) do
      raise ArgumentError,
            "Expected :expression_adapter #{inspect(module)} to export build_dynamic_expression/4"
    end
  end
end
