defmodule EctoShorts.Dynamics do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Converts filter parameter maps into composable dynamic expressions.

  Used internally by `EctoShorts.CommonFilters.Filter` and other query
  builders to translate `{field, value}` pairs and boolean operator trees
  into composable dynamic expressions. Delegates the actual expression
  construction to the configured dynamic adapter.

  ## Key concepts

  ### Params format

  Filter params are plain maps or keyword lists of `{field, value}` pairs:

      %{title: "hello", published: true}

  Boolean sub-groups use the `:and` and `:or` keys with a list of sub-params:

      %{or: [%{published: true}, %{published: false}]}

  Helper operators such as `:datetime_add` and `:date_add` can be embedded
  inside field values to produce relative date expressions at the database level.

  ### Adapter delegation

  `EctoShorts.Dynamics` owns all routing: boolean handling, schema validation,
  payload resolution (`:exists`, `:all`, `:any`), and predicate merging.
  It calls `build_dynamic/4` on the configured `EctoShorts.Dynamic` adapter
  once per resolved leaf `{key, value}` pair. The built-in adapter is
  `EctoShorts.Dynamics.Postgres`. Override it by configuring
  `:dynamic_adapter` in your application config.

  ### Schema-aware filtering

  When a source with a schema module is provided, the module's
  `:query_fields` reflection is used to warn on unknown filter keys
  rather than silently ignoring them.

  ## Getting started

      import Ecto.Query

      params = %{title: "hello", published: true}

      dynamic_expr =
        EctoShorts.Dynamics.convert_to_dynamic(EctoShorts.Schema.Post, nil, params)

      from p in EctoShorts.Schema.Post, where: ^dynamic_expr

  ## Configuration

  * `:dynamic_adapter` - A module that implements the `EctoShorts.Dynamic` behaviour.
    Configurable globally via `config :ecto_shorts, dynamic_adapter: MyApp.Adapter`
    or at runtime via the `:dynamic_adapter` option on `convert_to_dynamic/4`.
    Defaults to resolved from repo adapter.

  See also `EctoShorts.Dynamic`, `EctoShorts.CommonFilters`, and
  `EctoShorts.Config`.
  """

  alias EctoShorts.CommonFilters
  alias EctoShorts.CommonSchema
  alias EctoShorts.Config

  require Ecto.Query

  @adapters %{
    Ecto.Adapters.Postgres => EctoShorts.Dynamics.Postgres
  }

  @boolean_operators [:and, :or]
  @quantifier_operators [:all, :any]
  @logger_prefix "EctoShorts.Dynamics"

  @doc false
  def adapters, do: @adapters

  @doc """
  Converts filter params into a single composable dynamic expression.

  Accepts a source (schema module, `{table_name, schema}` tuple, or
  `Ecto.Query`), a `binding_selector` used to resolve the correct query
  binding inside `dynamic/2`, a `params` map or keyword list of
  `{field, value}` pairs, and an optional `opts` keyword list.

  Returns `nil` when `params` is empty. Otherwise returns a
  dynamic expression that can be spliced into a query with `^`.

  ## Options

  * `:dynamic_adapter` - A module that implements the `EctoShorts.Dynamic`
    behaviour to use for this call. Defaults to resolved from repo adapter.

  * `:repo` - The `Ecto.Repo` module used to resolve the dynamic expression adapter
    when the option `:dynamic_adapter` is not set. Defaults to `EctoShorts.Config.repo/0`.

  ## Examples

      iex> import Ecto.Query
      ...> dyn = EctoShorts.Dynamics.convert_to_dynamic(EctoShorts.Schema.Post, nil, %{published: true})
      ...> is_struct(dyn, Ecto.Query.DynamicExpr)
      true

      iex> EctoShorts.Dynamics.convert_to_dynamic(EctoShorts.Schema.Post, nil, %{})
      nil

  See also `EctoShorts.Dynamic`.
  """
  @spec convert_to_dynamic(
          source :: term(),
          binding_selector :: term(),
          value :: term(),
          opts :: keyword()
        ) :: Ecto.Query.dynamic_expr() | nil
  def convert_to_dynamic(source, binding_selector, term, opts \\ []) do
    adapter = adapter_for_repo!(opts)
    source = CommonSchema.normalize_source(source)
    append_predicates(adapter, source, nil, binding_selector, term, opts)
  end

  defp append_predicates(adapter, source, dyn_left, binding_selector, {:exists, value}, opts) do
    resolved = resolve_exists_payload(source, value, opts)

    merge_predicate(
      dyn_left,
      :and,
      build_dynamic_or_warn(adapter, source, binding_selector, :exists, resolved)
    )
  end

  defp append_predicates(adapter, source, dyn_left, binding_selector, {key, value}, opts) do
    cond do
      adapter.operator?(key) ->
        merge_predicate(dyn_left, :and, build_dynamic_or_warn(adapter, source, binding_selector, key, value))

      key in @boolean_operators ->
        expand_and_reduce(adapter, source, dyn_left, binding_selector, key, value, opts)

      true ->
        resolved_value = resolve_quantifier_payload(source, key, value, opts)

        if source_has_schema?(source) do
          schema_fields = CommonSchema.get_schema_reflection(source, :query_fields)

          if key not in schema_fields do
            EctoShorts.Logger.warning(
              @logger_prefix,
              "Expected a query field for schema #{inspect(source)}, got: #{inspect(key)}"
            )

            dyn_left
          else
            if Keyword.keyword?(resolved_value) do
              Enum.reduce(resolved_value, dyn_left, fn entry, dyn_acc ->
                append_predicates(adapter, source, dyn_acc, binding_selector, {key, entry}, opts)
              end)
            else
              merge_predicate(
                dyn_left,
                :and,
                build_dynamic_or_warn(adapter, source, binding_selector, key, resolved_value)
              )
            end
          end
        else
          if Keyword.keyword?(resolved_value) do
            Enum.reduce(resolved_value, dyn_left, fn entry, dyn_acc ->
              append_predicates(adapter, source, dyn_acc, binding_selector, {key, entry}, opts)
            end)
          else
            merge_predicate(
              dyn_left,
              :and,
              build_dynamic_or_warn(adapter, source, binding_selector, key, resolved_value)
            )
          end
        end
    end
  end

  defp append_predicates(
         _adapter,
         _source,
         dyn_left,
         _binding_selector,
         %Ecto.Query.DynamicExpr{} = dyn,
         _opts
       ) do
    merge_predicate(dyn_left, :and, dyn)
  end

  defp append_predicates(adapter, source, dyn_left, binding_selector, params, opts) do
    if (is_map(params) and not is_struct(params)) or Keyword.keyword?(params) do
      Enum.reduce(params, dyn_left, fn {k, v}, dyn_acc ->
        append_predicates(adapter, source, dyn_acc, binding_selector, {k, v}, opts)
      end)
    else
      EctoShorts.Logger.warning(
        @logger_prefix,
        "Expected params to be a map or keyword list, got: #{inspect(params)}"
      )

      dyn_left
    end
  end

  defp expand_and_reduce(adapter, source, dyn_left, binding_selector, boolean_op, entries, opts) do
    Enum.reduce(entries, dyn_left, fn
      {field, keyword_value}, dyn_acc when is_atom(field) and is_list(keyword_value) ->
        if Keyword.keyword?(keyword_value) do
          Enum.reduce(keyword_value, dyn_acc, fn {op, val}, inner_acc ->
            dyn_right = build_dynamic_or_warn(adapter, source, binding_selector, field, {op, val})
            merge_predicate(inner_acc, boolean_op, dyn_right)
          end)
        else
          dyn_right = append_predicates(adapter, source, nil, binding_selector, {field, keyword_value}, opts)
          merge_predicate(dyn_acc, boolean_op, dyn_right)
        end

      entry, dyn_acc ->
        dyn_right = append_predicates(adapter, source, nil, binding_selector, entry, opts)
        merge_predicate(dyn_acc, boolean_op, dyn_right)
    end)
  end

  defp build_dynamic_or_warn(adapter, source, binding_selector, key, expr) do
    case adapter.build_dynamic(source, binding_selector, key, expr) do
      nil ->
        EctoShorts.Logger.warning(
          @logger_prefix,
          "Adapter #{inspect(adapter)} returned nil for field #{inspect(key)} with expression: #{inspect(expr)}"
        )

        nil

      dyn_right ->
        dyn_right
    end
  end

  defp merge_predicate(dyn_left, _op, nil), do: dyn_left
  defp merge_predicate(nil, _op, dyn_right), do: dyn_right
  defp merge_predicate(dyn_left, :and, dyn_right), do: Ecto.Query.dynamic([], ^dyn_left and ^dyn_right)
  defp merge_predicate(dyn_left, :or, dyn_right), do: Ecto.Query.dynamic([], ^dyn_left or ^dyn_right)

  defp source_has_schema?({_, schema}) when is_atom(schema) and not is_nil(schema), do: true
  defp source_has_schema?(_), do: false

  defp resolve_exists_payload(source, {:not, inner}, opts) do
    {:not, resolve_exists_payload(source, inner, opts)}
  end

  defp resolve_exists_payload(source, [not: inner], opts) do
    {:not, resolve_exists_payload(source, inner, opts)}
  end

  defp resolve_exists_payload(source, value, opts) do
    cond do
      is_struct(value, Ecto.Query) ->
        value

      is_struct(value, Ecto.SubQuery) ->
        value

      is_map(value) and not is_struct(value) ->
        resolve_exists_payload(source, Map.to_list(value), opts)

      is_list(value) ->
        build_subquery(source, value, true, opts)

      true ->
        value
    end
  end

  defp resolve_quantifier_payload(source, field_key, {quantifier, inner}, opts)
       when quantifier in @quantifier_operators do
    {quantifier, resolve_subquery_payload(source, inner, field_key, opts)}
  end

  defp resolve_quantifier_payload(source, field_key, value, opts)
       when is_map(value) and not is_struct(value) do
    resolve_quantifier_payload(source, field_key, Map.to_list(value), opts)
  end

  defp resolve_quantifier_payload(source, field_key, value, opts) when is_list(value) do
    if Keyword.keyword?(value) do
      Enum.map(value, fn {k, v} ->
        {k, resolve_quantifier_payload(source, field_key, v, opts)}
      end)
    else
      value
    end
  end

  defp resolve_quantifier_payload(_source, _field_key, value, _opts), do: value

  defp resolve_subquery_payload(source, params, select_default, opts) do
    cond do
      is_struct(params, Ecto.Query) ->
        params

      is_struct(params, Ecto.SubQuery) ->
        params

      is_map(params) and not is_struct(params) ->
        resolve_subquery_payload(source, Map.to_list(params), select_default, opts)

      Keyword.keyword?(params) and Keyword.has_key?(params, :from) ->
        build_subquery(source, params, select_default, opts)

      true ->
        params
    end
  end

  defp build_subquery(source, params, select_default, opts) do
    {from_source, filter_params} = Keyword.pop(params, :from, source)
    CommonFilters.convert_params_to_filter(from_source, put_default_select(filter_params, select_default), opts)
  end

  defp put_default_select(params, default) when is_list(params) do
    if Keyword.has_key?(params, :select), do: params, else: Keyword.put(params, :select, default)
  end

  defp adapter_for_repo!(opts) do
    builder = opts[:dynamic_adapter] || Config.dynamic_adapter()

    if not is_nil(builder) do
      if Code.ensure_loaded?(builder) and function_exported?(builder, :build_dynamic, 4) do
        builder
      else
        raise ArgumentError,
              "The specified dynamic adapter #{inspect(builder)} does not implement build_dynamic/4"
      end
    else
      repo = Config.repo!(opts)

      unless is_atom(repo) and Code.ensure_loaded?(repo) and
               function_exported?(repo, :__adapter__, 0) do
        raise ArgumentError,
              "Expected :repo to be an Ecto.Repo module that exports __adapter__/0, got: #{inspect(repo)}"
      end

      with nil <- Map.get(@adapters, repo.__adapter__()) do
        raise ArgumentError, """
        Unsupported Ecto repo adapter: #{inspect(repo.__adapter__())} (repo: #{inspect(repo)}).

        EctoShorts currently supports dynamic expressions for Postgres only.

        Use an Ecto SQL adapter with Postgres, or provide a custom dynamic expression adapter module via:

            dynamic_adapter: MyApp.DynamicExpressionAdapter
        """
      end
    end
  end
end
