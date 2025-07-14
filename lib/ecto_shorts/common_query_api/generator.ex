defmodule EctoShorts.CommonQueryAPI.Generator do
  @moduledoc false
  alias EctoShorts.CommonQueryAPI.Generator

  @doc """
  Generates a set of query helper functions (wrappers around Ecto.Query macros)
  based on the provided options and injects them into the current module.

  This macro supports all major query macros (e.g., `dynamic`, `where`, `select`, `join`)
  and allows you to generate both positional and named binding variants.

  ## Options

    * `:max_positional_bindings` - Max number of positional bindings to support (required)
    * `:prefix` - Atom prefix for generated binding vars (default: `:b`)
    * `:operators` - List of operator atoms to generate clauses for (e.g., `:==`, `:in`, `:ilike`)
    * `:builder` - Module implementing `build_expr/4` (and optionally `build_head_ast/4`)
    * `:reverse_ops` - Operators for which the key/value should be reversed

  ## Examples

  ### Example 1: Define dynamic/3 functions for `==`, `ilike`, `in`, and `fragment_like` (named binding support)

      EctoShorts.CommonQueryAPI.Generator.define_query_api(
        :dynamic,
        max_positional_bindings: 3,
        builder: EctoShorts.CommonQueryAPI.Generator.FieldExprBuilder,
        operators: [:==, :ilike, :in, :fragment_like],
        reverse_ops: []
      )

  ### Example 2: Define join/6 wrappers for association and subquery joins

      EctoShorts.CommonQueryAPI.Generator.define_query_api(
        :join,
        max_positional_bindings: 2,
        builder: EctoShorts.CommonQueryAPI.Generator.JoinExprBuilder,
        operators: [:association, :subquery]
      )
  """
  defmacro define_query_api(macro_name, opts \\ []) do
    query_api_ast = Generator.build_query_api_ast(macro_name, __CALLER__, opts)

    quote do
      import Ecto.Query
      unquote(query_api_ast)
    end
  end

  @doc """
  Returns quoted AST for all generated clauses for a given macro name.

  ## Used Internally By
    * `define_query_api/2`

  ## Options

    See `define_query_api/2` for full list of supported options.

  ## Examples

      # Build clauses for dynamic/3 with both named and positional variants
      EctoShorts.CommonQueryAPI.Generator.build_query_api_ast(
        :dynamic,
        __ENV__,
        max_positional_bindings: 2,
        prefix: :b,
        builder: EctoShorts.CommonQueryAPI.Generator.FieldExprBuilder,
        operators: [:==, :in]
      )

      # Build only positional bindings for join/6
      EctoShorts.CommonQueryAPI.Generator.build_query_api_ast(
        :join,
        __ENV__,
        max_positional_bindings: 2,
        builder: EctoShorts.CommonQueryAPI.Generator.JoinExprBuilder,
        operators: [:association, :subquery]
      )
  """
  def build_query_api_ast(macro_name, env \\ __ENV__, opts \\ []) do
    max = Keyword.fetch!(opts, :max_positional_bindings)
    prefix = Keyword.get(opts, :prefix, :b)
    ops = Keyword.get(opts, :operators, [:ilike])
    builder = Keyword.fetch!(opts, :builder)

    positional_fns =
      for op <- ops, count <- 1..max do
        Generator.build_query_api_function_ast(
          builder,
          macro_name,
          {:positional, count},
          op,
          prefix,
          env,
          opts
        )
      end

    named_fns =
      for op <- ops do
        Generator.build_query_api_function_ast(
          builder,
          macro_name,
          :named,
          op,
          prefix,
          env,
          opts
        )
      end

    fn_asts = positional_fns ++ named_fns

    quote do
      (unquote_splicing(fn_asts))
    end
  end

  @doc """
  Builds a single function clause AST for a specific macro/operation combination.

  This function is the core of how the Generator creates query macro wrappers.
  It's called by `build_query_api_ast/3` to produce a single function clause.

  ## Parameters

    * `builder_module` - Module that implements `build_expr/4`
    * `macro_name` - The query macro to wrap (e.g., `:dynamic`, `:join`, `:select`)
    * `binding_input` - Either `{:positional, n}` or `:named` to indicate binding style
    * `op` - The operation atom (e.g., `:==`, `:in`, `:association`, `:fragment_like`)
    * `prefix` - Prefix used when generating binding vars (default: `:b`)
    * `opts` - The keyword list of options passed from the generator macro

  ## Examples

      # Build AST for dynamic/3 using 2 positional bindings
      EctoShorts.CommonQueryAPI.Generator.build_query_api_function_ast(
        EctoShorts.CommonQueryAPI.Generator.FieldExprBuilder,
        :dynamic,
        {:positional, 2},
        :==,
        :b,
        __ENV__,
        []
      )

      # Build AST for join/6 using named binding
      EctoShorts.CommonQueryAPI.Generator.build_query_api_function_ast(
        EctoShorts.CommonQueryAPI.Generator.JoinExprBuilder,
        :join,
        :named,
        :association,
        :b,
        __ENV__,
        [hints: "use_index"]
      )
  """
  def build_query_api_function_ast(
        builder_module,
        macro_name,
        binding_input,
        op,
        prefix \\ :b,
        env \\ __ENV__,
        opts \\ []
      ) do
    # Ensure builder_module is a real module atom (handles alias AST or literal atoms)
    builder_module =
      builder_module
      |> Macro.expand(env)
      |> case do
        {:__aliases__, _, aliases} -> Module.concat(aliases)
        atom when is_atom(atom) -> atom
      end

    query_var = Macro.var(:query, env.context)
    qual_var = Macro.var(:qual, env.context)
    opts_var = Macro.var(:opts, env.context)

    reverse_ops = Keyword.get(opts, :reverse_ops, [])

    {key_var, value_var} =
      if op in reverse_ops do
        {Macro.var(:value, env.context), Macro.var(:key, env.context)}
      else
        {Macro.var(:key, env.context), Macro.var(:value, env.context)}
      end

    binding_info =
      case binding_input do
        {:positional, count} ->
          bindings =
            Enum.map(0..(count - 1), fn i ->
              Macro.var(:"#{prefix}#{i}", env.context)
            end)

          {:positional, count, bindings}

        :named ->
          binding_alias_var = Macro.var(:binding_alias, env.context)
          binding_var = Macro.var(:"#{prefix}0", env.context)

          {:named, binding_alias_var, binding_var}
      end

    vars_info = {query_var, qual_var, key_var, value_var, opts_var}

    inner_expr = builder_module.build_expr(op, binding_info, vars_info)

    fn_head =
      build_function_head(
        builder_module,
        macro_name,
        {op, binding_info, vars_info}
      )

    fn_body =
      build_function_body(
        macro_name,
        {op, inner_expr, binding_info, vars_info},
        opts
      )

    quote do
      def unquote(fn_head) do
        unquote(fn_body)
      end
    end
  end

  defp build_function_head(
         builder_module,
         macro_name,
         {
          op,
          binding_info,
          {query_var, qual_var, key_var, value_var, opts_var} = vars_info
        }
       ) do
    if function_exported?(builder_module, :build_head_ast, 4) do
      builder_module.build_head_ast(macro_name, op, binding_info, vars_info)
    else
      fn_head_binding_var =
        case binding_info do
          {:positional, count, bindings} ->
            Enum.at(bindings, count - 1)

          {:named, binding_alias_var, _binding_var} ->
            binding_alias_var

        end

      args =
        case macro_name do
          :join -> [query_var, fn_head_binding_var, qual_var, key_var, {op, value_var}, opts_var]
          :dynamic -> [fn_head_binding_var, key_var, {op, value_var}]
          _ -> [query_var, fn_head_binding_var, key_var, {op, value_var}]
        end

      quote do
        unquote(macro_name)(unquote_splicing(args))
      end
    end
  end

  defp build_function_body(
    :dynamic,
    {
      op,
      inner_expr,
      binding_info,
      {query_var, qual_var, key_var, value_var, opts_var} = _vars_info
    },
    _opts
  ) do
    transform_ast = build_transform_ast(:dynamic, key_var, op, value_var)

    case binding_info do
      {:positional, _count, bindings} ->
        quote do
          unquote_splicing(transform_ast)

          Ecto.Query.dynamic([unquote_splicing(bindings)], unquote(inner_expr))
        end

      {:named, binding_alias_var, binding_var} ->
        quote do
          unquote_splicing(transform_ast)

          if unquote(binding_alias_var) do
            Ecto.Query.dynamic(
              [{^unquote(binding_alias_var), unquote(binding_var)}],
              unquote(inner_expr)
            )
          else
            Ecto.Query.dynamic([unquote(binding_var)], unquote(inner_expr))
          end
        end
    end
  end

  defp build_function_body(
    :join,
    {
      op,
      inner_expr,
      binding_info,
      {query_var, qual_var, key_var, value_var, opts_var} = _vars_info
    },
    opts
  ) do
    join_opts =
      if Keyword.has_key?(opts, :hints) do
        hints = opts[:hints]

        quote do
          [on: ^on_expr, as: ^as, prefix: ^prefix, hints: unquote(hints)]
        end
      else
        quote do
          [on: ^on_expr, as: ^as, prefix: ^prefix]
        end
      end

    transform_ast = build_transform_ast(:join, key_var, op, value_var)

    case binding_info do
      {:positional, _count, bindings} ->
        quote do
          on_expr = unquote(opts_var)[:on] || true
          as = unquote(opts_var)[:as]
          prefix = unquote(opts_var)[:prefix]

          unquote_splicing(transform_ast)

          Ecto.Query.join(
            unquote(query_var),
            unquote(qual_var),
            [unquote_splicing(bindings)],
            unquote(inner_expr),
            unquote(join_opts)
          )
        end

      {:named, binding_alias_var, binding_var} ->
        quote do
          on_expr = unquote(opts_var)[:on] || true
          as = unquote(opts_var)[:as]
          prefix = unquote(opts_var)[:prefix]

          unquote_splicing(transform_ast)

          if binding_alias do
            Ecto.Query.join(
              unquote(query_var),
              unquote(qual_var),
              [{^unquote(binding_alias_var), unquote(binding_var)}],
              unquote(inner_expr),
              unquote(join_opts)
            )
          else
            Ecto.Query.join(
              unquote(query_var),
              unquote(qual_var),
              [unquote(binding_var)],
              unquote(inner_expr),
              unquote(join_opts)
            )
          end
        end
    end
  end

  defp build_function_body(
    macro_name,
    {
      op,
      inner_expr,
      binding_info,
      {query_var, qual_var, key_var, value_var, opts_var} = vars_info
    },
    _opts
  ) do
    transform_ast = build_transform_ast(macro_name, key_var, op, value_var)

    case binding_info do
      {:positional, _count, bindings} ->
        quote do
          unquote_splicing(transform_ast)

          Ecto.Query.unquote(macro_name)(
            query,
            [unquote_splicing(bindings)],
            unquote(inner_expr)
          )
        end

      {:named, binding_alias_var, binding_var} ->
        quote do
          unquote_splicing(transform_ast)

          if binding_alias do
            Ecto.Query.unquote(macro_name)(
              query,
              [{^binding_alias, unquote(binding_var)}],
              unquote(inner_expr)
            )
          else
            Ecto.Query.unquote(macro_name)(
              query,
              [unquote(binding_var)],
              unquote(inner_expr)
            )
          end
        end
    end
  end

  defp build_transform_ast(macro_name, key_var, op, value_var) do
    [
      quote do
        unquote(value_var) =
          if function_exported?(__MODULE__, :transform, 4) do
            __MODULE__.transform(
              unquote(value_var),
              {unquote(macro_name), unquote(key_var), unquote(op)}
            )
          else
            unquote(value_var)
          end
      end
    ]
  end
end
