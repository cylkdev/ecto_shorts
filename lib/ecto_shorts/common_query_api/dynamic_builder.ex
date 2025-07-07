defmodule EctoShorts.CommonQueryAPI.DynamicBuilder do
  @moduledoc false

  alias EctoShorts.CommonQueryAPI.Builder

  @max_positional_bindings Application.compile_env(
                             EctoShorts.Config.app(),
                             :max_positional_bindings
                           ) || 10

  defmacro define_base_api(before \\ []) do
    blocks = build_base_ast(before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_base_fragment_api(fragment_name, statement, var_names \\ [], exprs \\ [], before \\ []) do
    blocks = build_base_fragment_ast(fragment_name, statement, var_names, exprs, before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_positional_binding_api(before \\ []) do
    blocks = build_positional_binding_ast(before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_positional_binding_and_fragment_api(params, guards \\ [], before \\ []) do
    blocks = build_positional_binding_and_fragment_ast(params, guards, before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_named_binding_api(before \\ []) do
    blocks = build_named_binding_ast(before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_named_binding_and_fragment_api(params, guards \\ []) do
    blocks = build_named_binding_and_fragment_ast(params, guards, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  # API

  @doc """
  EctoShorts.CommonQueryAPI.DynamicBuilder.build_base_fragment_ast(:ilike, "example", [var: :patterns, field: :key])
  """
  def build_base_fragment_ast(fragment_name, statement, var_names \\ [], exprs \\ [], before \\ [], env \\ __ENV__) do
    vars = Builder.create_vars(var_names, env)
    exprs = Builder.create_exprs(exprs, env)
    before = quote_transformer(before)

    first_binding_ast =
      quote do
        def dynamic(:first, {unquote(fragment_name), unquote_splicing(vars)}) do
          unquote_splicing(before)

          Ecto.Query.dynamic([b], fragment(unquote(statement), unquote_splicing(exprs)))
        end
      end

    last_binding_ast =
      quote do
        def dynamic(:last, {unquote(fragment_name), unquote_splicing(vars)}) do
          unquote_splicing(before)

          Ecto.Query.dynamic([_, ..., b], fragment(unquote(statement), unquote_splicing(exprs)))
        end
      end

    [first_binding_ast, last_binding_ast]
  end

  @doc """
  ...
  """
  def build_base_ast(before \\ [], _env \\ __ENV__) do
    before = quote_transformer(before)

    first_binding_ast =
      quote do
        def dynamic(:first, value) do
          unquote_splicing(before)

          Ecto.Query.dynamic([b], ^value)
        end
      end

    last_binding_ast =
      quote do
        def dynamic(:last, value) do
          unquote_splicing(before)

          Ecto.Query.dynamic([_, ..., b], ^value)
        end
      end

    [first_binding_ast, last_binding_ast]
  end

  @doc """
  ...
  """
  def build_positional_binding_and_fragment_ast(fragment_name, statement, var_names \\ [], exprs \\ [], before \\ [], env \\ __ENV__) do
    vars = Builder.create_vars(var_names, env)
    exprs = Builder.create_exprs(exprs, env)
    before = quote_transformer(before)

    Enum.map(1..@max_positional_bindings, fn count ->
      var_bindings = Enum.map(1..count, fn i -> Macro.var(:"b#{i - 1}", env.context) end)

      quote do
        def dynamic(unquote(count), {unquote(fragment_name), unquote_splicing(vars)}) do
          unquote_splicing(before)

          Ecto.Query.dynamic([unquote_splicing(var_bindings)], fragment(unquote(statement), unquote_splicing(exprs)))
        end
      end
    end)
  end

  @doc """
  ...
  """
  def build_positional_binding_ast(before \\ [], env \\ __ENV__) do
    Enum.map(1..@max_positional_bindings, fn count ->
      var_bindings = Enum.map(1..count, fn i -> Macro.var(:"b#{i - 1}", env.context) end)

      before = quote_transformer(before)

      quote do
        def dynamic(unquote(count), value) do
          unquote_splicing(before)

          Ecto.Query.dynamic([unquote_splicing(var_bindings)], ^value)
        end
      end
    end)
  end

  @doc """
  ...
  """
  def build_named_binding_and_fragment_ast(fragment_name, statement, var_names \\ [], exprs \\ [], before \\ [], env \\ __ENV__) do
    vars = Builder.create_vars(var_names, env)
    exprs = Builder.create_exprs(exprs, env)
    before = quote_transformer(before)

    [
      quote do
        def dynamic(binding_alias, {unquote(fragment_name), unquote_splicing(vars)}) do
          unquote_splicing(before)

          if binding_alias !== nil do
            Ecto.Query.dynamic([{^binding_alias, b}], fragment(unquote(statement), unquote_splicing(exprs)))
          else
            Ecto.Query.dynamic([b], fragment(unquote(statement), unquote_splicing(exprs)))
          end
        end
      end
    ]
  end

  @doc """
  ...
  """
  def build_named_binding_ast(before \\ [], _env \\ __ENV__) do
    before = quote_transformer(before)

    [
      quote do
        def dynamic(binding_alias, value) do
          unquote_splicing(before)

          if binding_alias !== nil do
            Ecto.Query.dynamic([{^binding_alias, b}], ^value)
          else
            Ecto.Query.dynamic([b], ^value)
          end
        end
      end
    ]
  end

  defp quote_transformer({:fn, _, _} = ast) do
    [
      quote do
        fun = unquote(ast)
        value = fun.(value)
      end
    ]
  end

  defp quote_transformer(_), do: []
end
