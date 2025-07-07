defmodule EctoShorts.CommonQueryAPI.FilterBuilder do
  @moduledoc false

  alias EctoShorts.CommonQueryAPI.Builder

  @max_positional_bindings Application.compile_env(
                             EctoShorts.Config.app(),
                             :max_positional_bindings
                           ) || 10

  defmacro define_base_api(filter_name, guards \\ [], before \\ []) do
    blocks = build_base_ast(filter_name, guards, before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_base_fragment_api(filter_name, params, guards \\ [], before \\ []) do
    blocks = build_base_fragment_ast(filter_name, params, guards, before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_positional_binding_api(filter_name, guards \\ [], before \\ []) do
    blocks = build_positional_binding_ast(filter_name, guards, before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_positional_binding_and_fragment_api(
             filter_name,
             params,
             guards \\ [],
             before \\ []
           ) do
    blocks =
      build_positional_binding_and_fragment_ast(filter_name, params, guards, before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_named_binding_api(filter_name, guards \\ [], before \\ []) do
    blocks = build_named_binding_ast(filter_name, guards, before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_named_binding_and_fragment_api(filter_name, params, guards \\ []) do
    blocks = build_named_binding_and_fragment_ast(filter_name, params, guards, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  # API

  @doc """
  ...
  """
  def build_base_fragment_ast(filter_name, params, guards \\ [], before \\ [], env \\ __ENV__) do
    fragment_name = params.name
    fragment_input = params.input
    fragment_includes = params.includes

    {fragment_vars, fragment_ast} =
      Builder.build_fragment_ast(fragment_input, fragment_includes, env)

    guard_asts = collect_guard_asts(guards)

    before = build_transform_ast(before)

    first_binding_ast =
      if Enum.any?(guard_asts) do
        quote do
          def unquote(filter_name)(
                query,
                :first,
                {unquote_splicing([fragment_name] ++ fragment_vars)}
              )
              when unquote_splicing(guard_asts) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [b], unquote(fragment_ast))
          end
        end
      else
        quote do
          def unquote(filter_name)(
                query,
                :first,
                {unquote_splicing([fragment_name] ++ fragment_vars)}
              ) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [b], unquote(fragment_ast))
          end
        end
      end

    last_binding_ast =
      if Enum.any?(guard_asts) do
        quote do
          def unquote(filter_name)(
                query,
                :last,
                {unquote_splicing([fragment_name] ++ fragment_vars)}
              )
              when unquote_splicing(guard_asts) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [_, ..., b], ^value)
          end
        end
      else
        quote do
          def unquote(filter_name)(
                query,
                :last,
                {unquote_splicing([fragment_name] ++ fragment_vars)}
              ) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [_, ..., b], ^value)
          end
        end
      end

    [first_binding_ast, last_binding_ast]
  end

  @doc """
  ...
  """
  def build_base_ast(filter_name, guards \\ [], before \\ [], _env \\ __ENV__) do
    guard_asts = collect_guard_asts(guards)

    before = build_transform_ast(before)

    first_binding_ast =
      if Enum.any?(guard_asts) do
        quote do
          def unquote(filter_name)(query, :first, value) when unquote_splicing(guard_asts) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [b], ^value)
          end
        end
      else
        quote do
          def unquote(filter_name)(query, :first, value) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [b], ^value)
          end
        end
      end

    last_binding_ast =
      if Enum.any?(guard_asts) do
        quote do
          def unquote(filter_name)(query, :last, value) when unquote_splicing(guard_asts) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [_, ..., b], ^value)
          end
        end
      else
        quote do
          def unquote(filter_name)(query, :last, value) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [_, ..., b], ^value)
          end
        end
      end

    [first_binding_ast, last_binding_ast]
  end

  @doc """
  ...
  """
  def build_positional_binding_and_fragment_ast(
        filter_name,
        params,
        before \\ [],
        guards \\ [],
        env \\ __ENV__
      ) do
    fragment_name = params.name
    fragment_input = params.input
    fragment_includes = params.includes

    {fragment_vars, fragment_ast} =
      Builder.build_fragment_ast(fragment_input, fragment_includes, env)

    Enum.map(1..@max_positional_bindings, fn count ->
      var_bindings = Enum.map(1..count, fn i -> Macro.var(:"b#{i - 1}", env.context) end)

      guard_asts = collect_guard_asts(guards)

      before = build_transform_ast(before)

      if Enum.any?(guard_asts) do
        quote do
          def unquote(filter_name)(
                query,
                unquote(count),
                {unquote_splicing([fragment_name] ++ fragment_vars)}
              )
              when unquote_splicing(guard_asts) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(
              query,
              [unquote_splicing(var_bindings)],
              unquote(fragment_ast)
            )
          end
        end
      else
        quote do
          def unquote(filter_name)(
                query,
                unquote(count),
                {unquote_splicing([fragment_name] ++ fragment_vars)}
              ) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(
              query,
              [unquote_splicing(var_bindings)],
              unquote(fragment_ast)
            )
          end
        end
      end
    end)
  end

  @doc """
  ...
  """
  def build_positional_binding_ast(filter_name, guards \\ [], before \\ [], env \\ __ENV__) do
    Enum.map(1..@max_positional_bindings, fn count ->
      var_bindings = Enum.map(1..count, fn i -> Macro.var(:"b#{i - 1}", env.context) end)

      guard_asts = collect_guard_asts(guards)

      before = build_transform_ast(before)

      if Enum.any?(guard_asts) do
        quote do
          def unquote(filter_name)(query, unquote(count), value)
              when unquote_splicing(guard_asts) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [unquote_splicing(var_bindings)], ^value)
          end
        end
      else
        quote do
          def unquote(filter_name)(query, unquote(count), value) do
            unquote_splicing(before)

            Ecto.Query.unquote(filter_name)(query, [unquote_splicing(var_bindings)], ^value)
          end
        end
      end
    end)
  end

  @doc """
  ...
  """
  def build_named_binding_and_fragment_ast(filter_name, params, guards \\ [], env \\ __ENV__) do
    fragment_name = params.name
    fragment_input = params.input
    fragment_includes = params.includes

    {fragment_vars, fragment_ast} =
      Builder.build_fragment_ast(fragment_input, fragment_includes, env)

    var_b = Macro.var(:b, env.context)

    guard_asts = collect_guard_asts(guards)

    if Enum.any?(guard_asts) do
      [
        quote do
          def unquote(filter_name)(
                query,
                binding_alias,
                {unquote_splicing([fragment_name] ++ fragment_vars)}
              )
              when unquote_splicing(guard_asts) do
            if binding_alias !== nil do
              Ecto.Query.unquote(filter_name)(
                query,
                [{^binding_alias, unquote(var_b)}],
                unquote(fragment_ast)
              )
            else
              Ecto.Query.unquote(filter_name)(query, [unquote(var_b)], unquote(fragment_ast))
            end
          end
        end
      ]
    else
      [
        quote do
          def unquote(filter_name)(
                query,
                binding_alias,
                {unquote_splicing([fragment_name] ++ fragment_vars)}
              ) do
            if binding_alias !== nil do
              Ecto.Query.unquote(filter_name)(
                query,
                [{^binding_alias, unquote(var_b)}],
                unquote(fragment_ast)
              )
            else
              Ecto.Query.unquote(filter_name)(query, [unquote(var_b)], unquote(fragment_ast))
            end
          end
        end
      ]
    end
  end

  @doc """
  ...
  """
  def build_named_binding_ast(filter_name, guards \\ [], before \\ [], env \\ __ENV__) do
    var_b = Macro.var(:b, env.context)

    guard_asts = collect_guard_asts(guards)

    before = build_transform_ast(before)

    if Enum.any?(guard_asts) do
      [
        quote do
          def unquote(filter_name)(query, binding_alias, value)
              when unquote_splicing(guard_asts) do
            unquote_splicing(before)

            if binding_alias !== nil do
              Ecto.Query.unquote(filter_name)(query, [{^binding_alias, unquote(var_b)}], ^value)
            else
              Ecto.Query.unquote(filter_name)(query, [unquote(var_b)], ^value)
            end
          end
        end
      ]
    else
      [
        quote do
          def unquote(filter_name)(query, binding_alias, value) do
            unquote_splicing(before)

            if binding_alias !== nil do
              Ecto.Query.unquote(filter_name)(query, [{^binding_alias, unquote(var_b)}], ^value)
            else
              Ecto.Query.unquote(filter_name)(query, [unquote(var_b)], ^value)
            end
          end
        end
      ]
    end
  end

  defp build_transform_ast({:fn, _, _} = ast) do
    [
      quote do
        fun = unquote(ast)
        value = fun.(value)
      end
    ]
  end

  defp build_transform_ast(_), do: []

  defp collect_guard_asts(guards) do
    guards
    |> Enum.reduce(nil, fn name, ast ->
      if ast === nil do
        quote do: unquote(name)(value)
      else
        quote do
          unquote(ast) and unquote(name)(value)
        end
      end
    end)
    |> List.wrap()
  end
end
