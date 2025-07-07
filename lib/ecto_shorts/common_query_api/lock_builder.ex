defmodule EctoShorts.CommonQueryAPI.LockBuilder do
  @moduledoc false

  # alias EctoShorts.CommonQueryAPI.Builder

  @max_positional_bindings Application.compile_env(
                             EctoShorts.Config.app(),
                             :max_positional_bindings
                           ) || 10

  defmacro define_base_api(name, input, before \\ []) do
    blocks = build_base_ast(name, input, before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_positional_binding_api(name, input, before \\ [], opts \\ []) do
    blocks = build_positional_binding_ast(name, input, before, __CALLER__, opts)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_named_binding_api(name, input, before \\ []) do
    blocks = build_named_binding_ast(name, input, before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  @doc """
  ...
  """
  def build_base_ast(name, input, before, env) do
    var_elem = Macro.var(:b, env.context)

    lock_name = :"lock_#{Macro.underscore(name)}"

    [
      quote do
        def unquote(lock_name)(query, :first) do
          unquote_splicing(before)

          Ecto.Query.lock(query, [unquote(var_elem)], unquote(input))
        end
      end,
      quote do
        def unquote(lock_name)(query, :last) do
          unquote_splicing(before)

          Ecto.Query.lock(query, [_, ..., b], unquote(input))
        end
      end
    ]
  end

  @doc """
  ...
  """
  def build_positional_binding_ast(name, input, before, env, opts) do
    lock_name = :"lock_#{Macro.underscore(name)}"

    Enum.map(1..@max_positional_bindings, fn count ->
      var_names = Enum.map(1..count, fn i -> :"b#{i - 1}" end)
      var_binding = Enum.map(var_names, fn name -> Macro.var(name, env.context) end)

      quote do
        def unquote(lock_name)(query, unquote(count)) do
          unquote_splicing(before)

          Ecto.Query.lock(query, [unquote_splicing(var_binding)], unquote(input))
        end
      end
    end)
  end

  @doc """
  ...
  """
  def build_named_binding_ast(name, input, before, env) do
    var_elem = Macro.var(:b, env.context)

    lock_name = :"lock_#{Macro.underscore(name)}"

    [
      quote do
        def unquote(lock_name)(query, binding_alias) do
          unquote_splicing(before)

          if binding_alias !== nil do
            Ecto.Query.lock(query, [{^binding_alias, unquote(var_elem)}], unquote(input))
          else
            Ecto.Query.lock(query, [unquote(var_elem)], unquote(input))
          end
        end
      end
    ]
  end
end
