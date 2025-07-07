defmodule EctoShorts.CommonQueryAPI.Example do
  @doc """
  block =
    quote do
      Ecto.Query.dynamic([q], field(q, :id) == 1)
    end
  EctoShorts.CommonQueryAPI.Example.build_positional_binding_ast(block)
  """
  def build_positional_binding_ast(transform_fun \\ nil, guards \\ [], max \\ 10, env \\ __ENV__)
      when is_integer(max) and max > 0 do
    Enum.map(1..max, fn count ->
      var_bindings = Enum.map(1..count, fn i -> Macro.var(:"b#{i - 1}", env.context) end)

      guards_asts = collect_guard_asts(guards)

      before = build_before_ast(transform_fun)

      if Enum.any?(guards_asts) do
        quote do
          def dynamic(unquote(count), value) when unquote_splicing(guards_asts) do
            unquote_splicing(before)

            Ecto.Query.dynamic([unquote_splicing(var_bindings)], ^value)
          end
        end
      else
        quote do
          def dynamic(unquote(count), value) do
            unquote_splicing(before)

            Ecto.Query.dynamic([unquote_splicing(var_bindings)], ^value)
          end
        end
      end
    end)
  end

  defp build_before_ast(nil) do
    []
  end

  defp build_before_ast({:fn, _, _} = ast) do
    [
      quote do
        fun = unquote(ast)
        value = fun.(value)
      end
    ]
  end

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
