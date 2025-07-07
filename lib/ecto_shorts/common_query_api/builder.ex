defmodule EctoShorts.CommonQueryAPI.Builder do
  @moduledoc false

  def create_vars(names, env \\ __ENV__) do
    Enum.map(names, &Macro.var(&1, env.context))
  end

  def create_exprs(kwd, env \\ __ENV__) do
    kwd
    |> Enum.reduce([], fn
      {:field, key}, acc ->
        var_b = Macro.var(:b, env.context)

        var_key = Macro.var(key, env.context)
        quoted_field = (quote do: field(unquote(var_b), ^unquote(var_key)))

        [quoted_field | acc]

      {:var, name}, acc ->
        var_name = Macro.var(name, env.context)

        pinned_var = (quote do: ^unquote(var_name))

        [pinned_var | acc]

      {:value, value}, acc ->
        quoted_value = (quote do: unquote(value))

        [quoted_value | acc]

    end)
    |> Enum.reverse()
  end
end
