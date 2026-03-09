defmodule EctoShorts.Dynamics.Postgres.ScalarExprBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc false

  alias EctoShorts.Generator.Blueprint
  alias EctoShorts.Dynamics.Helpers

  @keys [
    :==,
    :eq
  ]

  @behaviour EctoShorts.Generator.ClauseSpec

  @impl true
  def keys, do: @keys

  @impl true
  def specs_for(key, {binding_directive, target_var}, q_var, opts) do
    context = opts[:context]

    value_var = Macro.var(:value, context)
    key_var = Macro.var(:key, context)
    field_expr = field_expr(key, q_var, {key_var, value_var})

    [
      %Blueprint{
        guard: nil,
        key: key_var,
        head: {key, nil},
        body:
          Helpers.dyn_expr(
            {binding_directive, target_var},
            q_var,
            field_expr(key, q_var, {key_var, nil}),
            context
          )
      },
      %Blueprint{
        guard: quote(do: not is_list(unquote(value_var))),
        key: key_var,
        head: {key, value_var},
        body:
          Helpers.dyn_expr(
            {binding_directive, target_var},
            q_var,
            field_expr,
            context
          )
      }
    ]
  end

  @doc false
  def field_expr(key, q_var, {key_var, nil}) when key in @keys do
    quote do
      is_nil(field(unquote(q_var), ^unquote(key_var)))
    end
  end

  def field_expr(key, q_var, {key_var, value_var}) when key in @keys do
    quote do
      field(unquote(q_var), ^unquote(key_var)) == ^unquote(value_var)
    end
  end
end
