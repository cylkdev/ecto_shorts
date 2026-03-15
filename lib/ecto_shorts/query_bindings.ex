defmodule EctoShorts.QueryBindings do
  @moduledoc false
  alias EctoShorts.Config

  @default_max_positional_bindings 10

  @spec query_binding_contracts(atom(), keyword()) :: Macro.t()
  def query_binding_contracts(context \\ __MODULE__, opts \\ []) do
    max_positional_bindings =
      opts[:positions] || Config.max_positional_bindings() || @default_max_positional_bindings

    target_binding_var = Macro.var(:q, context)
    binding_alias_var = Macro.var(:binding_alias, context)
    step_var = Macro.var(:_, context)

    binding_patterns =
      [
        {{:as, nil}, [target_binding_var]},
        {{:as, binding_alias_var},
         [
           quote do
             {^unquote(binding_alias_var), unquote(target_binding_var)}
           end
         ]}
      ] ++
        Enum.map(1..max_positional_bindings, fn index ->
          {{:at, index}, positional_binding_vars(index, target_binding_var, step_var)}
        end)

    {target_binding_var, binding_patterns}
  end

  def negated_expr(expr) do
    quote do
      not unquote(expr)
    end
  end

  def special_form_ast(left, :in, right) do
    quote do
      unquote(left) in unquote(right)
    end
  end

  def special_form_ast(left, op, right) when op in [:==, :eq] do
    quote do
      unquote(left) == unquote(right)
    end
  end

  def special_form_ast(left, op, right) when op in [:!=, :ne] do
    quote do
      unquote(left) != unquote(right)
    end
  end

  def special_form_ast(left, op, right) when op in [:>, :gt] do
    quote do
      unquote(left) > unquote(right)
    end
  end

  def special_form_ast(left, op, right) when op in [:<, :lt] do
    quote do
      unquote(left) < unquote(right)
    end
  end

  def special_form_ast(left, op, right) when op in [:>=, :gte] do
    quote do
      unquote(left) >= unquote(right)
    end
  end

  def special_form_ast(left, op, right) when op in [:<=, :lte] do
    quote do
      unquote(left) <= unquote(right)
    end
  end

  def special_form_ast(left, :+, right) do
    quote do
      unquote(left) + unquote(right)
    end
  end

  def special_form_ast(left, :-, right) do
    quote do
      unquote(left) - unquote(right)
    end
  end

  def special_form_ast(left, :*, right) do
    quote do
      unquote(left) * unquote(right)
    end
  end

  def special_form_ast(left, :/, right) do
    quote do
      unquote(left) / unquote(right)
    end
  end

  def pinned_ast(var) do
    quote do
      ^unquote(var)
    end
  end

  def dyn_expr({:as, target_var}, q_var, field_expr, _context) do
    quote do
      if is_nil(unquote(target_var)) do
        dynamic([unquote(q_var)], unquote(field_expr))
      else
        dynamic([{^unquote(target_var), unquote(q_var)}], unquote(field_expr))
      end
    end
  end

  def dyn_expr({:at, index}, q_var, field_expr, context) do
    step_var = Macro.var(:_, context)
    query_binding_vars = positional_binding_vars(index, q_var, step_var)

    quote do
      dynamic([unquote_splicing(query_binding_vars)], unquote(field_expr))
    end
  end

  def positional_binding_vars(index, q_var, step_var) when is_integer(index) and index >= 1 do
    if index === 1 do
      [q_var]
    else
      1..(index - 1)
      |> Enum.map(fn _ -> step_var end)
      |> Kernel.++([q_var])
    end
  end
end
