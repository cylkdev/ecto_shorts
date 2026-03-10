defmodule EctoShorts.Dynamics.Helpers do
  @doc """
  ...
  """
  def negated_expr(expr) do
    quote do
      not unquote(expr)
    end
  end

  @doc """
  ...
  """
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

  @doc """
  ...
  """
  def pinned_ast(var) do
    quote do
      ^unquote(var)
    end
  end

  @doc """
  ...
  """
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

  @doc """
  ...
  """
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
