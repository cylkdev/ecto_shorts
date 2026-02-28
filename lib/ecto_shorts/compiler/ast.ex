defmodule EctoShorts.Compiler.AST do
  @moduledoc since: "3.0.0"
  @moduledoc """
  AST helper functions for building dynamic query expressions at compile time.

  Provides quoted AST builders for `Ecto.Query.dynamic/2`, `field/2`,
  `not/1`, and search/pattern expressions used by the clause spec providers.
  """

  alias Ecto.Query

  @doc false
  def field_ast(target_binding_var_ast, key_var_ast) do
    quote do
      field(unquote(target_binding_var_ast), ^unquote(key_var_ast))
    end
  end

  @doc false
  def dynamic_ast(binding_body_asts, expr_ast) do
    quote do
      unquote(Query).dynamic([unquote_splicing(binding_body_asts)], unquote(expr_ast))
    end
  end

  @doc false
  def not_ast(expr_ast) do
    quote do
      not unquote(expr_ast)
    end
  end

  @doc false
  def search_query_ast(value_var_ast) do
    quote do
      "%#{unquote(value_var_ast)}%"
    end
  end

  @doc false
  def list_of_patterns_ast(values_var_ast) do
    quote do
      Enum.map(unquote(values_var_ast), &"%#{&1}%")
    end
  end

  @doc false
  def normalize_patterns_ast(value_var_ast) do
    quote do
      case unquote(value_var_ast) do
        v when is_list(v) -> Enum.map(v, &"%#{&1}%")
        v -> ["%#{v}%"]
      end
    end
  end
end
