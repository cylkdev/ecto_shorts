defmodule EctoShorts.Compiler.AST do
  @moduledoc since: "3.0.0"
  @moduledoc """
  AST builders for generating dynamic query expressions at compile time.

  Use this module when building clause spec providers that need to generate
  quoted AST for `Ecto.Query.dynamic/2` expressions, field references, negations,
  or pattern matching expressions. These helpers produce the quoted code that
  becomes part of compiled `compose/3` function clauses.

  ## When to use AST helpers

  Use these functions when you need to:

  * **Build clause specs** - generate the `:body` field for a `ClauseSpec`
    that will be compiled into an `compose/3` clause.
  * **Create dynamic expressions** - build `dynamic/2` calls with proper
    binding references at compile time.
  * **Reference fields** - generate `field/2` calls that work with any
    binding selector.
  * **Negate expressions** - wrap expressions in `not/1` for negated filters.
  * **Build pattern matchers** - create search patterns for LIKE/ILIKE
    operators.

  ## How AST helpers work

  Each helper function returns quoted AST (the result of `quote do ... end`)
  that will be injected into generated function clauses. The AST is not
  evaluated immediately - it becomes part of the compiled module code.

  ### Example flow

  When you call `dynamic_ast/2`:

      binding_bodies = [quote(do: q)]
      expr = quote(do: field(q, ^key) == ^val)
      ast = dynamic_ast(binding_bodies, expr)

  The result is quoted AST that represents:

      Ecto.Query.dynamic([q], field(q, ^key) == ^val)

  This AST is then used in a clause spec's `:body` field, which gets compiled
  into a function clause.

  ## AST building patterns

  ### Pattern 1: Build a simple equality expression

      binding_bodies = [quote(do: r)]
      key_var = Macro.var(:key, nil)
      val_var = Macro.var(:val, nil)

      field_ref = field_ast(quote(do: r), key_var)
      expr = quote(do: unquote(field_ref) == ^unquote(val_var))
      dynamic_ast(binding_bodies, expr)

  This generates AST for:

      dynamic([r], field(r, ^key) == ^val)

  ### Pattern 2: Build a negated expression

      binding_bodies = [quote(do: r)]
      key_var = Macro.var(:key, nil)
      val_var = Macro.var(:val, nil)

      field_ref = field_ast(quote(do: r), key_var)
      expr = quote(do: unquote(field_ref) != ^unquote(val_var))
      negated = not_ast(expr)
      dynamic_ast(binding_bodies, negated)

  This generates AST for:

      dynamic([r], not (field(r, ^key) != ^val))

  ### Pattern 3: Build a LIKE pattern expression

      val_var = Macro.var(:val, nil)
      pattern = search_query_ast(val_var)
      # pattern becomes: "%\#{val}%"

      binding_bodies = [quote(do: r)]
      key_var = Macro.var(:key, nil)
      field_ref = field_ast(quote(do: r), key_var)
      expr = quote(do: like(unquote(field_ref), ^unquote(pattern)))
      dynamic_ast(binding_bodies, expr)

  This generates AST for:

      dynamic([r], like(field(r, ^key), ^"%\#{val}%"))

  ### Pattern 4: Build an IN expression with list

      val_var = Macro.var(:vals, nil)

      binding_bodies = [quote(do: r)]
      key_var = Macro.var(:key, nil)
      field_ref = field_ast(quote(do: r), key_var)
      expr = quote(do: unquote(field_ref) in ^unquote(val_var))
      dynamic_ast(binding_bodies, expr)

  This generates AST for:

      dynamic([r], field(r, ^key) in ^vals)

  ## Generated code examples

  The AST helpers produce quoted code that becomes part of compiled functions.
  Here's what the generated clauses look like after compilation:

  ### Input (using AST helpers)

      binding_bodies = [quote(do: r)]
      key_var = Macro.var(:key, nil)
      val_var = Macro.var(:val, nil)

      ClauseSpec.new(%{
        binding_head: {:as, nil},
        key: key_var,
        head: quote(do: {:==, unquote(val_var)}),
        body: dynamic_ast(
          binding_bodies,
          quote(do: unquote(field_ast(quote(do: r), key_var)) == ^unquote(val_var))
        )
      })

  ### Output (compiled function clause)

      def compose({:as, nil}, key, {:==, val}) do
        Ecto.Query.dynamic([r], field(r, ^key) == ^val)
      end

  ## Common use cases

  ### Use case 1: Equality comparison

  Build a clause that matches `field == value`:

      dynamic_ast(
        [quote(do: r)],
        quote(do: unquote(field_ast(quote(do: r), key_var)) == ^unquote(val_var))
      )

  ### Use case 2: Range comparison

  Build a clause that matches `field > value`:

      dynamic_ast(
        [quote(do: r)],
        quote(do: unquote(field_ast(quote(do: r), key_var)) > ^unquote(val_var))
      )

  ### Use case 3: Pattern matching

  Build a clause that matches `field LIKE pattern`:

      pattern = search_query_ast(val_var)
      dynamic_ast(
        [quote(do: r)],
        quote(do: like(unquote(field_ast(quote(do: r), key_var)), ^unquote(pattern)))
      )

  ### Use case 4: List membership

  Build a clause that matches `field IN list`:

      dynamic_ast(
        [quote(do: r)],
        quote(do: unquote(field_ast(quote(do: r), key_var)) in ^unquote(vals_var))
      )

  ## Integration with ClauseSpec

  AST helpers are typically used when building the `:body` field of a
  `ClauseSpec`:

      ClauseSpec.new(%{
        binding_head: {:as, nil},
        key: Macro.var(:key, nil),
        head: quote(do: {:==, val}),
        body: dynamic_ast(
          [quote(do: r)],
          quote(do: field(r, ^key) == ^val)
        )
      })

  The `:body` field contains the AST that will become the function body of
  the generated `compose/3` clause.

  See also `EctoShorts.Compiler.ClauseSpec`, `EctoShorts.Compiler.ClauseBuilder`,
  and `EctoShorts.Compiler`.
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
