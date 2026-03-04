defmodule EctoShorts.Compiler.QueryBindingBuilder do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Generates binding pattern ASTs for positional and named bindings at compile time.

  Use this module when building the compiler infrastructure that needs to
  generate function clauses for every binding pattern. The builder produces
  `{binding_head, binding_body}` tuples that represent how to match and
  reference query bindings in generated `compose/3` clauses.

  ## Binding patterns explained

  Ecto queries support two types of bindings:

  * **Named bindings** - use the `:as` option to give bindings symbolic names
    (for example, `from p in Post, as: :post`).
  * **Positional bindings** - reference bindings by their position in the
    query (for example, the first join is position 1, the second is position 2).

  The query binding builder generates patterns for both types.

  ## Positional bindings

  Positional bindings use `{:at, position}` as the binding selector:

      # First binding (position 1)
      {:at, 1}

      # Second binding (position 2)
      {:at, 2}

  The builder generates patterns from 1 to `max_binding_positions`:

      # For max_binding_positions = 3
      [
        {{:at, 1}, [q]},
        {{:at, 2}, [_, q]},
        {{:at, 3}, [_, _, q]}
      ]

  The binding body list has underscores for all positions except the target
  position, which uses the target binding variable.

  ## Named bindings

  Named bindings use `{:as, name}` as the binding selector:

      # Root binding (no alias)
      {:as, nil}

      # Named binding (for example, :post, :user, :comment)
      {:as, :post}

  The builder generates two named binding patterns:

      [
        {{:as, nil}, [q]},
        {{:as, binding_alias}, [{^binding_alias, q}]}
      ]

  The first pattern matches the root binding. The second pattern matches any
  named binding by capturing the alias in a variable.

  ## Generated patterns

  For `max_binding_positions = 3`, the builder generates:

      [
        # Positional bindings
        {{:at, 1}, [q]},
        {{:at, 2}, [_, q]},
        {{:at, 3}, [_, _, q]},

        # Named bindings
        {{:as, nil}, [q]},
        {{:as, binding_alias}, [{^binding_alias, q}]}
      ]

  Each tuple contains:

  * **binding_head** - the pattern to match in the first argument of
    `compose/3`.
  * **binding_body** - the binding list to use in `dynamic/2` expressions.

  ## How it works

  The builder follows this algorithm:

  1. **Receive parameters** - accepts `context` (the module being compiled)
     and `max_binding_positions` (the maximum number of positional bindings).
  2. **Create variables** - generates variable ASTs for `binding_alias`, `q`,
     and `_` scoped to the context module.
  3. **Build positional patterns** - creates one pattern for each position
     from 1 to `max_binding_positions`.
  4. **Build named patterns** - creates two patterns for named bindings (root
     and aliased).
  5. **Return patterns** - combines all patterns into a single list with the
     target binding variable.

  ## Usage in compiler

  The compiler uses these patterns to generate clauses for every binding type:

      {target_binding, patterns} = QueryBindingBuilder.query_binding_contracts(
        MyApp.Adapter.Compiled,
        3
      )

      for {binding_head, binding_bodies} <- patterns do
        specs = provider.clause_specs(context, binding_head, target_binding, binding_bodies)
        Enum.map(specs, &ClauseBuilder.clause_ast/1)
      end

  This generates clauses that match all binding patterns.

  ## Positional binding examples

  ### Position 1

      binding_head: {:at, 1}
      binding_body: [q]

  Used in a clause:

      def compose({:at, 1}, key, {:==, val}) do
        dynamic([q], field(q, ^key) == ^val)
      end

  ### Position 2

      binding_head: {:at, 2}
      binding_body: [_, q]

  Used in a clause:

      def compose({:at, 2}, key, {:==, val}) do
        dynamic([_, q], field(q, ^key) == ^val)
      end

  ### Position 3

      binding_head: {:at, 3}
      binding_body: [_, _, q]

  Used in a clause:

      def compose({:at, 3}, key, {:==, val}) do
        dynamic([_, _, q], field(q, ^key) == ^val)
      end

  ## Named binding examples

  ### Root binding

      binding_head: {:as, nil}
      binding_body: [q]

  Used in a clause:

      def compose({:as, nil}, key, {:==, val}) do
        dynamic([q], field(q, ^key) == ^val)
      end

  ### Aliased binding

      binding_head: {:as, binding_alias}
      binding_body: [{^binding_alias, q}]

  Used in a clause:

      def compose({:as, binding_alias}, key, {:==, val}) do
        dynamic([{^binding_alias, q}], field(q, ^key) == ^val)
      end

  This clause matches any named binding (for example, `{:as, :post}`,
  `{:as, :user}`, `{:as, :comment}`).

  ## Configuration

  The number of positional bindings is controlled by `max_binding_positions`.
  Increase this value when your queries join more tables than the default:

      defmodule MyApp.Adapter do
        use EctoShorts.Compiler,
          specs: MyApp.Adapter.Specs,
          max_binding_positions: 10
      end

  This generates patterns for positions 1 through 10.

  See also `EctoShorts.Compiler`, `EctoShorts.Compiler.ClauseSpecProvider`,
  and `EctoShorts.CommonQuery`.
  """

  def query_binding_contracts(context, max_binding_positions) do
    binding_alias_var = Macro.var(:binding_alias, context)
    target_binding_var = Macro.var(:q, context)
    step_var = Macro.var(:_, context)

    positional_binding_patterns =
      build_binding_patterns(:positional, target_binding_var, step_var, max_binding_positions)

    named_binding_patterns =
      build_binding_patterns(
        :named,
        target_binding_var,
        binding_alias_var,
        max_binding_positions
      )

    all_binding_patterns = positional_binding_patterns ++ named_binding_patterns

    {target_binding_var, all_binding_patterns}
  end

  defp build_binding_patterns(:named, target_binding_var, binding_alias_var, _max_pos) do
    [
      {
        {:as, nil},
        [
          quote do
            unquote(target_binding_var)
          end
        ]
      },
      {
        {:as, binding_alias_var},
        [
          quote do
            {^unquote(binding_alias_var), unquote(target_binding_var)}
          end
        ]
      }
    ]
  end

  defp build_binding_patterns(:positional, target_binding_var, step_var, max_pos) do
    Enum.map(1..max_pos, fn i ->
      {{:at, i}, Enum.map(1..i, &if(&1 === i, do: target_binding_var, else: step_var))}
    end)
  end
end
