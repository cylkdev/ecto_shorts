defmodule EctoShorts.Compiler.ClauseSpecProvider do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Defines the behavior for clause spec providers.

  Use this module when building custom dynamic expression adapters that need
  to generate `compose/3` clauses at compile time. A clause spec
  provider implements the `clause_specs/4` callback, which returns a list of
  clause specs for each binding pattern.

  ## When to implement a provider

  Implement a clause spec provider when you need to:

  * **Build a custom adapter** - create a dynamic expression adapter with
    database-specific operators.
  * **Generate clauses programmatically** - produce many similar clauses
    with different binding patterns.
  * **Support multiple operators** - handle equality, comparison, pattern
    matching, and custom operators.
  * **Integrate with EctoShorts.Compiler** - use the compiler to generate
    function clauses at compile time.

  ## Implementing a provider

  A provider module must implement the `clause_specs/4` callback:

      defmodule MyApp.Adapter.Specs do
        @behaviour EctoShorts.Compiler.ClauseSpecProvider

        alias EctoShorts.Compiler.ClauseSpec

        @impl true
        def clause_specs(_context, binding_head, _target_binding, binding_bodies) do
          Enum.map(binding_bodies, fn _binding_body ->
            ClauseSpec.new(%{
              binding_head: binding_head,
              key: Macro.var(:key, nil),
              head: quote(do: {:==, val}),
              body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) == ^val))
            })
          end)
        end
      end

  Then use it with the compiler:

      defmodule MyApp.Adapter do
        use EctoShorts.Compiler, specs: MyApp.Adapter.Specs
      end

  ## Callback parameters

  The `clause_specs/4` callback receives four parameters:

  ### Parameter 1: context

  The module being compiled (for example, `MyApp.Adapter.Compiled`). Use this
  to create unique variable names scoped to the compiled module:

      context = MyApp.Adapter.Compiled
      var = Macro.var(:my_var, context)

  ### Parameter 2: binding_head

  The binding pattern AST for this set of clauses. Examples:

  * `{:as, nil}` - root named binding
  * `{:as, binding_alias_var}` - any named binding
  * `{:at, 1}` - first positional binding
  * `{:at, 2}` - second positional binding

  Use this as the `:binding_head` field in your clause specs.

  ### Parameter 3: target_binding

  A variable AST representing the target binding (typically `Macro.var(:q, context)`).
  This is the variable name used in the binding list of `dynamic/2` expressions.

  ### Parameter 4: binding_bodies

  A list of binding body ASTs. For most providers, this list has one element.
  Use this to build the binding list for `dynamic/2` expressions:

      [binding_body] = binding_bodies
      body = quote do
        Ecto.Query.dynamic([unquote(binding_body)], ...)
      end

  ## Return value requirements

  The callback must return a list of clause specs. Each spec can be:

  * A `%ClauseSpec{}` struct (recommended)
  * A map with clause spec fields
  * A keyword list with clause spec fields

  The compiler validates each spec and generates the corresponding function
  clause.

  ## Provider patterns

  ### Pattern 1: Single operator provider

  Generate clauses for one operator:

      def clause_specs(_context, binding_head, _target_binding, binding_bodies) do
        Enum.map(binding_bodies, fn _binding_body ->
          ClauseSpec.new(%{
            binding_head: binding_head,
            key: Macro.var(:key, nil),
            head: quote(do: {:==, val}),
            body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) == ^val))
          })
        end)
      end

  ### Pattern 2: Multiple operator provider

  Generate clauses for several operators:

      def clause_specs(_context, binding_head, _target_binding, binding_bodies) do
        operators = [
          {:==, quote(do: field(r, ^key) == ^val)},
          {:!=, quote(do: field(r, ^key) != ^val)},
          {:>, quote(do: field(r, ^key) > ^val)},
          {:<, quote(do: field(r, ^key) < ^val)}
        ]

        for {op, expr} <- operators, _binding_body <- binding_bodies do
          ClauseSpec.new(%{
            binding_head: binding_head,
            key: Macro.var(:key, nil),
            head: quote(do: {unquote(op), val}),
            body: quote(do: Ecto.Query.dynamic([r], unquote(expr)))
          })
        end
      end

  ### Pattern 3: Provider with guards

  Add guards to validate value types:

      def clause_specs(_context, binding_head, _target_binding, binding_bodies) do
        Enum.map(binding_bodies, fn _binding_body ->
          ClauseSpec.new(%{
            binding_head: binding_head,
            key: Macro.var(:key, nil),
            head: quote(do: {:>, val}),
            body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) > ^val)),
            guard: quote(do: is_number(val))
          })
        end)
      end

  ### Pattern 4: Provider using AST helpers

  Use `EctoShorts.Compiler.AST` helpers for cleaner code:

      alias EctoShorts.Compiler.AST

      def clause_specs(_context, binding_head, _target_binding, binding_bodies) do
        Enum.map(binding_bodies, fn binding_body ->
          key_var = Macro.var(:key, nil)
          val_var = Macro.var(:val, nil)

          field_ref = AST.field_ast(quote(do: r), key_var)
          expr = quote(do: unquote(field_ref) == ^unquote(val_var))

          ClauseSpec.new(%{
            binding_head: binding_head,
            key: key_var,
            head: quote(do: {:==, unquote(val_var)}),
            body: AST.dynamic_ast([binding_body], expr)
          })
        end)
      end

  ## Complete provider example

  A provider that handles equality, comparison, and list membership:

      defmodule MyApp.Adapter.Specs do
        @behaviour EctoShorts.Compiler.ClauseSpecProvider

        alias EctoShorts.Compiler.{AST, ClauseSpec}

        @impl true
        def clause_specs(_context, binding_head, _target_binding, binding_bodies) do
          Enum.flat_map(binding_bodies, fn binding_body ->
            [
              equality_spec(binding_head, binding_body),
              not_equal_spec(binding_head, binding_body),
              greater_than_spec(binding_head, binding_body),
              less_than_spec(binding_head, binding_body),
              in_spec(binding_head, binding_body)
            ]
          end)
        end

        defp equality_spec(binding_head, binding_body) do
          key_var = Macro.var(:key, nil)
          val_var = Macro.var(:val, nil)

          ClauseSpec.new(%{
            binding_head: binding_head,
            key: key_var,
            head: quote(do: {:==, unquote(val_var)}),
            body: AST.dynamic_ast(
              [binding_body],
              quote(do: field(r, ^key) == ^val)
            )
          })
        end

        defp not_equal_spec(binding_head, binding_body) do
          key_var = Macro.var(:key, nil)
          val_var = Macro.var(:val, nil)

          ClauseSpec.new(%{
            binding_head: binding_head,
            key: key_var,
            head: quote(do: {:!=, unquote(val_var)}),
            body: AST.dynamic_ast(
              [binding_body],
              quote(do: field(r, ^key) != ^val)
            )
          })
        end

        defp greater_than_spec(binding_head, binding_body) do
          key_var = Macro.var(:key, nil)
          val_var = Macro.var(:val, nil)

          ClauseSpec.new(%{
            binding_head: binding_head,
            key: key_var,
            head: quote(do: {:>, unquote(val_var)}),
            body: AST.dynamic_ast(
              [binding_body],
              quote(do: field(r, ^key) > ^val)
            ),
            guard: quote(do: is_number(val))
          })
        end

        defp less_than_spec(binding_head, binding_body) do
          key_var = Macro.var(:key, nil)
          val_var = Macro.var(:val, nil)

          ClauseSpec.new(%{
            binding_head: binding_head,
            key: key_var,
            head: quote(do: {:<, unquote(val_var)}),
            body: AST.dynamic_ast(
              [binding_body],
              quote(do: field(r, ^key) < ^val)
            ),
            guard: quote(do: is_number(val))
          })
        end

        defp in_spec(binding_head, binding_body) do
          key_var = Macro.var(:key, nil)
          vals_var = Macro.var(:vals, nil)

          ClauseSpec.new(%{
            binding_head: binding_head,
            key: key_var,
            head: quote(do: {:in, unquote(vals_var)}),
            body: AST.dynamic_ast(
              [binding_body],
              quote(do: field(r, ^key) in ^vals)
            ),
            guard: quote(do: is_list(vals))
          })
        end
      end

  ## Troubleshooting

  **Problem:** Compiler raises "Expected ... to export clause_specs/4".

  **Solution:** Add the `@behaviour` attribute and implement `clause_specs/4`
  with the correct arity (four parameters).

  **Problem:** Generated clauses do not compile.

  **Solution:** Check that the `:body` field in your specs contains valid
  Ecto query AST. Use `Macro.to_string/1` to inspect the generated code.

  **Problem:** Clauses do not match at runtime.

  **Solution:** Verify that the `:head` pattern in your specs matches the
  expression structure you are passing to `compose/3`.

  **Problem:** Too many or too few clauses generated.

  **Solution:** Check the length of the list you return from `clause_specs/4`.
  Each spec becomes one function clause.

  See also `EctoShorts.Compiler`, `EctoShorts.Compiler.ClauseSpec`,
  `EctoShorts.Compiler.ClauseBuilder`, and `EctoShorts.Compiler.AST`.
  """

  alias EctoShorts.Compiler.ClauseSpec

  @callback clause_specs(
              context :: module(),
              binding_head_ast :: Macro.t(),
              target_binding_var :: Macro.t(),
              binding_body_asts :: [Macro.t()]
            ) :: [ClauseSpec.t() | map() | keyword()]
end
