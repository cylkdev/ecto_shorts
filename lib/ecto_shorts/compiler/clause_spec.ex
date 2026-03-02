defmodule EctoShorts.Compiler.ClauseSpec do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Validates and constructs clause specs for generating `apply_dynamic_expr/3` clauses.

  Use this module when building clause spec providers that generate function
  clauses for dynamic expression adapters. A clause spec is a struct containing
  AST values that describe a single function clause. After validating a spec
  with `new/1`, pass it to `EctoShorts.Compiler.ClauseBuilder` for clause
  generation.

  ## When to use clause specs

  Use `ClauseSpec` when you need to:

  * **Build custom adapters** - create dynamic expression adapters with
    database-specific operators.
  * **Generate function clauses** - produce `apply_dynamic_expr/3` clauses
    that match specific binding and expression patterns.
  * **Implement clause providers** - build modules that implement
    `EctoShorts.Compiler.ClauseSpecProvider`.
  * **Extend filter language** - add new filter operators to
    `EctoShorts.CommonFilters`.

  ## Clause spec fields

  A clause spec has five fields, four required and one optional:

  * `:binding_head` - AST for the first argument pattern (the binding selector,
    for example `{:as, nil}` or `{:at, 1}`).
  * `:key` - AST for the second argument pattern (the filter field atom,
    typically `Macro.var(:key, nil)`).
  * `:head` - AST for the third argument pattern (the expression structure,
    for example `{:==, val}` or `{:>, val}`).
  * `:body` - AST returned by the clause body (the dynamic expression that
    Ecto will evaluate).
  * `:guard` - optional AST for a `when` guard clause; omit or pass `nil`
    for no guard.

  Create AST values using `quote/1` and `Macro.var/2`.

  ## Building clause specs

  Follow these steps to build a clause spec:

  ### Step 1: Define the binding pattern

  Choose which binding selector this clause will match:

      # Named binding (matches {:as, nil})
      binding_head = quote(do: {:as, nil})

      # Positional binding (matches {:at, 1})
      binding_head = quote(do: {:at, 1})

      # Any named binding (matches {:as, :post}, {:as, :user}, etc.)
      binding_alias_var = Macro.var(:binding_alias, nil)
      binding_head = quote(do: {:as, unquote(binding_alias_var)})

  ### Step 2: Define the key pattern

  The key is typically a variable that captures the field name:

      key_var = Macro.var(:key, nil)

  ### Step 3: Define the expression head pattern

  The head pattern matches the filter expression structure:

      # Equality: {:==, value}
      val_var = Macro.var(:val, nil)
      head = quote(do: {:==, unquote(val_var)})

      # Greater than: {:>, value}
      head = quote(do: {:>, unquote(val_var)})

      # IN operator: {:in, list}
      vals_var = Macro.var(:vals, nil)
      head = quote(do: {:in, unquote(vals_var)})

  ### Step 4: Build the body expression

  The body is the dynamic expression that Ecto will evaluate:

      body = quote do
        Ecto.Query.dynamic([r], field(r, ^key) == ^val)
      end

  Or use AST helpers:

      body = EctoShorts.Compiler.AST.dynamic_ast(
        [quote(do: r)],
        quote(do: field(r, ^key) == ^val)
      )

  ### Step 5: Create the spec

  Combine all fields into a spec:

      ClauseSpec.new(%{
        binding_head: binding_head,
        key: key_var,
        head: head,
        body: body
      })

  ## Common patterns

  ### Pattern 1: Simple equality clause

      ClauseSpec.new(%{
        binding_head: {:as, nil},
        key: Macro.var(:key, nil),
        head: quote(do: {:==, val}),
        body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) == ^val))
      })

  This generates:

      def apply_dynamic_expr({:as, nil}, key, {:==, val}) do
        Ecto.Query.dynamic([r], field(r, ^key) == ^val)
      end

  ### Pattern 2: Comparison clause with guard

      ClauseSpec.new(%{
        binding_head: {:as, nil},
        key: Macro.var(:key, nil),
        head: quote(do: {:>, val}),
        body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) > ^val)),
        guard: quote(do: is_number(val))
      })

  This generates:

      def apply_dynamic_expr({:as, nil}, key, {:>, val}) when is_number(val) do
        Ecto.Query.dynamic([r], field(r, ^key) > ^val)
      end

  ### Pattern 3: List membership clause

      ClauseSpec.new(%{
        binding_head: {:as, nil},
        key: Macro.var(:key, nil),
        head: quote(do: {:in, vals}),
        body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) in ^vals)),
        guard: quote(do: is_list(vals))
      })

  This generates:

      def apply_dynamic_expr({:as, nil}, key, {:in, vals}) when is_list(vals) do
        Ecto.Query.dynamic([r], field(r, ^key) in ^vals)
      end

  ### Pattern 4: Named binding clause

      binding_alias_var = Macro.var(:binding_alias, nil)

      ClauseSpec.new(%{
        binding_head: {:as, binding_alias_var},
        key: Macro.var(:key, nil),
        head: quote(do: {:==, val}),
        body: quote do
          Ecto.Query.dynamic([{^binding_alias, r}], field(r, ^key) == ^val)
        end
      })

  This generates:

      def apply_dynamic_expr({:as, binding_alias}, key, {:==, val}) do
        Ecto.Query.dynamic([{^binding_alias, r}], field(r, ^key) == ^val)
      end

  ## Guard expressions

  Guards let you add runtime checks to clause matching. Use guards when you
  need to:

  * **Validate value types** - ensure the value is a number, list, or string.
  * **Check value ranges** - verify the value is within acceptable bounds.
  * **Prevent invalid operations** - skip clauses that would raise errors.

  ### Guard examples

  **Type check:**

      guard: quote(do: is_number(val))
      guard: quote(do: is_list(vals))
      guard: quote(do: is_binary(str))

  **Range check:**

      guard: quote(do: val > 0)
      guard: quote(do: val >= 0 and val <= 100)

  **Multiple conditions:**

      guard: quote(do: is_list(vals) and length(vals) > 0)

  ## Generated clause examples

  ### Input spec

      ClauseSpec.new(%{
        binding_head: {:at, 1},
        key: Macro.var(:key, nil),
        head: quote(do: {:>=, val}),
        body: quote(do: Ecto.Query.dynamic([r], field(r, ^key) >= ^val))
      })

  ### Output clause

      def apply_dynamic_expr({:at, 1}, key, {:>=, val}) do
        Ecto.Query.dynamic([r], field(r, ^key) >= ^val)
      end

  ## Validation and errors

  `new/1` validates that all required keys are present and raises when they
  are missing or when unknown keys are provided.

  ### Missing required keys

      ClauseSpec.new(%{key: :id})
      # ** (ArgumentError) the following keys must also be given when building
      # struct EctoShorts.Compiler.ClauseSpec: [:binding_head, :head, :body]

  ### Unknown keys

      ClauseSpec.new(%{
        binding_head: {:as, nil},
        key: Macro.var(:key, nil),
        head: quote(do: {:==, val}),
        body: quote(do: dynamic([r], field(r, ^key) == ^val)),
        unknown_field: :value
      })
      # ** (KeyError) key :unknown_field not found

  > #### Shape only {: .info}
  >
  > `ClauseSpec` does not validate the *meaning* of `:head` or `:body` -
  > only that the required keys exist and the struct can be constructed.
  > Invalid AST will cause compilation errors later when the clause is
  > generated.

  ## Integration with compiler

  Clause specs are used by `EctoShorts.Compiler` to generate function clauses
  at compile time:

      # In a clause spec provider module:
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

  The compiler calls this function for each binding pattern and generates
  the corresponding `apply_dynamic_expr/3` clauses.

  See also `EctoShorts.Compiler`, `EctoShorts.Compiler.ClauseBuilder`,
  `EctoShorts.Compiler.ClauseSpecProvider`, and `EctoShorts.Compiler.AST`.
  """

  @typedoc """
  A validated clause spec used to generate a single `apply_dynamic_expr/3`
  function clause.

  All field values are Elixir AST terms produced by `quote/1` or
  `Macro.var/2`.

  * `:binding_head` - AST for the first argument pattern (the binding
    selector, e.g. `{:as, nil}` or a positional binding expression).
  * `:key` - AST for the second argument pattern (the filter field atom,
    e.g. `Macro.var(:key, nil)`).
  * `:head` - AST for the third argument pattern (the expression head,
    e.g. `{:==, vals}`).
  * `:body` - AST returned by the generated clause body. This is the
    dynamic expression that Ecto will evaluate.
  * `:guard` - optional AST for a `when` guard on the generated clause.
    `nil` means no guard.
  """
  @type t() :: %__MODULE__{
          binding_head: Macro.t(),
          key: Macro.t(),
          head: Macro.t(),
          body: Macro.t(),
          guard: Macro.t() | nil
        }

  @enforce_keys [:binding_head, :key, :head, :body]
  defstruct [:binding_head, :key, :head, :body, :guard]

  @doc """
  Creates a `%ClauseSpec{}` struct from a map, keyword list, or existing struct.

  Validates that all required keys (`:binding_head`, `:key`, `:head`,
  `:body`) are present. The optional `:guard` key defaults to `nil` when
  not provided.

  Returns the validated `%ClauseSpec{}` struct, or raises if the input is
  invalid.

  ## Errors

  * Raises `ArgumentError` when any of the `@enforce_keys` (`:binding_head`,
    `:key`, `:head`, `:body`) are missing.
  * Raises `KeyError` when an unrecognised key is provided.

  Note: `new/1` does not validate the *meaning* of `:head` or `:body` - only
  that those keys exist and the struct can be constructed.

  ## Examples

      iex> spec =
      ...>   EctoShorts.Compiler.ClauseSpec.new(%{
      ...>     binding_head: quote(do: {:as, nil}),
      ...>     key: Macro.var(:key, nil),
      ...>     head: quote(do: {:==, vals}),
      ...>     body: quote(do: Ecto.Query.dynamic([q], field(q, ^key) in ^vals))
      ...>   })
      iex> match?(%EctoShorts.Compiler.ClauseSpec{}, spec)
      true

      iex> EctoShorts.Compiler.ClauseSpec.new(%{key: :id})
      ** (ArgumentError) the following keys must also be given when building struct EctoShorts.Compiler.ClauseSpec: [:binding_head, :head, :body]

  See also `EctoShorts.Compiler`.
  """
  @spec new(attrs :: map() | keyword() | t()) :: t()
  def new(%__MODULE__{} = spec) do
    spec
  end

  def new(attrs) when is_map(attrs) do
    new(Map.to_list(attrs))
  end

  def new(attrs) when is_list(attrs) do
    struct!(__MODULE__, attrs)
  end
end
