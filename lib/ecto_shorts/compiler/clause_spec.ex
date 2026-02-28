defmodule EctoShorts.Compiler.ClauseSpec do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Validates and constructs clause specs for generating `apply_dynamic_expr/3` clauses.

  A clause spec is a struct containing AST values that describe a single
  function clause for `apply_dynamic_expr/3`. After validating a spec with
  `new/1`, pass it to `EctoShorts.Compiler` for clause AST generation.

  This module does not build Ecto query expressions. It only validates the
  shape of the spec and returns a `%ClauseSpec{}` struct.

  ## Clause spec fields

  * `:binding_head` — AST for the first argument pattern (the binding selector)
  * `:key` — AST for the second argument pattern (the filter field atom)
  * `:head` — AST for the third argument pattern (the expression head)
  * `:body` — AST returned by the clause body (the dynamic expression)
  * `:guard` — optional AST for a `when` guard; omit or pass `nil` for no guard

  Create AST values using `quote/1` and `Macro.var/2`.

  ## Examples

      iex> spec =
      ...>   EctoShorts.Compiler.ClauseSpec.new(%{
      ...>     binding_head: quote(do: {:as, nil}),
      ...>     key: Macro.var(:key, nil),
      ...>     head: quote(do: {:==, v}),
      ...>     body: quote(do: Ecto.Query.dynamic([q], field(q, ^key) == ^v))
      ...>   })
      iex> match?(%EctoShorts.Compiler.ClauseSpec{}, spec)
      true

  ## Errors

  `new/1` raises `ArgumentError` when required keys are missing and
  `KeyError` when unknown keys are provided.

  > #### Shape only {: .info}
  >
  > `ClauseSpec` does not validate the *meaning* of `:head` or `:body` —
  > only that the required keys exist and the struct can be constructed.

  See also `EctoShorts.Compiler` and `EctoShorts.Dynamics.Adapter`.
  """

  @typedoc """
  A validated clause spec used to generate a single `apply_dynamic_expr/3`
  function clause.

  All field values are Elixir AST terms produced by `quote/1` or
  `Macro.var/2`.

  * `:binding_head` — AST for the first argument pattern (the binding
    selector, e.g. `{:as, nil}` or a positional binding expression).
  * `:key` — AST for the second argument pattern (the filter field atom,
    e.g. `Macro.var(:key, nil)`).
  * `:head` — AST for the third argument pattern (the expression head,
    e.g. `{:==, vals}`).
  * `:body` — AST returned by the generated clause body. This is the
    dynamic expression that Ecto will evaluate.
  * `:guard` — optional AST for a `when` guard on the generated clause.
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

  Note: `new/1` does not validate the *meaning* of `:head` or `:body` — only
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
