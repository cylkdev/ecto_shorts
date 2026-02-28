defmodule EctoShorts.Compiler.ClauseSpec do
  @moduledoc """
  Validates clause specs so they can generate `apply_dynamic_expr/3` clauses.

  A clause spec is a struct that contains AST values. You use it to describe one
  function clause for `apply_dynamic_expr/3`. You then pass the spec to
  `EctoShorts.Compiler.clause_ast/1`.

  This module does not build Ecto query expressions. It only checks the shape
  of the spec and returns a `%ClauseSpec{}` struct.

  ## Clause Specs

  A clause spec contains these keys:

    * `:binding_head` - AST for the first argument pattern
    * `:key` - AST for the second argument pattern
    * `:head` - AST for the third argument pattern
    * `:body` - AST returned by the clause body

  Create AST values using `quote/1` and `Macro.var/2`.

  ## Examples

  Validate a spec before you build a clause:

    iex> spec = %{
    ...>   binding_head: quote(do: {:as, nil}),
    ...>   key: Macro.var(:key, nil),
    ...>   head: quote(do: {:==, v}),
    ...>   body: quote(do: Ecto.Query.dynamic([q], field(q, ^key) == ^v))
    ...> }
    ...> EctoShorts.Compiler.ClauseSpec.new(spec)

  ## Errors

  `new/1` raises `ArgumentError` when required keys are missing and
  `KeyError` when unknown keys are provided.

  > NOTE: ClauseSpec does not validate the *meaning* of `:head` or `:body`.
  > It only checks that required keys exist.
  """

  @typedoc """
  A normalized clause spec.

  All values are AST values.
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
  Creates a `%ClauseSpec{}` from a map or keyword list.

  ## Return values

    * `{:ok, spec}` - a validated clause spec struct
    * `{:error, reason}` - a validation error reason

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
