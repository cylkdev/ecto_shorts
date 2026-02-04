defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseSpec do
  @moduledoc """
  Validates clause specs so they can generate `dynamic_field_expr/3` clauses.

  A clause spec is a struct that contains AST values. You use it to describe one
  function clause for `dynamic_field_expr/3`. You then pass the spec to
  `EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseBuilder.clause_ast/1`.

  This module does not build Ecto query expressions. It only checks the shape
  of the spec and returns a `%ClauseSpec{}` struct.

  ## Clause Specs

  A clause spec contains these keys:

    * `:kind` - tags the clause (any atom)
    * `:binding_head` - AST for the first argument pattern
    * `:key` - AST for the second argument pattern
    * `:head` - AST for the third argument pattern
    * `:body` - AST returned by the clause body

  Create AST values using `quote/1` and `Macro.var/2`.

  ## Examples

  Validate a spec before you build a clause:

    iex> spec = %{
    ...>   kind: :clause,
    ...>   binding_head: quote(do: {:as, nil}),
    ...>   key: Macro.var(:key, nil),
    ...>   head: quote(do: {:==, v}),
    ...>   body: quote(do: Ecto.Query.dynamic([q], field(q, ^key) == ^v))
    ...> }
    ...> EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseSpec.new(spec)

  ## Error Reasons

  `new/1` returns `{:error, reason}` for invalid specs:

    * `:missing_key` - a required key is missing
    * `:invalid_spec` - the input is not a map, keyword list, or `%ClauseSpec{}`

    iex> EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseSpec.new(%{kind: :clause})
    {:error, :missing_key}

  > NOTE: ClauseSpec does not validate the *meaning* of `:head` or `:body`.
  > It only checks that required keys exist.
  """

  alias NimbleOptions

  @typedoc """
  Clause kind tag.

  This is a free-form atom that lets you group or label clauses.
  `ClauseBuilder` does not branch on this value.
  """
  @type kind() :: :clause | atom()

  @typedoc """
  A normalized clause spec.

  All values are AST values.
  """
  @type t() :: %__MODULE__{
          kind: kind(),
          binding_head: Macro.t(),
          key: Macro.t(),
          head: Macro.t(),
          body: Macro.t(),
          guard: Macro.t() | nil
        }

  @enforce_keys [:kind, :binding_head, :key, :head, :body]

  defstruct kind: :clause,
            binding_head: nil,
            key: nil,
            head: nil,
            body: nil,
            guard: nil

  @schema [
    kind: [type: :atom, required: true, default: :clause],
    binding_head: [type: :any, required: true],
    key: [type: :any, required: true],
    head: [type: :any, required: true],
    body: [type: :any, required: true],
    guard: [type: :any]
  ]

  @doc """
  Creates a `%ClauseSpec{}` from a map or keyword list.

  This function validates options with `NimbleOptions`.

  ## Return values

    * `{:ok, spec}` - a validated clause spec struct
    * `{:error, reason}` - a validation error reason

  ## Examples

      iex> spec =
      ...>   EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseSpec.new(%{
      ...>     kind: :clause,
      ...>     binding_head: quote(do: {:as, nil}),
      ...>     key: Macro.var(:key, nil),
      ...>     head: quote(do: {:==, vals}),
      ...>     body: quote(do: Ecto.Query.dynamic([q], field(q, ^key) in ^vals))
      ...>   })
      iex> match?({:ok, %EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseSpec{}}, spec)
      true
  """
  @spec new(attrs :: map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(%__MODULE__{} = spec) do
    spec |> Map.from_struct() |> Map.to_list() |> new()
  end

  def new(attrs) when is_map(attrs) do
    attrs |> Map.to_list() |> new()
  end

  def new(attrs) do
    with {:ok, validated} <- NimbleOptions.validate(attrs, @schema) do
      {:ok, struct!(__MODULE__, validated)}
    end
  end

  def new!(attrs) do
    case new(attrs) do
      {:ok, spec} -> spec
      {:error, reason} -> raise ArgumentError, "Failed to create clause spec: #{inspect(reason)}"
    end
  end
end
