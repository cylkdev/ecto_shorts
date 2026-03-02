defmodule EctoShorts.SchemalessQuery do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Describes a schemaless query operation for `EctoShorts.CommonFilters.convert_params_to_filter/3`.

  ## Purpose

  A `SchemalessQuery` maps client-facing table names to internal source
  terms (schema modules, table strings, `{table, schema}` tuples, or
  queries). The caller builds the struct once and passes it as the first
  argument to `convert_params_to_filter/3`. The params use the
  `source_key` (default `:table`) as a top-level key to reference a
  table by name.

  This keeps the internal source details hidden from the client. The
  client only knows the name, and the struct controls which sources are
  available.

  ## Example

      %{tables: %{"posts" => MyApp.Schema.Post, "comments" => MyApp.Schema.Comment}}
      |> EctoShorts.SchemalessQuery.new()
      |> EctoShorts.CommonFilters.convert_params_to_filter(
          %{table: "posts", published: true},
          []
      )

  With a custom `:source_key`:

      %{tables: %{"posts" => MyApp.Schema.Post}, source_key: :source}
      |> EctoShorts.SchemalessQuery.new()
      |> EctoShorts.CommonFilters.convert_params_to_filter(
          %{source: "posts"},
          []
      )

  ## Fields

  * `tables` (default: `%{}`) - a map where each key is a string name
    and each value is any valid source term accepted by
    `convert_params_to_filter/3` (schema module, table string,
    `{table, schema}` tuple, or `Ecto.Query`).

  * `source_key` (default: `:table`) - the atom key in the params
    that identifies which entry in `tables` to use.

  See also `EctoShorts.CommonFilters`.
  """

  defstruct tables: %{}, source_key: :table

  @type t :: %__MODULE__{
          tables: %{optional(String.t()) => term()},
          source_key: atom()
        }

  @doc """
  Creates a new `SchemalessQuery` struct from the given attributes.

  ## Examples

      iex> EctoShorts.SchemalessQuery.new(%{tables: %{"posts" => "posts"}})
      %EctoShorts.SchemalessQuery{tables: %{"posts" => "posts"}, source_key: :table}

  See also `EctoShorts.CommonFilters.convert_params_to_filter/3`.
  """
  @spec new(Enumerable.t()) :: t()
  def new(attrs) do
    struct!(__MODULE__, attrs)
  end
end
