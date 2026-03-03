defmodule EctoShorts.Source do
  @moduledoc since: "3.0.0"
  @moduledoc """
  A `Source` is a small config struct that lets you give your API a simple,
  safe way to pick which database table (source) a request is allowed to
  query.

  Your client sends a friendly name like `"posts"`. Your server maps that
  name to the real thing you query with (a schema module like `MyApp.Schema.Post`,
  a table string, a `{table, schema}` tuple, or an `Ecto.Query`).

  You build the `Source` once, then pass it as the first argument to
  `convert_params_to_filter/3`. In the params, the client selects a source by
  setting a top-level key (by default `:table`) to one of the names you
  configured.

  This keeps internal details hidden. The client never sees schema modules or
  queries. They only know the allowed names, and the `Source` controls what
  those names can point to.

  ## Example

      %{
        tables: %{
          "posts" => MyApp.Schema.Post,
          "comments" => MyApp.Schema.Comment
        }
      }
      |> EctoShorts.Source.new()
      |> EctoShorts.CommonFilters.convert_params_to_filter(
        %{table: "posts", published: true},
        []
      )

  ## Using a custom key instead of `:table`

  If you want the client to use a different param key (for example `:source`
  instead of `:table`), set `:source_key` when you build the struct:

      %{
        tables: %{"posts" => MyApp.Schema.Post},
        source_key: :source
      }
      |> EctoShorts.Source.new()
      |> EctoShorts.CommonFilters.convert_params_to_filter(
        %{source: "posts"},
        []
      )

  ## Fields

  * `tables` (default: `%{}`)

    A map of allowed source names. Each key is the client-facing name (a string).
    Each value is a source term that `convert_params_to_filter/3` knows how to use:
    a schema module, a table string, a `{table, schema}` tuple, or an `Ecto.Query`.

  * `source_key` (default: `:table`)

    The atom key your params will use to choose a source from `tables`.

  See also `EctoShorts.CommonFilters`.
  """

  defstruct tables: %{}, source_key: :table

  @type t :: %__MODULE__{
          tables: %{optional(String.t()) => term()},
          source_key: atom()
        }

  @doc """
  Creates a new `Source` struct.

  ## Examples

      iex> EctoShorts.Source.new(%{tables: %{"posts" => "posts"}})
      %EctoShorts.Source{tables: %{"posts" => "posts"}, source_key: :table}

  See also `EctoShorts.CommonFilters.convert_params_to_filter/3`.
  """
  @spec new(Enumerable.t()) :: t()
  def new(attrs) do
    struct!(__MODULE__, attrs)
  end
end
