defmodule EctoShorts.CommonChanges do
  @moduledoc """
  `CommonChanges` is a collection of functions to help with managing
  and creating our `&changeset/2` function in our schemas.

  ### Preloading associations on change
  Often times we want to be able to change an association with
  `(put/cast)_assoc`, but we have an awkwardness of having to use
  a preload in a spot to do this. We can aleviate that by doing the following:

      defmodule MyApp.Accounts.User do
        def changeset(changeset, params) do
          changeset
            |> cast([:name, :email])
            |> validate_required([:name, :email])
            |> EctoShorts.CommonChanges.preload_change_assoc(:address)
        end
      end

  Doing this allows us to then pass address in via a map, or even using
  the struct from the database directly to add as a relation

  ### Validating relation is passed in somehow
  We can validate for a relation being passed in via id or by using our
  preload_change_assoc by doing the following:

      defmodule MyApp.Accounts.User do
        def changeset(changeset, params) do
          changeset
            |> cast([:name, :email, :address_id])
            |> validate_required([:name, :email])
            |> EctoShorts.CommonChanges.preload_change_assoc(:address,
              required_when_missing: :address_id
            )
        end
      end

  ### Conditional functions
  We can also run functions when something happens by defining conditional functions like so:

      defmodule MyApp.Accounts.User do
        alias EctoShorts.CommonChanges

        def changeset(changeset, params) do
          changeset
            |> cast([:name, :email, :address_id])
            |> validate_required([:name, :email])
            |> CommonChanges.put_when(
              &CommonChanges.changeset_field_nil?(&1, :email),
              &put_change(&1, :email, "some_default@gmail.com")
            )
        end
      end

  """

  alias Ecto.Changeset
  alias EctoShorts.{Actions, Config, SchemaHelpers}

  require Logger

  @doc since: "2.5.0"
  @doc """
  Returns `true` if the given field (or all fields in a list) have no pending change.

  Checks `Ecto.Changeset.get_change/2` for each field. If the change is
  `nil` (meaning no change was set), returns `true`.

  When `fields` is a list, returns `true` only if every field has a `nil` change.
  """
  def has_nil_change?(changeset, fields) when is_list(fields) do
    Enum.all?(fields, &has_nil_change?(changeset, &1))
  end

  def has_nil_change?(changeset, field) do
    changeset
    |> Changeset.get_change(field)
    |> is_nil()
  end

  @doc since: "2.5.0"
  @doc """
  Returns `true` if the given field (or all fields in a list) have an empty change.

  A change is considered empty if it is `[]` or `%{}`. Fields with `nil`
  changes or non-empty values return `false`.

  When `fields` is a list, returns `true` only if every field has an empty change.
  """
  def has_empty_change?(changeset, fields) when is_list(fields) do
    Enum.all?(fields, &has_empty_change?(changeset, &1))
  end

  def has_empty_change?(changeset, field) do
    case Changeset.get_change(changeset, field) do
      [] -> true
      map when map === %{} -> true
      _ -> false
    end
  end

  @doc """
  Prevents a field or list of fields from being set to nil if it already exists in persisted data.

  ## Examples

      iex> EctoShorts.CommonChanges.validate_not_unset(changeset, [:field])
  """
  def validate_not_unset(changeset, fields) when is_list(fields) do
    Enum.reduce(fields, changeset, fn field, acc_changeset ->
      validate_not_unset(acc_changeset, field)
    end)
  end

  def validate_not_unset(changeset, field) do
    original = Map.get(changeset.data, field)

    # skip if field is nil
    # check if field is being set to nil
    # skip if already errored
    should_error? =
      original !== nil and
        Changeset.changed?(changeset, field, to: nil) and
        not Keyword.has_key?(changeset.errors, field)

    if should_error? do
      Changeset.add_error(changeset, field, "can't be blank")
    else
      changeset
    end
  end

  @doc "Truncates datetime changes on the given field(s) to the specified precision."
  def truncate_datetime_change(changeset, fields, precision \\ :second)

  def truncate_datetime_change(changeset, fields, precision) when is_list(fields) do
    Enum.reduce(fields, changeset, fn field, acc_changeset ->
      truncate_datetime_change(
        acc_changeset,
        field,
        precision
      )
    end)
  end

  def truncate_datetime_change(changeset, field, precision) do
    Changeset.update_change(changeset, field, fn
      %DateTime{} = datetime -> DateTime.truncate(datetime, precision)
      %NaiveDateTime{} = naive_datetime -> NaiveDateTime.truncate(naive_datetime, precision)
      value -> value
    end)
  end

  @doc "Trims whitespace from string changes on the given field(s)."
  def trim_string_change(changeset, fields) do
    fields
    |> List.wrap()
    |> Enum.reduce(changeset, fn field, acc_changeset ->
      Changeset.update_change(acc_changeset, field, fn
        change when is_binary(change) -> String.trim(change)
        value -> value
      end)
    end)
  end

  @doc """
  Puts a change only if the field has no pending change.

  `value` can be a literal value, a 0-arity function, or a 1-arity function
  that receives the field name.
  """
  def put_new_change(changeset, field, value) do
    if Changeset.get_change(changeset, field) === nil do
      Changeset.put_change(
        changeset,
        field,
        resolve_value(value, field)
      )
    else
      changeset
    end
  end

  @doc """
  Puts a change only if the field's current value (data or changes) is `nil`.

  `value` can be a literal value, a 0-arity function, or a 1-arity function
  that receives the field name.
  """
  def put_new_value(changeset, field, value) do
    if Changeset.get_field(changeset, field) === nil do
      Changeset.put_change(
        changeset,
        field,
        resolve_value(value, field)
      )
    else
      changeset
    end
  end

  defp resolve_value(fun, field) when is_function(fun, 1), do: fun.(field)
  defp resolve_value(fun, _) when is_function(fun, 0), do: fun.()
  defp resolve_value(value, _), do: value

  @doc "Applies `change_func` to the changeset only when `when_func` returns `true`."
  def apply_when(changeset, when_func, change_func) do
    if when_func.(changeset) do
      case change_func.(changeset) do
        changeset when is_struct(changeset, Changeset) ->
          changeset

        term ->
          raise ArgumentError, "Expected function to return a changeset, got: #{inspect(term)}"
      end
    else
      changeset
    end
  end

  @doc """
  Returns true if the field on the changeset is an empty list in
  the data or changes.

  ### Examples

      iex> EctoShorts.CommonChanges.changeset_field_empty?(changeset, :comments)
  """
  @spec changeset_field_empty?(Changeset.t(), atom) :: boolean
  def changeset_field_empty?(changeset, key) do
    Changeset.get_field(changeset, key) === []
  end

  @doc """
  Returns true if the field on the changeset is nil in the data
  or changes.

  ### Examples

      iex> EctoShorts.CommonChanges.changeset_field_nil?(changeset, :comments)
  """
  @spec changeset_field_nil?(Changeset.t(), atom) :: boolean
  def changeset_field_nil?(changeset, key) do
    changeset |> Changeset.get_field(key) |> is_nil()
  end

  @doc """
  This function is the primary use function
  Preloads changeset assoc if change is made and then and put_or_cast's it

  ### Options

    * `required_when_missing` - Sets `:required` to true if the
      field is `nil` in both changes and data. See the
      `:required` option documentation for details.

    * `:required` - Indicates if the association is mandatory.
      For one-to-one associations, a non-nil value satisfies
      this validation. For many associations, a non-empty list
      is sufficient. See [Ecto.Changeset.cast_assoc/3](https://hexdocs.pm/ecto/Ecto.Changeset.html#cast_assoc/3)
      for more information.

  ## Example

      iex> CommonChanges.preload_change_assoc(changeset, :my_relation)
      iex> CommonChanges.preload_change_assoc(changeset, :my_relation, repo: MyApp.OtherRepo)
      iex> CommonChanges.preload_change_assoc(changeset, :my_relation, required: true)
      iex> CommonChanges.preload_change_assoc(changeset, :my_relation, required_when_missing: :my_relation_id)
  """
  @spec preload_change_assoc(Changeset.t(), atom(), keyword()) :: Changeset.t()
  def preload_change_assoc(changeset, key, opts \\ []) do
    required? =
      if opts[:required_when_missing] do
        changeset_field_nil?(changeset, opts[:required_when_missing])
      else
        opts[:required] === true
      end

    opts = Keyword.put(opts, :required, required?)

    if Map.has_key?(changeset.params, Atom.to_string(key)) do
      changeset
      |> preload_changeset_assoc(key, opts)
      |> put_or_cast_assoc(key, opts)
    else
      Changeset.cast_assoc(changeset, key, opts)
    end
  end

  @doc "Preloads a changesets association"
  @spec preload_changeset_assoc(Changeset.t(), atom) :: Changeset.t()
  @spec preload_changeset_assoc(Changeset.t(), atom, keyword()) :: Changeset.t()
  def preload_changeset_assoc(changeset, key, opts \\ [])

  def preload_changeset_assoc(changeset, key, opts) do
    if opts[:ids] do
      schema = changeset_relationship_schema(changeset, key)

      records = Actions.all(schema, %{ids: opts[:ids]}, opts)

      Map.update!(changeset, :data, &Map.put(&1, key, records))
    else
      Map.update!(changeset, :data, &Config.repo!(opts).preload(&1, key, opts))
    end
  end

  defp changeset_relationship_schema(changeset, key) do
    if Map.has_key?(changeset.types, key) and relationship_exists?(changeset.types[key]) do
      {:assoc, assoc} = Map.get(changeset.types, key)

      assoc.queryable
    else
      %parent_schema{} = changeset.data

      raise ArgumentError,
            "The key #{inspect(key)} is not an association for the queryable #{inspect(parent_schema)}."
    end
  end

  @doc """
  Determines put or cast on association with some special magic

  If you pass a many to many relation only a list of id's it will count that as a `member_update` and remove or add members to the relations list

  E.G. User many_to_many Fruit

  This would update the user to have only fruits with id 1 and 3
  ```elixir
  CommonChanges.put_or_cast_assoc(change(user, fruits: [%{id: 1}, %{id: 3}]), :fruits)
  ```
  """
  @spec put_or_cast_assoc(Changeset.t(), atom) :: Changeset.t()
  @spec put_or_cast_assoc(Changeset.t(), atom, Keyword.t()) :: Changeset.t()
  def put_or_cast_assoc(changeset, key, opts \\ []) do
    params_data = Map.get(changeset.params, Atom.to_string(key))

    find_method_and_put_or_cast(changeset, key, params_data, opts)
  end

  defp find_method_and_put_or_cast(changeset, key, nil, opts) do
    Changeset.cast_assoc(changeset, key, opts)
  end

  defp find_method_and_put_or_cast(changeset, key, params_data, opts) when is_list(params_data) do
    cond do
      SchemaHelpers.all_schema_struct?(params_data) ->
        Changeset.put_assoc(
          changeset,
          key,
          params_data,
          opts
        )

      member_update?(params_data) ->
        schema = changeset_relationship_schema(changeset, key)
        data = Actions.all(schema, ids: data_ids(params_data))
        Changeset.put_assoc(changeset, key, data, opts)

      SchemaHelpers.any_created?(params_data) ->
        ids = params_data |> data_ids() |> Enum.reject(&is_nil/1)

        changeset
        |> preload_changeset_assoc(key, Keyword.put(opts, :ids, ids))
        |> Changeset.cast_assoc(key, opts)

      true ->
        Changeset.cast_assoc(changeset, key, opts)
    end
  end

  defp find_method_and_put_or_cast(changeset, key, param_data, opts) do
    if SchemaHelpers.schema_struct?(param_data) do
      Changeset.put_assoc(changeset, key, param_data, opts)
    else
      Changeset.cast_assoc(changeset, key, opts)
    end
  end

  defp member_update?([]), do: false

  defp member_update?(schemas) do
    Enum.all?(schemas, fn
      %{id: id} = item when item === %{id: id} -> true
      _ -> false
    end)
  end

  defp data_ids(data), do: Enum.map(data, &Map.get(&1, :id))

  defp relationship_exists?({:assoc, _}), do: true
  defp relationship_exists?(_), do: false
end
