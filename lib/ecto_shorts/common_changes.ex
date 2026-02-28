defmodule EctoShorts.CommonChanges do
  @moduledoc """
  Helper functions for building `changeset/2` functions in Ecto schemas.

  Provides utilities for preloading and casting associations, applying
  conditional changes, validating that fields are not unset, and coercing
  field values. All functions accept an `Ecto.Changeset` and return an
  `Ecto.Changeset`.

  ## Preloading associations on change

  Use `preload_change_assoc/3` when you need to change an association via
  `cast_assoc/3` or `put_assoc/3` but the association is not yet preloaded:

      defmodule MyApp.Accounts.User do
        def changeset(changeset, params) do
          changeset
          |> cast([:name, :email])
          |> validate_required([:name, :email])
          |> EctoShorts.CommonChanges.preload_change_assoc(:address)
        end
      end

  ## Validating a relation is present

  Require that either an association or its foreign key is provided:

      |> EctoShorts.CommonChanges.preload_change_assoc(:address,
        required_when_missing: :address_id
      )

  ## Conditional changes

  Run a change function only when a condition is met:

      |> EctoShorts.CommonChanges.apply_when(
        &EctoShorts.CommonChanges.changeset_field_nil?(&1, :email),
        &Ecto.Changeset.put_change(&1, :email, "default@example.com")
      )

  See also `EctoShorts.CommonSchema` and `EctoShorts.Actions`.
  """

  @moduledoc groups: [
               %{title: "Changeset inspection", description: "Read or check changeset field state."},
               %{title: "Changeset mutation", description: "Apply conditional or coercive changes."},
               %{title: "Association management", description: "Preload and cast associations."}
             ]

  alias Ecto.Changeset
  alias EctoShorts.{Actions, Config, SchemaHelpers}

  require Logger

  @doc since: "3.0.0"
  @doc group: "Changeset inspection"
  @doc """
  Returns `true` if the given field (or all fields in a list) have no pending change.

  Checks `Ecto.Changeset.get_change/2` for each field. Returns `true` when
  the change is `nil` (i.e. no change was cast). When `fields` is a list,
  returns `true` only if **all** fields have a `nil` change.

  ## Examples

      iex> changeset = Ecto.Changeset.change(%EctoShorts.Schema.Post{})
      ...> EctoShorts.CommonChanges.has_nil_change?(changeset, :title)
      true

      iex> changeset = Ecto.Changeset.change(%EctoShorts.Schema.Post{}, title: "Hello")
      ...> EctoShorts.CommonChanges.has_nil_change?(changeset, :title)
      false

  See also `has_empty_change?/2` and `changeset_field_nil?/2`.
  """
  def has_nil_change?(changeset, fields) when is_list(fields) do
    Enum.all?(fields, &has_nil_change?(changeset, &1))
  end

  def has_nil_change?(changeset, field) do
    changeset
    |> Changeset.get_change(field)
    |> is_nil()
  end

  @doc since: "3.0.0"
  @doc group: "Changeset inspection"
  @doc """
  Returns `true` if the given field (or all fields in a list) have an empty change.

  A change is considered empty when it is `[]` or `%{}`. Fields with `nil`
  changes or non-empty values return `false`. When `fields` is a list,
  returns `true` only if **all** fields have an empty change.

  ## Examples

      iex> changeset = Ecto.Changeset.change(%EctoShorts.Schema.Post{}, comments: [])
      ...> EctoShorts.CommonChanges.has_empty_change?(changeset, :comments)
      true

      iex> changeset = Ecto.Changeset.change(%EctoShorts.Schema.Post{})
      ...> EctoShorts.CommonChanges.has_empty_change?(changeset, :comments)
      false

  See also `has_nil_change?/2` and `changeset_field_empty?/2`.
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

  @doc since: "3.0.0"
  @doc group: "Changeset mutation"
  @doc """
  Prevents a field (or list of fields) from being set to `nil` when the field
  already has a persisted value.

  Adds a `"can't be blank"` error to the changeset when the field is
  being changed to `nil` from a non-nil persisted value. Does nothing when
  the field is already `nil` or has not changed.

  ## Examples

      iex> cs = Ecto.Changeset.cast(%EctoShorts.Schema.Post{title: "Old"}, %{title: nil}, [:title])
      ...> cs = EctoShorts.CommonChanges.validate_not_unset(cs, :title)
      ...> cs.errors[:title]
      {"can't be blank", []}

  See also `put_new_change/3` and `changeset_field_nil?/2`.
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

  @doc since: "3.0.0"
  @doc group: "Changeset mutation"
  @doc """
  Truncates datetime changes on the given field(s) to the specified precision.

  Accepts `:second`, `:millisecond`, or `:microsecond` as `precision`.
  Works on both `DateTime` and `NaiveDateTime` values. Non-datetime values
  are passed through unchanged.

  ## Examples

      iex> dt = ~U[2024-01-01 12:00:00.123456Z]
      ...> cs = Ecto.Changeset.change(%EctoShorts.Schema.Post{}, inserted_at: dt)
      ...> cs = EctoShorts.CommonChanges.truncate_datetime_change(cs, :inserted_at, :second)
      ...> Ecto.Changeset.get_change(cs, :inserted_at).microsecond
      {0, 6}

  See also `trim_string_change/2`.
  """
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

  @doc since: "3.0.0"
  @doc group: "Changeset mutation"
  @doc """
  Trims leading and trailing whitespace from string changes on the given field(s).

  Accepts a single field atom or a list. Non-string change values are passed
  through unchanged.

  ## Examples

      iex> cs = Ecto.Changeset.change(%EctoShorts.Schema.Post{}, title: "  Hello  ")
      ...> cs = EctoShorts.CommonChanges.trim_string_change(cs, :title)
      ...> Ecto.Changeset.get_change(cs, :title)
      "Hello"

  See also `truncate_datetime_change/3` and `put_new_change/3`.
  """
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

  @doc since: "3.0.0"
  @doc group: "Changeset mutation"
  @doc """
  Puts a change only if the field has no pending change.

  `value` can be a literal value, a 0-arity function (called to produce the
  value), or a 1-arity function that receives the field name.

  ## Examples

      iex> cs = Ecto.Changeset.change(%EctoShorts.Schema.Post{})
      ...> cs = EctoShorts.CommonChanges.put_new_change(cs, :title, "Default")
      ...> Ecto.Changeset.get_change(cs, :title)
      "Default"

      iex> cs = Ecto.Changeset.change(%EctoShorts.Schema.Post{}, title: "Existing")
      ...> cs = EctoShorts.CommonChanges.put_new_change(cs, :title, "Default")
      ...> Ecto.Changeset.get_change(cs, :title)
      "Existing"

  See also `put_new_value/3` and `apply_when/3`.
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

  @doc since: "3.0.0"
  @doc group: "Changeset mutation"
  @doc """
  Puts a change only if the field's current value (data or changes) is `nil`.

  Unlike `put_new_change/3` which only checks pending changes, this function
  also considers the persisted value in `changeset.data`. `value` can be a
  literal value, a 0-arity function, or a 1-arity function that receives the
  field name.

  ## Examples

      iex> cs = Ecto.Changeset.change(%EctoShorts.Schema.Post{title: nil})
      ...> cs = EctoShorts.CommonChanges.put_new_value(cs, :title, "Default")
      ...> Ecto.Changeset.get_change(cs, :title)
      "Default"

  See also `put_new_change/3` and `apply_when/3`.
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

  @doc since: "3.0.0"
  @doc group: "Changeset mutation"
  @doc """
  Applies `change_func` to the changeset only when `when_func` returns `true`.

  `when_func` is a 1-arity function that receives the changeset and must
  return a boolean. `change_func` is a 1-arity function that receives the
  changeset and must return a changeset.

  Raises `ArgumentError` when `change_func` returns a non-changeset value.

  ## Examples

      iex> cs = Ecto.Changeset.change(%EctoShorts.Schema.Post{title: nil})
      ...> EctoShorts.CommonChanges.apply_when(
      ...>   cs,
      ...>   &EctoShorts.CommonChanges.changeset_field_nil?(&1, :title),
      ...>   &Ecto.Changeset.put_change(&1, :title, "Fallback")
      ...> )
      ...> |> Ecto.Changeset.get_change(:title)
      "Fallback"

  See also `put_new_change/3` and `put_new_value/3`.
  """
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

  @doc group: "Changeset inspection"
  @doc """
  Returns `true` if the field on the changeset is an empty list in the data or changes.

  Uses `Ecto.Changeset.get_field/2` which reads the current value from
  changes first, then falls back to data.

  ## Examples

      iex> cs = Ecto.Changeset.change(%EctoShorts.Schema.Post{comments: []})
      ...> EctoShorts.CommonChanges.changeset_field_empty?(cs, :comments)
      true

  See also `changeset_field_nil?/2` and `has_empty_change?/2`.
  """
  @spec changeset_field_empty?(Changeset.t(), atom) :: boolean
  def changeset_field_empty?(changeset, key) do
    Changeset.get_field(changeset, key) === []
  end

  @doc group: "Changeset inspection"
  @doc """
  Returns `true` if the field on the changeset is `nil` in the data or changes.

  Uses `Ecto.Changeset.get_field/2` which reads from changes first, then
  falls back to data.

  ## Examples

      iex> cs = Ecto.Changeset.change(%EctoShorts.Schema.Post{title: nil})
      ...> EctoShorts.CommonChanges.changeset_field_nil?(cs, :title)
      true

  See also `changeset_field_empty?/2` and `has_nil_change?/2`.
  """
  @spec changeset_field_nil?(Changeset.t(), atom) :: boolean
  def changeset_field_nil?(changeset, key) do
    changeset |> Changeset.get_field(key) |> is_nil()
  end

  @doc group: "Association management"
  @doc """
  Preloads a changeset association if needed, then puts or casts it.

  This is the primary helper for managing associations in `changeset/2`
  functions. When the association key is present in `changeset.params`,
  it preloads the existing data so `cast_assoc/3` can diff correctly.
  When absent, falls back to `Ecto.Changeset.cast_assoc/3`.

  ## Options

  * `:required_when_missing` - sets `:required` to `true` when the given
    field is `nil` in both changes and data. Use this to require that either
    an association or its foreign key is provided.
  * `:required` - when `true`, validates that the association is present.
    For one-to-one associations a non-nil value suffices; for many associations
    a non-empty list is required. See `Ecto.Changeset.cast_assoc/3` for details.
    Defaults to `false`.
  * `:repo` - the `Ecto.Repo` to use for the preload query. Defaults to
    `EctoShorts.Config.repo/0`.

  ## Examples

      iex> EctoShorts.CommonChanges.preload_change_assoc(changeset, :comments)
      iex> EctoShorts.CommonChanges.preload_change_assoc(changeset, :comments, required: true)
      iex> EctoShorts.CommonChanges.preload_change_assoc(changeset, :comments,
      ...>   required_when_missing: :author_id
      ...> )

  See also `preload_changeset_assoc/3` and `put_or_cast_assoc/3`.
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

  @doc group: "Association management"
  @doc """
  Preloads the given association on the changeset's data struct.

  When `:ids` is provided in `opts`, queries for records with those IDs and
  replaces the association list directly. Otherwise, calls
  `c:Ecto.Repo.preload/3` on the data struct.

  Returns the changeset with the association preloaded in `changeset.data`.

  See also `preload_change_assoc/3` and `put_or_cast_assoc/3`.
  """
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

  @doc group: "Association management"
  @doc """
  Chooses between `put_assoc` and `cast_assoc` based on the params value.

  Inspects the raw value in `changeset.params` for the given `key` and
  selects the appropriate strategy:

  * When params contains a list of schema structs — `put_assoc`.
  * When params contains a list of `%{id: id}` maps only (member update) —
    loads the matching records and calls `put_assoc`.
  * When params contains a list with some persisted IDs — preloads existing
    records and calls `cast_assoc`.
  * Otherwise — `cast_assoc`.

  ## Examples

      # Member update: replace all user fruits with ids 1 and 3
      EctoShorts.CommonChanges.put_or_cast_assoc(
        Ecto.Changeset.change(user, fruits: [%{id: 1}, %{id: 3}]),
        :fruits
      )

  See also `preload_change_assoc/3` and `preload_changeset_assoc/3`.
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
