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

  alias EctoShorts.{
    Actions,
    Config,
    SchemaHelpers
  }

  @type changeset :: Ecto.Changeset.t()
  @type key :: atom()

  @filename_web_safe_regex ~r|^[ A-Za-z0-9\-\_\.\(\)]+$|u

  @doc since: "2.5.0"
  @doc """
  ...
  """
  def validate_filename(changeset, key \\ :filename, opts \\ []) do
    changeset
    |> Changeset.validate_change(key, fn
      ^key, nil ->
        []

      ^key, change ->
        message =
          "Can only contain characters alphanumeric characters " <>
            "(A-Z, a-z, 0-9) or special characters space, " <>
            "hyphen (-), underscore(_), and period (.)"

        if Regex.match?(@filename_web_safe_regex, change), do: [], else: [{key, message}]
    end)
    |> Changeset.validate_length(key, min: opts[:min] || 1, max: opts[:max] || 255)
  end

  @doc since: "2.5.0"
  @doc """
  ...
  """
  def truncate_naive_datetime_change(changeset, key, precision \\ :second) do
    Changeset.update_change(changeset, key, fn
      datetime when is_struct(datetime, NaiveDateTime) ->
        NaiveDateTime.truncate(datetime, precision)

      term ->
        term
    end)
  end

  @doc since: "2.5.0"
  @doc """
  ...
  """
  def truncate_datetime_change(changeset, key, precision \\ :second) do
    Changeset.update_change(changeset, key, fn
      datetime when is_struct(datetime, DateTime) ->
        DateTime.truncate(datetime, precision)

      term ->
        term
    end)
  end

  @doc since: "2.5.0"
  @doc """
  ...
  """
  def put_new_change(changeset, key, term) do
    case Changeset.get_change(changeset, key) do
      nil -> Changeset.put_change(changeset, key, (is_function(term) && term.()) || term)
      _ -> changeset
    end
  end

  @doc since: "2.5.0"
  @doc """
  ...
  """
  def put_when_changed(changeset, when_key, when_fun \\ nil, update_key, update) do
    change = Changeset.get_change(changeset, when_key)

    change_present? =
      with true <- not is_nil(change) do
        (is_function(when_fun) && when_fun.(change)) || true
      end

    if change_present? do
      if is_function(update) do
        Changeset.put_change(changeset, update_key, update.())
      else
        Changeset.put_change(changeset, update_key, update)
      end
    else
      changeset
    end
  end

  @doc since: "2.5.0"
  @doc """
  ...
  """
  def put_new_when_changed(changeset, when_key, when_fun \\ nil, update_key, update) do
    change = Changeset.get_change(changeset, when_key)

    change_present? =
      with true <- not is_nil(change) do
        (is_function(when_fun) && when_fun.(change)) || true
      end

    if change_present? do
      if is_function(update) do
        put_new_change(changeset, update_key, update.())
      else
        put_new_change(changeset, update_key, update)
      end
    else
      changeset
    end
  end

  @doc "Run's changeset function if when function returns true"
  @spec put_when(
          changeset :: changeset(),
          when_func :: (changeset() -> boolean()),
          change_func :: (changeset() -> changeset())
        ) :: changeset()
  def put_when(changeset, when_func, change_func) do
    if when_func.(changeset) do
      change_func.(changeset)
    else
      changeset
    end
  end

  @doc since: "2.5.0"
  @doc """
  ...
  """
  @spec changeset_change_empty?(Changeset.t(), atom) :: boolean
  def changeset_change_empty?(changeset, key) do
    case Changeset.get_change(changeset, key) do
      change when is_list(change) -> change === []
      change when is_map(change) -> change === %{}
      _ -> false
    end
  end

  @doc since: "2.5.0"
  @doc """
  ...
  """
  @spec changeset_change_nil?(Changeset.t(), atom) :: boolean
  def changeset_change_nil?(changeset, key) do
    changeset
    |> Changeset.get_change(key)
    |> is_nil()
  end

  @doc """
  Returns true if the field on the changeset is an empty list in
  the data or changes.

  ### Examples

      iex> EctoShorts.CommonChanges.changeset_field_empty?(changeset, :comments)
  """
  @spec changeset_field_empty?(changeset :: changeset(), key :: key()) :: boolean
  def changeset_field_empty?(changeset, key) do
    case Changeset.get_field(changeset, key) do
      change when is_list(change) -> change === []
      change when is_map(change) -> change === %{}
      _ -> false
    end
  end

  @doc """
  Returns true if the field on the changeset is nil in the data
  or changes.

  ### Examples

      iex> EctoShorts.CommonChanges.changeset_field_nil?(changeset, :comments)
  """
  @spec changeset_field_nil?(Changeset.t(), atom) :: boolean
  def changeset_field_nil?(changeset, key) do
    changeset
    |> Changeset.get_field(key)
    |> is_nil()
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
  def preload_change_assoc(changeset, key, opts) do
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

  @spec preload_change_assoc(Changeset.t(), atom()) :: Changeset.t()
  def preload_change_assoc(changeset, key) do
    if Map.has_key?(changeset.params, Atom.to_string(key)) do
      changeset
      |> preload_changeset_assoc(key)
      |> put_or_cast_assoc(key)
    else
      Changeset.cast_assoc(changeset, key)
    end
  end

  @doc "Preloads a changesets association"
  @spec preload_changeset_assoc(Changeset.t(), atom) :: Changeset.t()
  @spec preload_changeset_assoc(Changeset.t(), atom, keyword()) :: Changeset.t()
  def preload_changeset_assoc(changeset, key, opts \\ [])

  def preload_changeset_assoc(changeset, key, opts) do
    if opts[:ids] do
      schema = changeset_relationship_schema(changeset, key)

      preloaded_data = Actions.all(schema, %{ids: opts[:ids]}, opts)

      Map.update!(changeset, :data, &Map.put(&1, key, preloaded_data))
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
      SchemaHelpers.all_schemas?(params_data) ->
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
        changeset
        |> preload_changeset_assoc(
          key,
          Keyword.put(opts, :ids, params_data |> data_ids() |> Enum.reject(&is_nil/1))
        )
        |> Changeset.cast_assoc(key, opts)

      true ->
        Changeset.cast_assoc(changeset, key, opts)
    end
  end

  defp find_method_and_put_or_cast(changeset, key, param_data, opts) do
    if SchemaHelpers.schema?(param_data) do
      Changeset.put_assoc(changeset, key, param_data, opts)
    else
      Changeset.cast_assoc(changeset, key, opts)
    end
  end

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
