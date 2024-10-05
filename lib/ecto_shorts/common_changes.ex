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
  alias EctoShorts.{Actions, Config}

  @type field :: atom()
  @type opts :: keyword()
  @type changeset :: Ecto.Changeset.t()
  @type changesets :: list(changeset())

  @doc "Run's changeset function if when function returns true"
  @spec put_when(
    changeset :: changeset(),
    when_func :: ((changeset()) -> boolean()),
    change_func :: ((changeset()) -> changeset())
  ) :: changeset()
  def put_when(changeset, when_func, change_func) do
    if when_func.(changeset) do
      change_func.(changeset)
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
  @spec changeset_field_empty?(
    changeset :: changeset(),
    field :: field()
  ) :: boolean()
  def changeset_field_empty?(changeset, field) do
    Changeset.get_field(changeset, field) === []
  end

  @doc """
  Returns true if the field on the changeset is nil in the data
  or changes.

  ### Examples

      iex> EctoShorts.CommonChanges.changeset_field_nil?(changeset, :comments)
  """
  @spec changeset_field_nil?(
    changeset :: changeset(),
    field :: field()
  ) :: boolean()
  def changeset_field_nil?(changeset, field) do
    changeset |> Changeset.get_field(field) |> is_nil()
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

  ### Example

      iex> CommonChanges.preload_change_assoc(changeset, :my_relation)
      iex> CommonChanges.preload_change_assoc(changeset, :my_relation, repo: MyApp.OtherRepo)
      iex> CommonChanges.preload_change_assoc(changeset, :my_relation, required: true)
      iex> CommonChanges.preload_change_assoc(changeset, :my_relation, required_when_missing: :my_relation_id)
  """
  @spec preload_change_assoc(
    changeset :: changeset(),
    field :: field(),
    opts :: opts()
  ) :: Changeset.t
  @spec preload_change_assoc(
    changeset :: changeset(),
    field :: field()
  ) :: Changeset.t
  def preload_change_assoc(changeset, field, opts \\ []) do
    if Map.has_key?(changeset.params, Atom.to_string(field)) do
      changeset
      |> preload_changeset_assoc(field, opts)
      |> put_or_cast_assoc(field, opts)
    else
      put_or_cast_assoc(changeset, field, opts)
    end
  end

  @doc """
  Preloads an association if it is not loaded.
  """
  @spec preload_changeset_assoc(
    changeset :: changeset(),
    field :: field(),
    opts :: opts()
  ) :: changeset()
  @spec preload_changeset_assoc(
    changeset :: changeset(),
    field :: field()
  ) :: changeset()
  def preload_changeset_assoc(changeset, field, opts \\ []) do
    Map.update!(changeset, :data, fn schema_data ->
      case Map.get(schema_data, field) do
        %Ecto.Association.NotLoaded{} -> Config.repo!(opts).preload(schema_data, field, opts)
        _ -> schema_data
      end
    end)
  end

  @doc """
  Determines put or cast on association with some special magic

  If you pass a many to many relation only a list of id's it will count that as a `member_update` and remove or add members to the relations list

  E.G. User many_to_many Fruit

  This would update the user to have only fruits with id 1 and 3
  ```elixir
  CommonChanges.put_or_cast_assoc(change(user, fruits: [%{id: 1}, %{id: 3}]), :fruits)
  ```

  This function raises if the association is a read-only `:through` association.
  See the [documentation](https://hexdocs.pm/ecto/Ecto.Schema.html#has_many/3-has_many-has_one-through) for more information.
  """
  @spec put_or_cast_assoc(
    changeset :: changeset(),
    field :: field(),
    opts :: opts()
  ) :: changeset()
  @spec put_or_cast_assoc(
    changeset :: changeset(),
    field :: field()
  ) :: changeset()
  def put_or_cast_assoc(changeset, field, opts \\ []) do
    required? =
      case opts[:required_when_missing] do
        nil -> opts[:required] === true
        field -> changeset_field_nil?(changeset, field)
      end

    opts = Keyword.put(opts, :required, required?)

    ecto_assoc = fetch_ecto_write_assoc!(changeset.data.__struct__, field)

    field_params = Map.get(changeset.params, Atom.to_string(field))

    changeset_put_or_cast_assoc(changeset, field, field_params, ecto_assoc, opts)
  end

  defp changeset_put_or_cast_assoc(changeset, field, field_params, %{cardinality: :many} = ecto_assoc, opts) do
    cond do
      is_nil(field_params) ->
        Changeset.cast_assoc(changeset, field, opts)

      Enum.all?(field_params, &ecto_schema?/1) ->
        Changeset.put_assoc(changeset, field, field_params, opts)

      Enum.all?(field_params, &member_update?/1) ->
        ids = params_ids(field_params)

        values = Actions.all(ecto_assoc.queryable, %{id: ids}, opts)

        Changeset.put_assoc(changeset, field, values, opts)

      Enum.any?(field_params, &has_id?/1) ->
        ids = params_ids(field_params)

        values = Actions.all(ecto_assoc.queryable, %{id: ids}, opts)

        changeset
        |> Map.update!(:data, &Map.put(&1, field, values))
        |> Changeset.cast_assoc(field, opts)

      true ->
        Changeset.cast_assoc(changeset, field, opts)

    end
  end

  defp changeset_put_or_cast_assoc(changeset, field, field_params, _ecto_assoc, opts) do
    cond do
      is_nil(field_params) ->
        Changeset.cast_assoc(changeset, field, opts)

      ecto_schema?(field_params) ->
        Changeset.put_assoc(changeset, field, field_params, opts)

      member_update?(field_params) ->
        changeset
        |> preload_changeset_assoc(field, opts)
        |> Changeset.put_assoc(field, field_params, opts)

      has_id?(field_params) ->
        changeset
        |> preload_changeset_assoc(field, opts)
        |> Changeset.cast_assoc(field, opts)

      true ->
        Changeset.cast_assoc(changeset, field, opts)

    end
  end

  defp fetch_ecto_write_assoc!(schema, field) do
    case fetch_ecto_assoc!(schema, field) do
      %Ecto.Association.HasThrough{} = ecto_assoc ->
        raise ArgumentError, """
        The field '#{inspect(field)}' is a read-only association for the schema
        '#{inspect(schema)}' and cannot be used with cast_assoc or put_assoc.

        For more information see the ecto documentation:
        https://hexdocs.pm/ecto/Ecto.Schema.html#has_many/3-has_many-has_one-through

        got:

        #{inspect(ecto_assoc, pretty: true)}
        """

      ecto_assoc -> ecto_assoc
    end
  end

  defp fetch_ecto_assoc!(schema, field) do
    with nil <- schema.__schema__(:association, field) do
      raise ArgumentError, "The field #{inspect(field)} is not an association of the schema #{inspect(schema)}."
    end
  end

  defp params_ids(params_list) when is_list(params_list) do
    params_list
    |> Enum.reduce([], fn
      %{id: id}, acc -> [id | acc]
      %{"id" => id}, acc -> [id | acc]
      _, acc -> acc
    end)
    |> Enum.reverse()
  end

  defp has_id?(%{id: _}), do: true
  defp has_id?(%{"id" => _}), do: true
  defp has_id?(_), do: false

  defp member_update?(%{id: id} = params) when params === %{id: id}, do: true
  defp member_update?(_), do: false

  defp ecto_schema?(%{__meta__: %{schema: _}}), do: true
  defp ecto_schema?(_), do: false
end
