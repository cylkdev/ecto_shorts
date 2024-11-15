defmodule EctoShorts.Schemas.PostAbstract do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset
  import Ecto.Query

  @timestamps_opts [type: :naive_datetime]

  schema "abstract table: posts" do
    field(:title, :string)
    field(:unique_identifier, :string)
    field(:likes, :integer)
    field(:tags, {:array, :string})
    field(:views, :integer)

    has_many(:comments, EctoShorts.Schemas.Comment, foreign_key: :post_id)

    has_many(:authors, through: [:comments, :user])

    belongs_to(:user, EctoShorts.Schemas.User)

    many_to_many(:users, EctoShorts.Schemas.User, join_through: EctoShorts.Schemas.UserPost)

    timestamps()
  end

  @available_fields [
    :likes,
    :title,
    :views,
    :tags,
    :unique_identifier,
    :user_id
  ]

  def changeset(model_or_changeset, attrs \\ %{}) do
    model_or_changeset
    |> cast(attrs, @available_fields)
    |> no_assoc_constraint(:comments)
    |> unique_constraint(:unique_identifier)
    |> validate_length(:title, min: 10)
  end

  def create_changeset(attrs \\ %{}) do
    changeset(%__MODULE__{}, attrs)
  end

  # This callback function is invoked by `EctoShorts.CommonFilters.convert_params_to_filter`
  # when `:search` is specified in parameters.
  def by_search(query, attrs) do
    filters = Map.to_list(attrs)

    where(query, ^filters)
  end
end
