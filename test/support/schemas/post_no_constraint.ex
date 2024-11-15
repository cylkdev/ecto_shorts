defmodule EctoShorts.Schemas.PostNoConstraint do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "posts" do
    field(:title, :string)
    field(:unique_identifier, :string)
    field(:likes, :integer)

    has_many(:comments, EctoShorts.Schemas.Comment, foreign_key: :post_id)

    has_many(:authors, through: [:comments, :user])

    belongs_to(:user, EctoShorts.Schemas.User)

    many_to_many(:users, EctoShorts.Schemas.User, join_through: EctoShorts.Schemas.UserPost)

    timestamps()
  end

  @available_fields [
    :likes,
    :title,
    :unique_identifier,
    :user_id
  ]

  def changeset(model_or_changeset, attrs \\ %{}) do
    cast(model_or_changeset, attrs, @available_fields)
  end

  def create_changeset(attrs \\ %{}) do
    changeset(%__MODULE__{}, attrs)
  end
end
