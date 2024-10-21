defmodule EctoShorts.Support.Schemas.PostNoConstraint do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  require Ecto.Query

  schema "posts" do
    field :title, :string
    field :unique_identifier, :string
    field :likes, :integer

    has_many :comments, EctoShorts.Support.Schemas.Comment, foreign_key: :post_id

    has_many :authors, through: [:comments, :user]

    belongs_to :user, EctoShorts.Support.Schemas.User

    many_to_many :users, EctoShorts.Support.Schemas.User,
      join_through: EctoShorts.Support.Schemas.UserPost

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
