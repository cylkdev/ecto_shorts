defmodule EctoShorts.Schemas.User do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "users" do
    field(:email, :string)

    has_many(:comments, EctoShorts.Schemas.Comment)

    many_to_many(:posts, EctoShorts.Schemas.Post, join_through: EctoShorts.Schemas.UserPost)

    timestamps()
  end

  @available_fields [:email]

  def changeset(model_or_changeset, attrs \\ %{}) do
    cast(model_or_changeset, attrs, @available_fields)
  end
end
