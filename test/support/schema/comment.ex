defmodule EctoShorts.Schema.Comment do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "comments" do
    field(:body, :string)
    field(:replies, :integer)

    belongs_to(:post, EctoShorts.Schema.Post)

    belongs_to(:user, EctoShorts.Schema.User)

    timestamps()
  end

  @available_fields [
    :body,
    :replies,
    :post_id,
    :user_id
  ]

  def changeset(model_or_changeset, attrs \\ %{}) do
    model_or_changeset
    |> cast(attrs, @available_fields)
    |> validate_length(:body, min: 3)
  end
end
