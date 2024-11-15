defmodule EctoShorts.Schemas.PostTimestampCustomFieldName do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "posts" do
    field(:title, :string)

    timestamps(
      type: :utc_datetime,
      inserted_at: :created_at,
      updated_at: :modified_at
    )
  end

  @available_fields [:title]

  def changeset(model_or_changeset, attrs \\ %{}) do
    cast(model_or_changeset, attrs, @available_fields)
  end

  def create_changeset(attrs \\ %{}) do
    changeset(%__MODULE__{}, attrs)
  end
end
