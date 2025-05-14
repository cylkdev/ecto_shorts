defmodule EctoShorts.Schema.AbstractPost do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  require Ecto.Query

  schema "abstract table: posts" do
    belongs_to :user, EctoShorts.Schema.User

    field :title, :string
    field :body, :string
    field :published, :boolean
    field :tags, {:array, :string}
    field :views, :integer
    field :permalink, :string

    field :notes, :string, source: :custom_string_field

    has_many :comments, EctoShorts.Schema.Comment, foreign_key: :post_id

    timestamps()
  end

  @available_fields [
    :title,
    :body,
    :notes,
    :permalink,
    :tags,
    :user_id,
    :views
  ]

  def changeset(model_or_changeset, attrs \\ %{}) do
    model_or_changeset
    |> cast(attrs, @available_fields)
    |> no_assoc_constraint(:comments)
    |> unique_constraint(:permalink)
  end
end
