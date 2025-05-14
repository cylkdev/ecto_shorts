defmodule EctoShorts.Schema.Post do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  require Ecto.Query

  schema "posts" do
    belongs_to :user, EctoShorts.Schema.User

    field :title, :string
    field :body, :string
    field :published, :boolean
    field :tags, {:array, :string}
    field :views, :integer
    field :permalink, :string

    field :notes, :string, source: :custom_string_field

    has_many :comments, EctoShorts.Schema.Comment

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

  def create_changeset(attrs \\ %{}) do
    changeset(%__MODULE__{}, attrs)
  end

  # This callback function is invoked by `EctoShorts.CommonFilters.convert_params_to_filter`
  # when `:search` is specified in parameters.
  def by_search(query, attrs) do
    filters = Map.to_list(attrs)

    Ecto.Query.where(query, ^filters)
  end
end
