defmodule EctoShorts.Schema.PostNoPrimaryKey do
  use Ecto.Schema

  @primary_key false

  schema "posts" do
    field :title, :string
  end
end
