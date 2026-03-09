defmodule EctoShorts.Repo.Migrations.CreateUsersAndPosts do
  use Ecto.Migration

  def change do
    create table(:users) do
      add :first_name, :string

      timestamps()
    end

    create table(:posts) do
      add :title, :string
      add :body, :string
      add :views, :integer
      add :published, :boolean
      add :published_at, :utc_datetime
      add :tags, {:array, :string}
      add :author_id, references(:users, on_delete: :nothing)

      timestamps()
    end

    create index(:posts, [:author_id])
  end
end
