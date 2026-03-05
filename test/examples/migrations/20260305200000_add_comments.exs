defmodule EctoShorts.Repo.Migrations.AddComments do
  use Ecto.Migration

  def change do
    create table(:comments) do
      add :body, :string
      add :published, :boolean
      add :published_at, :naive_datetime
      add :replies, :integer
      add :tags, {:array, :string}
      add :post_id, references(:posts, on_delete: :nothing)
      add :author_id, references(:users, on_delete: :nothing)

      timestamps()
    end

    create index(:comments, [:post_id])
    create index(:comments, [:author_id])
  end
end
