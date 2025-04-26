defmodule EctoShorts.Repo.Migrations.CreatePosts do
  use Ecto.Migration

  def change do
    create table(:posts) do
      add :title, :string
      add :unique_identifier, :string
      add :views, :integer
      add :tags, {:array, :string}

      add :custom_string_field, :string

      add :user_id, references(:users,
        on_delete: :nilify_all,
        on_update: :update_all
      )

      timestamps()
    end

    create unique_index(:posts, :unique_identifier)
  end
end
