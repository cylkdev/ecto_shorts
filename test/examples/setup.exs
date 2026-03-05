defmodule TestRepo do
  @moduledoc false
  use Ecto.Repo,
    otp_app: :ecto_shorts,
    adapter: Ecto.Adapters.Postgres
end

# Ensure Repo configuration is available
Application.put_env(:ecto_shorts, :sql_sandbox, true)
Application.put_env(:ecto_shorts, :ecto_repos, [TestRepo])
Application.put_env(:ecto_shorts, TestRepo,
  username: "postgres",
  database: "ecto_shorts_test",
  hostname: "localhost",
  show_sensitive_data_on_connection_error: true,
  log: :debug,
  stacktrace: true,
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 20
)

defmodule Post do
  use Ecto.Schema

  schema "posts" do
    field :title, :string
    field :body, :string
    field :views, :integer
    field :published, :boolean
    field :published_at, :utc_datetime
    field :tags, {:array, :string}
    belongs_to :author, User
    timestamps()
  end
end

defmodule User do
  use Ecto.Schema

  schema "users" do
    field :first_name, :string
    has_many :posts, Post
  end
end
