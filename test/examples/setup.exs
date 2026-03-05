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

defmodule EctoShorts.TestPost do
  use Ecto.Schema
  import Ecto.Changeset

  schema "posts" do
    field :title, :string
    field :body, :string
    field :views, :integer
    field :published, :boolean
    field :published_at, :utc_datetime
    field :tags, {:array, :string}
    belongs_to :author, EctoShorts.TestUser
    has_many :comments, EctoShorts.TestComment
    has_many :participants, through: [:comments, :user]
    timestamps()
  end
end

defmodule EctoShorts.TestUser do
  use Ecto.Schema
  import Ecto.Changeset

  schema "users" do
    field :first_name, :string
    has_many :posts, EctoShorts.TestPost
    has_many :comments, EctoShorts.TestComment
  end
end

defmodule EctoShorts.TestComment do
  use Ecto.Schema
  import Ecto.Changeset

  schema "comments" do
    belongs_to :author, EctoShorts.TestUser
    belongs_to :post, EctoShorts.TestPost

    field :body, :string
    field :published, :boolean
    field :published_at, :naive_datetime
    field :replies, :integer
    field :tags, {:array, :string}

    timestamps()
  end
end
