defmodule EctoShorts.Support.Repo2 do
  @moduledoc false
  use Ecto.Repo,
    otp_app: :ecto_shorts,
    adapter: Ecto.Adapters.Postgres

  @spec start_test_repo :: :ignore | {:error, any()} | {:ok, pid()}
  def start_test_repo do
    start_link(
      username: "postgres",
      database: "ecto_shorts_test",
      hostname: "localhost",
      show_sensitive_data_on_connection_error: true,
      log: :debug,
      stacktrace: true,
      pool: Ecto.Adapters.SQL.Sandbox,
      pool_size: 5
    )
  end
end
