# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
import Config

config :ecto_shorts,
  mix_env: Mix.env(),
  repo: nil,
  replica: nil,
  error_module: EctoShorts.Actions.Error,
  join_source_module: nil,
  hints: [],
  compiler: [max_positional_bindings: 10]

if Mix.env() === :test do
  config :ecto_shorts, ecto_repos: [EctoShorts.Repo]
  config :ecto_shorts, repo: EctoShorts.Repo
  config :ecto_shorts, :sql_sandbox, true
  config :ecto_shorts, :hints, test_index: ["USE INDEX(test_index)"]

  config :ecto_shorts, EctoShorts.Repo,
    username: "postgres",
    database: "ecto_shorts_test",
    hostname: "localhost",
    show_sensitive_data_on_connection_error: true,
    log: :debug,
    stacktrace: true,
    pool: Ecto.Adapters.SQL.Sandbox,
    pool_size: 10
end
