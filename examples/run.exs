# Run the migration first
IO.puts("Running migration...")

ExUnit.start(autorun: false)

# Load and run tests
Code.require_file("setup.exs", __DIR__)

# Load and run tests
Code.require_file("ecto_query_dsl.exs", __DIR__)

# Start the Repo first with a name so it can be registered
{:ok, _} = TestRepo.start_link()

# Run migrations
migration_path = Path.join(__DIR__, "migrations")

case Ecto.Migrator.run(TestRepo, migration_path, :up, all: true) do
  migrations when is_list(migrations) ->
    if Enum.empty?(migrations) do
      IO.puts("No migrations to run (already up to date)")
    else
      IO.puts("Successfully ran #{length(migrations)} migration(s)")
    end

  error ->
    IO.puts("Migration failed: #{inspect(error)}")
    System.halt(1)
end

IO.puts("\nRunning ecto_query_dsl.exs...")

# Start ExUnit and configure Sandbox AFTER Repo is started
ExUnit.run()
Ecto.Adapters.SQL.Sandbox.mode(TestRepo, :manual)

IO.puts("\nDone!")
