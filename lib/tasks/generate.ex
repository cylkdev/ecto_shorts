# defmodule Mix.Tasks.EctoShorts.Generate do
#   use Mix.Task

#   @shortdoc "Generates a positional binding compiler module"

#   @switches [
#     provider: :string,
#     module: :string,
#     range_start: :integer,
#     range_end: :integer,
#     dir: :string,
#     help: :boolean
#   ]

#   @aliases [
#     p: :provider,
#     m: :module,
#     s: :range_start,
#     e: :range_end,
#     d: :dir,
#     h: :help
#   ]

#   @usage """
#   Usage:
#     mix ecto_shorts.generate --provider MyApp.Specs --module MyApp.Generated

#   Options:
#     --provider, -p     Specs provider module (required)
#     --module, -m       Output module name (required)
#     --range-start, -s  Starting positional binding index (default: 1)
#     --range-end, -e    Ending positional binding index (default: compiler max)
#     --dir, -d          Output base directory
#     --help, -h         Show this message
#   """

#   @impl true
#   def run(args) do
#     {opts, positional, invalid} =
#       OptionParser.parse(args, strict: @switches, aliases: @aliases)

#     cond do
#       opts[:help] ->
#         Mix.shell().info(@usage)

#       invalid != [] ->
#         Mix.raise("Invalid options: #{format_invalid(invalid)}\n\n#{@usage}")

#       positional != [] ->
#         Mix.raise("Unexpected arguments: #{Enum.join(positional, ", ")}\n\n#{@usage}")

#       true ->
#         provider = fetch_module!(opts, :provider)
#         module_name = fetch_module!(opts, :module)
#         range_start = Keyword.get(opts, :range_start, 1)
#         range_end = Keyword.get(opts, :range_end, EctoShorts.Generator.max_positional_bindings())

#         validate_range!(range_start, range_end)

#         opts
#         |> Keyword.take([:dir])
#         |> then(fn write_opts ->
#           EctoShorts.Generator.write_positional_binding_module_file(
#             provider,
#             module_name,
#             range_start,
#             range_end,
#             write_opts
#           )
#         end)
#     end
#   end

#   defp fetch_module!(opts, key) do
#     opts
#     |> Keyword.fetch!(key)
#     |> parse_module!()
#   rescue
#     KeyError ->
#       Mix.raise("Missing required option --#{key}\n\n#{@usage}")
#   end

#   defp parse_module!(value) do
#     value
#     |> String.trim()
#     |> String.trim_leading("Elixir.")
#     |> String.split(".", trim: true)
#     |> case do
#       [] -> Mix.raise("Expected a module name, got: #{inspect(value)}")
#       parts -> Module.concat(parts)
#     end
#   end

#   defp validate_range!(range_start, _range_end) when range_start < 1 do
#     Mix.raise("--range-start must be greater than or equal to 1")
#   end

#   defp validate_range!(range_start, range_end) when range_start > range_end do
#     Mix.raise("--range-start must be less than or equal to --range-end")
#   end

#   defp validate_range!(_range_start, _range_end), do: :ok

#   defp format_invalid(invalid) do
#     Enum.map_join(invalid, ", ", fn {key, value} ->
#       "#{key}=#{inspect(value)}"
#     end)
#   end
# end
