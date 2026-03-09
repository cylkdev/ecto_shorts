# defmodule Mix.Tasks.Compile.Generated do
#   use Mix.Task.Compiler

#   @recursive true

#   @manifest ".mix/generated_modules.manifest"
#   @generated_dir "priv/generated"
#   @source_file "priv/generated_specs.bin"

#   @impl true
#   def run(_args) do
#     File.mkdir_p!(@generated_dir)
#     File.mkdir_p!(Path.dirname(@manifest))

#     spec = load_spec()
#     hash = :erlang.phash2(spec)

#     prev_hash =
#       case File.read(@manifest) do
#         {:ok, contents} -> String.to_integer(contents)
#         _ -> nil
#       end

#     if prev_hash == hash do
#       :noop
#     else
#       write_generated_files(spec)
#       File.write!(@manifest, Integer.to_string(hash))
#       :ok
#     end
#   end

#   @impl true
#   def manifests, do: [@manifest]

#   @impl true
#   def clean do
#     File.rm_rf!("generated")
#     File.rm(@manifest)
#     :ok
#   end

#   defp load_spec do
#     @source_file
#     |> File.read!()
#     |> :erlang.binary_to_term()
#   end

#   defp write_generated_files(spec) do
#     buckets = bucket_specs(spec)

#     # remove old generated files first
#     Path.wildcard(Path.join(@generated_dir, "*.ex"))
#     |> Enum.each(&File.rm!/1)

#     Enum.with_index(buckets, 1)
#     |> Enum.each(fn {bucket, idx} ->
#       path = Path.join(@generated_dir, "bucket_#{idx}.ex")
#       File.write!(path, render_bucket_module(bucket, idx))
#     end)
#   end

#   defp bucket_specs(spec) do
#     spec
#     |> Enum.chunk_every(100)
#   end

#   defp render_bucket_module(clauses, idx) do
#     defs =
#       Enum.map_join(clauses, "\n", fn {a, b, c, d, body_tag} ->
#         """
#         def some_func(#{inspect(a)}, #{inspect(b)}, #{inspect(c)}, #{inspect(d)}) do
#           MyApp.Generated.Shared.exec(#{inspect(body_tag)})
#         end
#         """
#       end)

#     """
#     defmodule #{inspect(Module.concat([MyApp.Generated, :"Bucket#{idx}"]))} do
#       #{defs}
#     end
#     """
#   end
# end
