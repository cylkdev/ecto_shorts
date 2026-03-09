defmodule EctoShorts.Generator do
  @moduledoc since: "3.0.0"

  alias EctoShorts.Generator.AST

  @doc """
  Writes one or more generated module files to disk.

  Returns the full list of written file paths. When `opts[:partitions]` is
  greater than `1`, the first path belongs to the dispatcher module and the
  remaining paths belong to partition modules.
  """
  def write_module_files(builder, module_name, opts \\ []) do
    builder
    |> generate_modules(module_name, opts)
    |> Enum.map(fn {compiled_module_name, path, content} ->
      {compiled_module_name, write_file(path, content)}
    end)
  end

  @doc false
  def module_file_path(builder, module_name, opts \\ []) do
    dir = Path.join(priv_dir(), "generated")
    path = opts[:path] || module_to_path(builder)
    filename = opts[:filename] || module_to_filename(module_name)

    Path.join([dir, path, filename])
  end

  defp module_to_filename(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> Kernel.<>(".ex")
  end

  defp write_file(final_path, content) do
    parent_dir = Path.dirname(final_path)
    File.mkdir_p!(parent_dir)
    File.write!(final_path, content)
    final_path
  end

  defp module_to_path(builder) do
    builder
    |> Module.split()
    |> Enum.drop(1)
    |> Enum.map(&Macro.underscore/1)
    |> Enum.join("/")
  end

  defp priv_dir do
    :ecto_shorts |> :code.priv_dir() |> to_string()
  end

  @doc """
  Generates one or more modules for the given builder.

  Returns a list of maps with `:module`, `:path`, and `:content`. When
  `opts[:partitions]` is greater than `1`, the first entry is the dispatcher
  module and the remaining entries are partition modules.
  """
  def generate_modules(builder, module_name, opts \\ [])
      when is_atom(module_name) do
    partition_count = opts[:partitions] || 1

    clauses = build_clauses(builder, opts)

    if partition_count == 1 do
      [
        {
          module_name,
          module_file_path(builder, module_name, opts),
          module_template_string(module_name, clauses)
        }
      ]
    else
      clauses
      |> partition_clauses(partition_count)
      |> Enum.with_index(1)
      |> Enum.map(fn {partition_clauses, index} ->
        partition_module = partition_module_name(module_name, index)
        opts = Keyword.put(opts, :filename, partition_filename(module_name, index, opts))

        {
          partition_module,
          module_file_path(builder, partition_module, opts),
          module_template_string(partition_module, partition_clauses)
        }
      end)
    end
  end

  defp build_clauses(builder, opts) do
    named_clauses = AST.named_clause_asts(builder, opts)
    count_or_range = opts[:positions] || 10

    positional_clauses =
      count_or_range
      |> to_range()
      |> Enum.flat_map(fn index -> AST.positional_clause_asts(builder, index, opts) end)

    named_clauses ++ positional_clauses
  end

  defp to_range(count_or_range) do
    cond do
      is_integer(count_or_range) and count_or_range >= 1 ->
        1..count_or_range

      is_struct(count_or_range, Range) ->
        count_or_range

      true ->
        raise ArgumentError,
              "Expected a positive integer or range, got #{inspect(count_or_range)}"
    end
  end

  defp partition_clauses([], _count), do: [[]]

  defp partition_clauses(clauses, count) do
    actual_count =
      clauses
      |> length()
      |> min(count)
      |> max(1)

    chunk_size =
      clauses
      |> length()
      |> then(&div(&1 + actual_count - 1, actual_count))
      |> max(1)

    Enum.chunk_every(clauses, chunk_size)
  end

  defp partition_module_name(module_name, index) when is_integer(index) and index >= 1 do
    Module.concat(module_name, :"Partition#{index}")
  end

  defp partition_filename(module_name, index, opts) do
    filename = opts[:filename] || module_to_filename(module_name)
    extname = Path.extname(filename)
    basename = Path.rootname(filename, extname)

    "#{basename}_partition_#{index}#{extname}"
  end

  defp module_template_string(module_name, clauses) do
    formatted =
      clauses
      |> Enum.map(&quote_to_string/1)
      |> Enum.join("\n")

    format("""
    defmodule #{inspect(module_name)} do
      @moduledoc false
      import Ecto.Query, only: [dynamic: 2]

      @type binding_selector :: {:as | :at, term()}
      @type key :: atom()
      @type value :: term()
      @type compose_res :: Ecto.Query.t() | nil

      @doc false
      #{formatted}

      def dynamic_expr(_, _, _) do
        nil
      end
    end
    """)
  end

  defp format(text) do
    text
    |> Code.format_string!(
      line_length: 110,
      force_do_end_blocks: true,
      migrate_call_parens_on_pipe: true
    )
    |> IO.iodata_to_binary()
  end

  defp quote_to_string(quoted) do
    quoted
    |> Code.quoted_to_algebra()
    |> Inspect.Algebra.format(:infinity)
    |> IO.iodata_to_binary()
  end
end
