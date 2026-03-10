defmodule EctoShorts.Generator do
  @moduledoc since: "3.0.0"

  alias EctoShorts.Generator.AST

  @doc """
  Writes one generated module file to disk.

  Returns the written module name and file path.
  """
  def write_module_file(builder, module_name, opts \\ []) do
    builder
    |> generate_module(module_name, opts)
    |> write_module()
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

  @doc false
  def write_module({compiled_module_name, path, content}) do
    {compiled_module_name, write_file(path, content)}
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
  Generates one module for the given builder.

  Returns a `{module, path, content}` tuple.
  """
  def generate_module(builder, module_name, opts \\ [])
      when is_atom(module_name) do
    clauses = build_clauses(builder, opts)

    {
      module_name,
      module_file_path(builder, module_name, opts),
      module_template_string(module_name, clauses)
    }
  end

  defp build_clauses(builder, opts) do
    named_clauses = AST.named_clause_asts(builder, opts)
    count = opts[:positions] || 10

    positional_clauses =
      Enum.flat_map(1..count, fn index ->
        AST.positional_clause_asts(builder, index, opts)
      end)

    named_clauses ++ positional_clauses
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

      @type selected_binding :: {:as | :at, term()}
      @type key :: atom()
      @type value :: term()
      @type negated :: :not | nil
      @type compose_res :: Ecto.Query.t() | nil

      @doc false
      #{formatted}

      def dynamic_expr(_, _, _, _) do
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
