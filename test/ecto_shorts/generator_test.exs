defmodule EctoShorts.GeneratorTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Generator

  defmodule StubBuilder do
    @behaviour EctoShorts.Generator.ClauseSpec

    def operators, do: [:ids]

    def specs_for(spec_key, selected_binding, _q_var, opts) do
      context = opts[:context]
      negated_var = Macro.var(:_negated, context)
      value_var = Macro.var(:value, context)

      [
        %EctoShorts.Generator.Blueprint{
          guard: nil,
          key: spec_key,
          head: [negated_var, value_var],
          body:
            quote do
              _selected_binding = unquote(selected_binding)
              dynamic([], true)
              {unquote(spec_key), unquote(value_var)}
            end
        }
      ]
    end
  end

  test "generate_module/3 emits one module with named and positional clauses" do
    module = Module.concat(__MODULE__, :"Tmp#{System.unique_integer([:positive])}")

    content = Generator.generate_module(StubBuilder, module, positions: 2)

    assert content =~ "def dynamic_expr({:as, binding_alias}, :ids"
    assert content =~ "def dynamic_expr({:at, 2}, :ids"
    refute content =~ "Partition1"
  end

  test "write_file/2 writes one file for one module" do
    module = Module.concat(__MODULE__, :"Tmp#{System.unique_integer([:positive])}")
    dir = Path.join(System.tmp_dir!(), "ecto_shorts_generator_test_#{System.unique_integer([:positive])}")
    path = Path.join(dir, "single_module.ex")
    content = Generator.generate_module(StubBuilder, module, positions: 2)

    assert :ok = Generator.write_file(path, content)

    on_exit(fn ->
      File.rm(path)
      File.rmdir(dir)
    end)

    assert File.exists?(path)
    refute path =~ "partition"
  end
end
