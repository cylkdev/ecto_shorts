defmodule EctoShorts.GeneratorTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Dynamics.Postgres.CommonExprBuilder
  alias EctoShorts.Generator

  test "generate_module/3 emits one module with named and positional clauses" do
    module = Module.concat(__MODULE__, :"Tmp#{System.unique_integer([:positive])}")

    assert {^module, _path, content} = Generator.generate_module(Spec, module, positions: 2)

    assert content =~ "def dynamic_expr({:as, binding_alias}, :ids"
    assert content =~ "def dynamic_expr({:at, 2}, :ids"
    refute content =~ "Partition1"
  end

  test "write_module_file/3 writes one file for one module" do
    module = Module.concat(__MODULE__, :"Tmp#{System.unique_integer([:positive])}")

    assert {^module, path} =
             Generator.write_module_file(Spec, module,
               path: "generator_test",
               filename: "single_module.ex",
               positions: 2
             )

    on_exit(fn ->
      File.rm(path)
      File.rmdir(Path.dirname(path))
    end)

    assert File.exists?(path)
    refute path =~ "partition"
  end
end
