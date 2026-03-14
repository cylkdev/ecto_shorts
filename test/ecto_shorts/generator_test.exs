defmodule EctoShorts.GeneratorTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Dynamics.Postgres.CommonExprBuilder
  alias EctoShorts.Generator

  test "generate_module/3 emits one module with named and positional clauses" do
    module = Module.concat(__MODULE__, :"Tmp#{System.unique_integer([:positive])}")

    content = Generator.generate_module(CommonExprBuilder, module, positions: 2)

    assert content =~ "def dynamic_expr({:as, binding_alias}, :ids"
    assert content =~ "def dynamic_expr({:at, 2}, :ids"
    refute content =~ "Partition1"
  end

  test "write_file/2 writes one file for one module" do
    module = Module.concat(__MODULE__, :"Tmp#{System.unique_integer([:positive])}")
    dir = Path.join(System.tmp_dir!(), "ecto_shorts_generator_test_#{System.unique_integer([:positive])}")
    path = Path.join(dir, "single_module.ex")
    content = Generator.generate_module(CommonExprBuilder, module, positions: 2)

    assert :ok = Generator.write_file(path, content)

    on_exit(fn ->
      File.rm(path)
      File.rmdir(dir)
    end)

    assert File.exists?(path)
    refute path =~ "partition"
  end
end
