defmodule EctoShorts.Generator.UnifiedBindingGenerationTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Dynamics.Postgres.CommonExpr.Spec
  alias EctoShorts.Generator

  test "generate_modules/4 emits one module with named and positional clauses" do
    module = Module.concat(__MODULE__, :"Tmp#{System.unique_integer([:positive])}")

    assert [
             {^module, _path, content}
           ] = Generator.generate_modules(Spec, module, %{}, positions: 2)

    assert content =~ "def dynamic_expr({:as, binding_alias}, :ids"
    assert content =~ "def dynamic_expr({:at, 2}, :ids"
  end
end
