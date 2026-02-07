defmodule EctoShorts.ConfigTest do
  use ExUnit.Case, async: false

  alias EctoShorts.Config

  setup do
    original = Application.get_env(:ecto_shorts, :compiler, :__missing__)

    on_exit(fn ->
      case original do
        :__missing__ -> Application.delete_env(:ecto_shorts, :compiler)
        value -> Application.put_env(:ecto_shorts, :compiler, value)
      end
    end)

    :ok
  end

  describe "compiler/0" do
    test "returns [] when compiler config is not set" do
      Application.delete_env(:ecto_shorts, :compiler)
      assert Config.compiler() == []
    end

    test "returns configured compiler options" do
      compiler_opts = [max_positional_bindings: 42]
      Application.put_env(:ecto_shorts, :compiler, compiler_opts)
      assert Config.compiler() == compiler_opts
    end
  end
end
