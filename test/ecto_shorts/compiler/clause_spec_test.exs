defmodule EctoShorts.Compiler.ClauseSpecTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Compiler.ClauseSpec

  test "new/1 validates required keys and returns a struct" do
    key_var = Macro.var(:key, nil)
    v_var = Macro.var(:v, nil)

    spec =
      ClauseSpec.new(%{
        binding_head: quote(do: {:as, nil}),
        key: key_var,
        head: quote(do: {:==, unquote(v_var)}),
        body: quote(do: :ok)
      })

    assert %ClauseSpec{} = spec
    assert spec.guard === nil
  end

  test "new/1 raises when required keys are absent" do
    assert_raise ArgumentError, fn ->
      ClauseSpec.new(%{key: :id})
    end
  end

  test "new/1 raises for unknown keys" do
    assert_raise KeyError, fn ->
      ClauseSpec.new(%{
        binding_head: quote(do: {:as, nil}),
        key: Macro.var(:key, nil),
        head: quote(do: :anything),
        body: quote(do: :ok),
        unknown: :nope
      })
    end
  end

  test "new/1 accepts a ClauseSpec struct" do
    spec =
      ClauseSpec.new(%{
        binding_head: quote(do: {:as, nil}),
        key: Macro.var(:key, nil),
        head: quote(do: :anything),
        body: quote(do: :ok)
      })

    assert %ClauseSpec{} = ClauseSpec.new(spec)
  end
end
