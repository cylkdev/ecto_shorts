defmodule EctoShorts.Generator.BlueprintTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Generator.Blueprint

  test "new/1 validates required keys and returns a struct" do
    key_var = Macro.var(:key, nil)
    v_var = Macro.var(:v, nil)

    spec =
      Blueprint.new(%{
        binding_head: quote(do: {:as, nil}),
        key: key_var,
        head: quote(do: {:==, unquote(v_var)}),
        body: quote(do: :ok)
      })

    assert %Blueprint{} = spec
    assert spec.guard === nil
  end

  test "new/1 raises when required keys are absent" do
    assert_raise ArgumentError, fn ->
      Blueprint.new(%{key: :id})
    end
  end

  test "new/1 raises for unknown keys" do
    assert_raise KeyError, fn ->
      Blueprint.new(%{
        binding_head: quote(do: {:as, nil}),
        key: Macro.var(:key, nil),
        head: quote(do: :anything),
        body: quote(do: :ok),
        unknown: :nope
      })
    end
  end

  test "new/1 accepts a Blueprint struct" do
    spec =
      Blueprint.new(%{
        binding_head: quote(do: {:as, nil}),
        key: Macro.var(:key, nil),
        head: quote(do: :anything),
        body: quote(do: :ok)
      })

    assert %Blueprint{} = Blueprint.new(spec)
  end
end
