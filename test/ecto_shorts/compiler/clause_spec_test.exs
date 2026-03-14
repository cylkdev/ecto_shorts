defmodule EctoShorts.Generator.BlueprintTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Generator.Blueprint

  test "struct!/2 validates required keys and returns a struct" do
    key_var = Macro.var(:key, nil)
    v_var = Macro.var(:v, nil)

    spec =
      struct!(Blueprint, %{
        key: key_var,
        head: [v_var],
        body: quote(do: :ok),
        guard: nil
      })

    assert %Blueprint{} = spec
    assert spec.guard === nil
  end

  test "struct!/2 raises when required keys are absent" do
    assert_raise ArgumentError, fn ->
      struct!(Blueprint, %{key: :id})
    end
  end

  test "struct!/2 raises for unknown keys" do
    assert_raise KeyError, fn ->
      struct!(Blueprint, %{
        key: Macro.var(:key, nil),
        head: [quote(do: :anything)],
        body: quote(do: :ok),
        guard: nil,
        unknown: :nope
      })
    end
  end

  test "struct!/2 accepts a Blueprint struct" do
    spec =
      struct!(Blueprint, %{
        key: Macro.var(:key, nil),
        head: [quote(do: :anything)],
        body: quote(do: :ok),
        guard: nil
      })

    assert %Blueprint{} = struct!(Blueprint, Map.from_struct(spec))
  end
end
