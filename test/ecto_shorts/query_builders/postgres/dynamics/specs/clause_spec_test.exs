defmodule EctoShorts.QueryBuilder.Dynamics.Compiler.ClauseSpecTest do
  use ExUnit.Case, async: true

  alias EctoShorts.QueryBuilder.Dynamics.Compiler.ClauseSpec

  test "new/1 validates required keys and returns a struct" do
    key_var = Macro.var(:key, nil)
    v_var = Macro.var(:v, nil)

    assert {:ok, %ClauseSpec{} = spec} =
             ClauseSpec.new(%{
               binding_head: quote(do: {:as, nil}),
               key: key_var,
               head: quote(do: {:==, unquote(v_var)}),
               body: quote(do: :ok)
             })

    assert spec.guard == nil
  end

  test "new/1 returns error when required keys are absent" do
    assert {:error,
            %NimbleOptions.ValidationError{
              message: "required :binding_head option not found, received options: [:key]",
              key: :binding_head,
              value: nil,
              keys_path: []
            }} = ClauseSpec.new(%{key: :id})
  end

  test "new/1 returns :invalid_spec for unknown keys" do
    assert {:error,
            %NimbleOptions.ValidationError{
              message:
                "unknown options [:unknown], valid options are: [:binding_head, :key, :head, :body, :guard]",
              key: [:unknown],
              value: nil,
              keys_path: []
            }} =
             ClauseSpec.new(%{
               binding_head: quote(do: {:as, nil}),
               key: Macro.var(:key, nil),
               head: quote(do: :anything),
               body: quote(do: :ok),
               unknown: :nope
             })
  end

  test "new/1 accepts a ClauseSpec struct" do
    spec =
      ClauseSpec.new!(%{
        binding_head: quote(do: {:as, nil}),
        key: Macro.var(:key, nil),
        head: quote(do: :anything),
        body: quote(do: :ok)
      })

    assert {:ok, %ClauseSpec{}} = ClauseSpec.new(spec)
  end
end
