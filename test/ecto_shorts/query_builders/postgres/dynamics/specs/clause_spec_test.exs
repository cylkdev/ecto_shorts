defmodule EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseSpecTest do
  use ExUnit.Case, async: true

  alias EctoShorts.QueryBuilders.Postgres.Dynamics.Specs.ClauseSpec

  test "new/1 validates required keys and returns a struct" do
    key_var = Macro.var(:key, nil)
    v_var = Macro.var(:v, nil)

    assert {:ok, %ClauseSpec{} = spec} =
             ClauseSpec.new(%{
               kind: :clause,
               binding_head: quote(do: {:as, nil}),
               key: key_var,
               head: quote(do: {:==, unquote(v_var)}),
               body: quote(do: :ok)
             })

    assert spec.kind == :clause
    assert spec.guard == nil
  end

  test "new/1 returns error when required keys are absent" do
    assert {:error,
            %NimbleOptions.ValidationError{
              message: "required :binding_head option not found, received options: [:kind]",
              key: :binding_head,
              value: nil,
              keys_path: []
            }} = ClauseSpec.new(%{kind: :clause})
  end

  test "new/1 returns :invalid_spec when kind is not an atom" do
    assert {:error,
            %NimbleOptions.ValidationError{
              message: "invalid value for :kind option: expected atom, got: 123",
              key: :kind,
              value: 123,
              keys_path: []
            }} =
             ClauseSpec.new(%{
               kind: 123,
               binding_head: quote(do: {:as, nil}),
               key: Macro.var(:key, nil),
               head: quote(do: :anything),
               body: quote(do: :ok)
             })
  end

  test "new/1 returns :invalid_spec for unknown keys" do
    assert {:error,
            %NimbleOptions.ValidationError{
              message:
                "unknown options [:unknown], valid options are: [:kind, :binding_head, :key, :head, :body, :guard]",
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
        kind: :clause,
        binding_head: quote(do: {:as, nil}),
        key: Macro.var(:key, nil),
        head: quote(do: :anything),
        body: quote(do: :ok)
      })

    assert {:ok, %ClauseSpec{}} = ClauseSpec.new(spec)
  end
end
