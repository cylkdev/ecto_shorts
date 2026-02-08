defmodule EctoShorts.Dynamics.DynamicExpressionAdapterOptsTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Dynamics
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  test "convert_to_dynamic/4 accepts :dynamic_adapter override" do
    expected = dynamic([q], field(q, ^:views) == ^:override)

    actual =
      Dynamics.convert_to_dynamic(
        Post,
        {:as, nil},
        {:views, 1},
        dynamic_adapter: EctoShorts.TestDynamicExpressionAdapter
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic/4 raises for unsupported repo adapter" do
    assert_raise ArgumentError,
                 ~r/Unsupported Ecto repo adapter/,
                 fn ->
                   Dynamics.convert_to_dynamic(
                     Post,
                     {:as, nil},
                     {:views, 1},
                     repo: EctoShorts.TestUnsupportedRepo
                   )
                 end
  end
end
