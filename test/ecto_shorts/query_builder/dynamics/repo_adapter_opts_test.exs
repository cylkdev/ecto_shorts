defmodule EctoShorts.QueryBuilder.Dynamics.RepoAdapterOptsTest do
  use ExUnit.Case, async: true

  alias EctoShorts.QueryBuilder.Dynamics

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  test "convert_to_dynamic/4 accepts a Postgres repo adapter" do
    expected = dynamic([q], field(q, ^:views) == ^1)

    actual =
      Dynamics.convert_to_dynamic(
        EctoShorts.Schema.Post,
        {:as, nil},
        {:views, 1}
      )

    assert_dynamic(expected, actual)
  end
end
