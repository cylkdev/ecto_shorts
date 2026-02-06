defmodule EctoShorts.Dynamics.RepoAdapterOptsTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Dynamics

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  test "convert_to_dynamic/4 infers Postgres adapter from repo.__adapter__/0" do
    expected = dynamic([q], field(q, ^:views) == ^1)

    actual =
      Dynamics.convert_to_dynamic(
        EctoShorts.Schema.Post,
        {:as, nil},
        {:views, 1},
        repo: EctoShorts.Repo
      )

    assert_dynamic(expected, actual)
  end
end
