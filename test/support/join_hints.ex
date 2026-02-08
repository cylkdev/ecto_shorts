defmodule EctoShorts.TestJoinHints do
  @moduledoc false

  import Ecto.Query, only: [join: 5]

  alias Ecto.Query

  def resolve_join_source(binding_selector, source_key, params) do
    EctoShorts.TestJoinSources.resolve_join_source(binding_selector, source_key, params)
  end

  def build_hint(
        %Query{} = query,
        _binding_selector,
        %{
          hints: [:users_age_index],
          qualifier: qualifier,
          as: as,
          prefix: prefix,
          on: true,
          source: {:fragment, source_query}
        }
      ) do
    hinted_query =
      join(
        query,
        qualifier,
        [],
        j in ^source_query,
        as: ^as,
        on: true,
        prefix: ^prefix,
        hints: ["USE INDEX(users_age_index)"]
      )

    {:ok, hinted_query}
  end

  def build_hint(_query, _binding_selector, _hint_context) do
    {:error, :unsupported_hint_key}
  end
end
