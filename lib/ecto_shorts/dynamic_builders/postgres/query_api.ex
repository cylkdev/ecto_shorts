defmodule EctoShorts.DynamicBuilders.Postgres.QueryAPI do
  alias EctoShorts.CommonQueryAPI.DynamicBuilder

  require Ecto.Query
  require EctoShorts.CommonQueryAPI.DynamicBuilder

  # Query.dynamic(
  #       [{^binding_alias, d}],
  #       fragment(
  #         """
  #         NOT EXISTS (
  #           SELECT 1
  #           FROM unnest(?) AS input_tag
  #           WHERE NOT EXISTS (
  #             SELECT 1
  #             FROM unnest(?) AS db_tag
  #             WHERE input_tag LIKE db_tag
  #           )
  #         )
  #         """,
  #         ^patterns,
  #         field(d, ^key)
  #       )
  #     )

  DynamicBuilder.define_base_fragment_api(
    :ilike,
    """
    NOT EXISTS (
      SELECT 1
      FROM unnest(?) AS input_tag
      WHERE NOT EXISTS (
        SELECT 1
        FROM unnest(?) AS db_tag
        WHERE input_tag LIKE db_tag
      )
    )
    """,
    [:key, :patterns],
    [var: :patterns, field: :key]
  )
end
