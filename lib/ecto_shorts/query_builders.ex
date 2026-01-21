defmodule EctoShorts.QueryBuilders do
  @moduledoc false

  alias EctoShorts.QueryBuilders.Postgres

  def build_query(
        schema,
        filter,
        query,
        binding_selector,
        join_params,
        opts \\ []
      ) do
    adapter(opts).build_query(schema, filter, query, binding_selector, join_params, opts)
  end

  defp adapter(opts) do
    opts[:query_builder] || Postgres
  end
end
