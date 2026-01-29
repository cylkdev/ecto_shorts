defmodule EctoShorts.QueryBuilders do
  @moduledoc false

  @default_adapter EctoShorts.QueryBuilders.Postgres

  def build_query(source, query, binding_selector, current_filter, args, opts \\ []) do
    adapter = Keyword.get(opts, :query_builder, @default_adapter)
    adapter.build_query(source, query, binding_selector, current_filter, args, opts)
  end
end
