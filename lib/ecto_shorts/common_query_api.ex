defmodule EctoShorts.CommonQueryAPI do
  alias EctoShorts.CommonQueryAPI.Generator

  alias EctoShorts.CommonQueryAPI.Generator.{
    FieldExprBuilder
  }

  require EctoShorts.CommonQueryAPI.Generator

  # Generator.define_query_api(
  #   :dynamic,
  #   max_positional_bindings: 2,
  #   builder: FieldExprBuilder,
  #   operators: [:==, :!=, :<, :<=, :>, :>=, :in, :ilike, :like, :=~]
  # )
end
