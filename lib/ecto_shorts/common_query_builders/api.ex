defmodule EctoShorts.CommonFilters.CommonQueryBuilders.API do
  alias Ecto.Query

  require Ecto.Query

  def join_association(query, {current_binding, join_binding_alias}, key, opts) do
    qual = opts[:qualifier] || :left

    on = opts[:on] || true

    prefix = opts[:prefix]

    if current_binding do
      Query.with_named_binding(query, join_binding_alias, fn query, as ->
        Query.join(query, qual, [{^current_binding, q}], assoc(q, ^key),
          as: ^as,
          on: ^on,
          prefix: ^prefix
        )
      end)
    else
      Query.with_named_binding(query, join_binding_alias, fn query, as ->
        Query.join(query, qual, [q], assoc(q, ^key),
          as: ^as,
          on: ^on,
          prefix: ^prefix
        )
      end)
    end
  end

  def join_association(query, current_binding, key, opts) do
    qual = opts[:qualifier] || :left

    on = opts[:on] || true

    prefix = opts[:prefix]

    if current_binding do
      Query.join(query, qual, [{^current_binding, q}], assoc(q, ^key), on: ^on, prefix: ^prefix)
    else
      Query.join(query, qual, [q], assoc(q, ^key), on: ^on, prefix: ^prefix)
    end
  end

  def join_subquery(query, {current_binding, join_binding_alias}, from, opts) do
    qual = opts[:qualifier] || :left

    on = opts[:on] || true

    prefix = opts[:prefix]

    Query.with_named_binding(query, join_binding_alias, fn query, as ->
      if current_binding do
        Query.join(query, qual, [{^current_binding, q}], subquery(from),
          as: ^as,
          on: ^on,
          prefix: ^prefix
        )
      else
        Query.join(query, qual, [q], subquery(from), as: ^as, on: ^on, prefix: ^prefix)
      end
    end)
  end

  def join_subquery(query, current_binding, from, opts) do
    qual = opts[:qualifier] || :left

    on = opts[:on] || true

    prefix = opts[:prefix]

    if current_binding do
      Query.join(query, qual, [{^current_binding, q}], subquery(from), on: ^on, prefix: ^prefix)
    else
      Query.join(query, qual, [q], subquery(from), on: ^on, prefix: ^prefix)
    end
  end
end
