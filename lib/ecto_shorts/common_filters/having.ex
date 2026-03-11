defmodule EctoShorts.CommonFilters.Having do
  alias EctoShorts.Compiler
  alias Ecto.Query

  require Ecto.Query

  {_, binding_patterns} =
    Compiler.query_binding_contracts(__MODULE__, positions: 10)

  def build_query(_filter, _source, query, _selected_binding, nil, _opts), do: query

  def build_query(filter, _source, query, selected_binding, dyn, _opts) do
    build_having(filter, query, selected_binding, dyn)
  end

  for {quoted_binding_head, quoted_binding_body} <- binding_patterns do
    defp build_having(:having, query, unquote(quoted_binding_head), dyn) do
      Query.having(
        query,
        [unquote_splicing(quoted_binding_body)],
        ^dyn
      )
    end

    defp build_having(:or_having, query, unquote(quoted_binding_head), dyn) do
      Query.or_having(
        query,
        [unquote_splicing(quoted_binding_body)],
        ^dyn
      )
    end
  end

  defp build_having(filter, query, _selected_binding, dyn) do
    case filter do
      :having ->
        Query.having(query, ^dyn)

      :or_having ->
        Query.or_having(query, ^dyn)
    end
  end
end
