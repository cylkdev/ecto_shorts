defmodule EctoShorts.CommonFilters.WithNamedBindingTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 with_named_binding shapes" do
    test "matches Ecto.Query for the documented with_named_binding workflow" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}]},
          []
        )

      assert_query(expected, actual)
    end

    # `with_named_binding` is idempotent. If the named binding already exists on the
    # query, the join params are ignored and the query passes through unchanged.
    test "matches Ecto.Query when with_named_binding no-ops on an existing binding" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{with_named_binding: %{author: %{join: [association: [source: :author, as: :author]]}}},
          []
        )

      assert_query(source, actual)
    end
  end
end
