defmodule EctoShorts.CommonFilters.SchemalessAssociationFilterTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters

  # Association shorthand requires schema introspection to detect that a key
  # corresponds to an association. For a schemaless source, `association_key?/2`
  # always returns false and the key falls through to a plain field-equality
  # filter instead of producing a join. This test pins that silent-skip behavior.
  describe "convert_params_to_filter/3 association shorthand degradation (schemaless)" do
    # BUG: For a schemaless source, a map value for an unknown key recurses
    # into the filter pipeline and produces a nil dynamic expression, causing
    # Ecto to raise ArgumentError at query build time instead of ignoring it.
    @tag :skip
    test "a key that would be an association shorthand on a schema source is silently skipped for a schemaless source" do
      actual =
        CommonFilters.convert_params_to_filter(
          "posts",
          %{comments: %{published: true}},
          []
        )

      assert actual.joins == []
    end
  end
end
