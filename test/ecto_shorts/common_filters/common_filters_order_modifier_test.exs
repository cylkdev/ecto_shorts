defmodule EctoShorts.CommonFilters.OrderModifierTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 order modifier shapes" do
    # A bare atom for `prepend_order_by` defaults to `desc:` order, unlike `order_by:`
    # where a bare atom defaults to `asc:`.
    test "matches Ecto.Query for a root prepend_order_by atom" do
      expected = prepend_order_by(Post, [], desc: :title)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{prepend_order_by: :title},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root prepend_order_by ordered keyword list" do
      expected =
        prepend_order_by(
          Post,
          [],
          asc: :published_at,
          desc: :title
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{prepend_order_by: [asc: :published_at, desc: :title]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding prepend_order_by atom" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name
      expected = prepend_order_by(source, [author: a], desc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                prepend_order_by: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding prepend_order_by ordered keyword list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name
      expected = prepend_order_by(source, [author: a], asc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                prepend_order_by: [asc: :first_name]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding prepend_order_by atom" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      expected = prepend_order_by(source, [_, a], desc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                prepend_order_by: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding prepend_order_by ordered keyword list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      expected = prepend_order_by(source, [_, a], asc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                prepend_order_by: [asc: :first_name]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for reverse_order on an existing ordered query" do
      source = from p in Post, order_by: [asc: p.title]
      expected = reverse_order(source)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{reverse_order: true},
          []
        )

      assert_query(expected, actual)
    end

    # `reverse_order:` is applied after all other params in the same call, including
    # `order_by:` values that appear in the same params map.
    test "matches Ecto.Query for reverse_order after local order_by params" do
      expected =
        Post
        |> order_by([], desc: :title)
        |> reverse_order()

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{order_by: :title, reverse_order: true},
          []
        )

      assert_query(expected, actual)
    end
  end
end
