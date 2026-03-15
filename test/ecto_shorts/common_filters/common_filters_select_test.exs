defmodule EctoShorts.CommonFilters.SelectTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 select shapes" do
    test "matches Ecto.Query for a root select field atom" do
      expected = from(p in Post, select: p.title)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: :title},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for selecting the full root binding" do
      expected = from(p in Post, select: p)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: true},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root select map field list" do
      expected = from(p in Post, select: map(p, [:id, :title]))

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: {:map, [:id, :title]}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root select map alias mapping" do
      expected =
        from(p in Post,
          select: %{post_id: p.id, post_title: p.title}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: {:map, %{post_id: :id, post_title: :title}}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root select struct projection" do
      expected = from(p in Post, select: struct(p, [:id, :title]))

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: {:struct, [:id, :title]}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding select field" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          select: a.first_name
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                select: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding select map alias mapping" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          select: %{author_name: a.first_name}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                select: %{author_name: :first_name}
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a first positional binding select alias mapping" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          select: p.title
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              first: %{
                select: :title
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a last positional binding select alias mapping" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          join: c in assoc(p, :comments)
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          join: c in assoc(p, :comments),
          select: c.body
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              last: %{
                select: :body
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when select overwrites an existing select" do
      source = from(p in Post, select: p.title)
      expected = from(p in Post, select: p.id)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              source,
              %{select: :id},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Query already has a :select expression"
    end
  end
end
