defmodule EctoShorts.CommonFilters.WindowsExtendedTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 windows extended paths" do
    test "matches Ecto.Query for a root windows map payload (map conversion path)" do
      field_name = :author_id

      expected =
        windows(Post, [p], post_window: [partition_by: [field(p, ^field_name)], order_by: []])

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: %{post_window: %{partition_by: :author_id}}},
          []
        )

      assert_query(expected, actual)
    end

    test "keeps the query unchanged when windows list contains a non-pair entry" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{windows: [:not_a_pair]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :windows params to be a map or keyword list"
    end

    test "matches Ecto.Query for windows with order_by as a bare atom" do
      order_field = :inserted_at

      expected =
        windows(Post, [p],
          post_window: [
            partition_by: [],
            order_by: [field(p, ^order_field)]
          ]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [order_by: :inserted_at]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for windows with order_by as a {dir, field} tuple" do
      order_field = :inserted_at

      expected =
        windows(Post, [p],
          post_window: [
            partition_by: [],
            order_by: [{:desc, field(p, ^order_field)}]
          ]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [order_by: [desc: :inserted_at]]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for windows with partition_by as nil (empty list result)" do
      order_field = :inserted_at

      expected =
        windows(Post, [p],
          post_window: [
            partition_by: [],
            order_by: [desc: field(p, ^order_field)]
          ]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [order_by: [desc: :inserted_at]]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for windows with order_by as a bare-atom list (non-keyword)" do
      field1 = :inserted_at
      field2 = :id

      expected =
        windows(Post, [p],
          post_window: [
            partition_by: [],
            order_by: [field(p, ^field1), field(p, ^field2)]
          ]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [order_by: [:inserted_at, :id]]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for windows with partition_by as a bare-atom list (non-keyword)" do
      field1 = :author_id
      field2 = :id

      expected =
        windows(Post, [p],
          post_window: [
            partition_by: [field(p, ^field1), field(p, ^field2)],
            order_by: []
          ]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [partition_by: [:author_id, :id]]]},
          []
        )

      assert_query(expected, actual)
    end
  end
end
