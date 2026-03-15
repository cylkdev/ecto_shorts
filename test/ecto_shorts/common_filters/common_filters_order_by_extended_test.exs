defmodule EctoShorts.CommonFilters.OrderByExtendedTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 order_by list shapes" do
    test "orders by a list of direction-field tuples" do
      expected = from p in Post, order_by: [asc: p.title, desc: p.views]

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{order_by: [asc: :title, desc: :views]},
          []
        )

      assert_query(expected, actual)
    end

    test "orders by a list of bare atoms defaulting to desc" do
      expected = from p in Post, order_by: [desc: p.title, desc: p.views]

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{order_by: [:title, :views]},
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 prepend_order_by list shapes" do
    test "prepend_order_by with a list of direction-field tuples" do
      expected = prepend_order_by(Post, [], asc: :title, desc: :views)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{prepend_order_by: [asc: :title, desc: :views]},
          []
        )

      assert_query(expected, actual)
    end

    test "prepend_order_by with a list of bare atoms defaulting to desc" do
      expected = prepend_order_by(Post, [], desc: :title, desc: :views)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{prepend_order_by: [:title, :views]},
          []
        )

      assert_query(expected, actual)
    end

    test "prepend_order_by with named binding and a list of direction-field tuples" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name

      expected =
        prepend_order_by(source, [author: a], asc: field(a, ^field_name), desc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{as: %{author: %{prepend_order_by: [asc: :first_name, desc: :first_name]}}},
          []
        )

      assert_query(expected, actual)
    end

    test "prepend_order_by with positional binding and a list of direction-field tuples" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      expected = prepend_order_by(source, [_, a], asc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{at: %{2 => %{prepend_order_by: [asc: :first_name]}}},
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 order_by DynamicExpr and fallthrough paths" do
    test "order_by passes a DynamicExpr entry through unchanged in a list" do
      dyn = Ecto.Query.dynamic([p], p.views > ^0)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{order_by: [dyn]},
          []
        )

      refute is_nil(actual)
    end

    test "order_by passes a non-atom non-dynamic list entry through as-is (other branch)" do
      dyn = Ecto.Query.dynamic([p], p.views > ^0)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{order_by: [asc: dyn]},
          []
        )

      assert %Ecto.Query{} = actual
    end

    test "order_by with a non-binding selector uses the fallthrough build_order_by" do
      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{order_by: [asc: :title, desc: :views]},
          []
        )

      assert %Ecto.Query{} = actual
    end

    test "prepend_order_by passes a DynamicExpr entry through unchanged in a list" do
      dyn = Ecto.Query.dynamic([p], p.views > ^0)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{prepend_order_by: [dyn]},
          []
        )

      assert %Ecto.Query{} = actual
    end

    test "prepend_order_by passes a non-atom non-dynamic list entry through as-is (other branch)" do
      dyn = Ecto.Query.dynamic([p], p.views > ^0)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{prepend_order_by: [asc: dyn]},
          []
        )

      assert %Ecto.Query{} = actual
    end

    test "prepend_order_by with a raw keyword list uses fallthrough path" do
      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{prepend_order_by: [asc: :title]},
          []
        )

      assert %Ecto.Query{} = actual
    end
  end

  describe "convert_params_to_filter/3 reverse_order warning" do
    test "logs a warning and returns the query unchanged when reverse_order is not true" do
      expected = from p in Post, order_by: [asc: p.title]

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              expected,
              %{reverse_order: false},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :reverse_order value to be true"
    end
  end
end
