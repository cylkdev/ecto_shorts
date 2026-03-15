defmodule EctoShorts.CommonFilters.WithNamedBindingExtendedTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query
  import ExUnit.CaptureLog

  describe "convert_params_to_filter/3 with_named_binding warning paths" do
    test "logs a warning and returns query unchanged when params is not a map or keyword list" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_named_binding: "bad value"},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :with_named_binding params to be a map or keyword list"
    end

    test "logs a warning and returns query unchanged for a non-tuple element in the params list" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_named_binding: [:not_a_pair]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :with_named_binding params to be a map or keyword list"
    end

    test "logs a warning when the callback does not create the named binding" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_named_binding: [author: %{limit: 1}]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "callback function for with_named_binding/3 should create a named binding"
    end
  end
end
