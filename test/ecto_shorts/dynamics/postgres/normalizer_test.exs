defmodule EctoShorts.Dynamics.Postgres.NormalizerTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Dynamics.Postgres.Normalizer

  describe "normalize_params/1" do
    test "passes a plain scalar through as a single-element list" do
      assert [true] = Normalizer.normalize_params(true)
      assert [42] = Normalizer.normalize_params(42)
      assert ["hello"] = Normalizer.normalize_params("hello")
    end

    test "converts a map to a keyword list and normalizes" do
      assert [{:title, "hello"}] = Normalizer.normalize_params(%{title: "hello"})
    end

    test "flattens a keyword list" do
      assert [{:title, "hello"}, {:published, true}] =
               Normalizer.normalize_params(title: "hello", published: true)
    end

    test "preserves quantifier operators" do
      result = Normalizer.normalize_params(all: [:a, :b])
      assert [{:all, [:a, :b]}] = result
    end

    test "preserves arithmetic value operators as value nodes" do
      result = Normalizer.normalize_params(+: [1, 2])
      assert [{:+, {1, 2}}] = result
    end

    test "preserves datetime wrapper operators as value nodes" do
      result = Normalizer.normalize_params(datetime: [add: [count: 1, interval: :day]])
      assert [{:datetime, {:add, [count: 1, interval: :day]}}] = result
    end

    test "recursively normalizes nested keyword values" do
      result = Normalizer.normalize_params(views: [>: 10])
      assert [{:views, {:>, 10}}] = result
    end

    test "normalizes a string field name inside a field marker" do
      result = Normalizer.normalize_params([{:inserted_at, {:field, "inserted_at"}}])
      assert [{:inserted_at, {:field, :inserted_at}}] = result
    end
  end

  describe "normalize_value_node/1" do
    test "passes scalars through unchanged" do
      assert 42 = Normalizer.normalize_value_node(42)
      assert :foo = Normalizer.normalize_value_node(:foo)
      assert "bar" = Normalizer.normalize_value_node("bar")
      assert nil == Normalizer.normalize_value_node(nil)
    end

    test "converts a map to a list and re-normalizes" do
      assert [{:a, 1}] = Normalizer.normalize_value_node(%{a: 1})
    end

    test "normalizes {:field, atom_name}" do
      assert {:field, :inserted_at} = Normalizer.normalize_value_node({:field, :inserted_at})
    end

    test "normalizes {:field, binary_name} — atomizes the string" do
      assert {:field, :inserted_at} = Normalizer.normalize_value_node({:field, "inserted_at"})
    end

    test "normalizes {:value, inner}" do
      assert {:value, 5} = Normalizer.normalize_value_node({:value, 5})
    end

    test "normalizes arithmetic operator node {op, [left, right]}" do
      assert {:+, {1, 2}} = Normalizer.normalize_value_node({:+, [1, 2]})
      assert {:-, {10, 3}} = Normalizer.normalize_value_node({:-, [10, 3]})
      assert {:*, {4, 5}} = Normalizer.normalize_value_node({:*, [4, 5]})
      assert {:/, {10, 2}} = Normalizer.normalize_value_node({:/, [10, 2]})
    end

    test "raises for arithmetic node with wrong arity" do
      assert_raise ArgumentError, ~r/two-element list/, fn ->
        Normalizer.normalize_value_node({:+, [1]})
      end
    end

    test "normalizes datetime wrapper {:datetime, payload}" do
      assert {:datetime, {:add, [count: 1, interval: :day]}} =
               Normalizer.normalize_value_node({:datetime, [add: [count: 1, interval: :day]]})
    end

    test "normalizes datetime wrapper {:date, payload}" do
      assert {:date, {:ago, [count: 7, interval: :day]}} =
               Normalizer.normalize_value_node({:date, [ago: [count: 7, interval: :day]]})
    end

    test "raises for datetime wrapper with unrecognized operation" do
      assert_raise ArgumentError, ~r/datetime/, fn ->
        Normalizer.normalize_value_node({:datetime, [unknown: [count: 1, interval: :day]]})
      end
    end

    test "normalizes datetime operation node {op, params}" do
      assert {:add, [count: 5, interval: :hour]} =
               Normalizer.normalize_value_node({:add, [count: 5, interval: :hour]})
    end

    test "normalizes datetime operation node with field" do
      assert {:ago, [field: :inserted_at, count: 3, interval: :day]} =
               Normalizer.normalize_value_node({:ago, [field: :inserted_at, count: 3, interval: :day]})
    end

    test "normalizes field: shorthand keyword" do
      assert {:field, :title} = Normalizer.normalize_value_node(field: :title)
    end

    test "normalizes value: shorthand keyword" do
      assert {:value, 99} = Normalizer.normalize_value_node(value: 99)
    end

    test "normalizes empty list" do
      assert [] = Normalizer.normalize_value_node([])
    end

    test "recursively normalizes a list" do
      assert [{:field, :title}, 42] =
               Normalizer.normalize_value_node([{:field, :title}, 42])
    end
  end

  describe "normalize_field_name/1" do
    test "passes an atom through unchanged" do
      assert :inserted_at = Normalizer.normalize_field_name(:inserted_at)
    end

    test "converts a binary to an existing atom" do
      assert :inserted_at = Normalizer.normalize_field_name("inserted_at")
    end

    test "raises for a binary that is not an existing atom" do
      assert_raise ArgumentError, fn ->
        Normalizer.normalize_field_name("this_atom_certainly_does_not_exist_xyz_abc_123")
      end
    end
  end

  describe "normalize_datetime_node/1" do
    test "accepts a keyword list with count and interval" do
      assert [count: 1, interval: :day] =
               Normalizer.normalize_datetime_node(count: 1, interval: :day)
    end

    test "accepts a keyword list with field, count, and interval" do
      assert [field: :inserted_at, count: 2, interval: :hour] =
               Normalizer.normalize_datetime_node(field: :inserted_at, count: 2, interval: :hour)
    end

    test "accepts a map and normalizes it" do
      assert [count: 5, interval: :minute] =
               Normalizer.normalize_datetime_node(%{count: 5, interval: :minute})
    end

    test "normalizes string field name inside datetime node" do
      result = Normalizer.normalize_datetime_node(field: "inserted_at", count: 1, interval: :day)
      assert [field: :inserted_at, count: 1, interval: :day] = result
    end

    test "raises for a non-keyword list" do
      assert_raise ArgumentError, ~r/keyword list or map/, fn ->
        Normalizer.normalize_datetime_node([:a, :b])
      end
    end
  end

  describe "normalize_keyword_params/2" do
    test "returns empty list for empty input" do
      assert [] = Normalizer.normalize_keyword_params([], [])
    end

    test "preserves quantifier entries" do
      assert [{:all, [1, 2]}] = Normalizer.normalize_keyword_params([{:all, [1, 2]}], [])
      assert [{:any, :foo}] = Normalizer.normalize_keyword_params([{:any, :foo}], [])
    end

    test "preserves arithmetic operator entries as value nodes" do
      assert [{:+, {1, 2}}] = Normalizer.normalize_keyword_params([{:+, [1, 2]}], [])
    end

    test "preserves datetime wrapper entries as value nodes" do
      assert [{:datetime, {:add, [count: 1, interval: :day]}}] =
               Normalizer.normalize_keyword_params(
                 [{:datetime, [add: [count: 1, interval: :day]]}],
                 []
               )
    end

    test "flattens a regular key/value pair" do
      assert [{:title, "hello"}] =
               Normalizer.normalize_keyword_params([{:title, "hello"}], [])
    end

    test "returns entries in original order" do
      result = Normalizer.normalize_keyword_params([a: 1, b: 2, c: 3], [])
      assert [{:a, 1}, {:b, 2}, {:c, 3}] = result
    end
  end
end
