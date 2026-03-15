defmodule EctoShorts.Actions.SourceTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Actions.Source

  describe "new/1" do
    test "builds a Source struct with empty tables when no tables key given" do
      result = Source.new([])
      assert %Source{tables: %{}} = result
    end

    test "builds a Source struct with provided tables map" do
      result = Source.new(tables: %{"users" => :users_table})
      assert %Source{tables: %{"users" => :users_table}} = result
    end
  end
end
