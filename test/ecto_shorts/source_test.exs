defmodule EctoShorts.Actions.SourceTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Actions.Source

  describe "new/1" do
    test "builds a Source struct with empty store when no store key given" do
      result = Source.new([])
      assert %Source{store: %{}} = result
    end

    test "builds a Source struct with provided store map" do
      result = Source.new(store: %{"users" => :users_table})
      assert %Source{store: %{"users" => :users_table}} = result
    end
  end
end
