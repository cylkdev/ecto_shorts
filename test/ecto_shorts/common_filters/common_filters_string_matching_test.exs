defmodule EctoShorts.CommonFilters.StringMatchingTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 string matching" do
    test "matches records where the field contains the text using like" do
      expected = from p in Post, where: like(p.title, ^"%hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{like: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "preserves caller-supplied wildcard patterns using like" do
      expected = from p in Post, where: like(p.title, ^"hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{like: "hello%"}}, [])

      assert Ecto.Adapters.SQL.to_sql(:all, EctoShorts.Config.repo(), expected) ===
               Ecto.Adapters.SQL.to_sql(:all, EctoShorts.Config.repo(), q2)
    end

    test "matches records where the field contains the text case-insensitively using ilike" do
      expected = from p in Post, where: ilike(p.title, ^"%hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{ilike: "hello"}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field matches any pattern in the like list" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: fragment("? LIKE ANY(?)", p.title, ^patterns)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{like: ["hello", "world"]}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field matches any pattern in the ilike list" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: fragment("? ILIKE ANY(?)", p.title, ^patterns)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{ilike: ["hello", "world"]}}, [])

      assert_sql(expected, q2)
    end

    test "preserves caller-supplied wildcard patterns in the ilike list" do
      patterns = ["hello%", "%world"]

      expected =
        from(p in Post,
          where: fragment("? ILIKE ANY(?)", p.title, ^patterns)
        )

      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{ilike: ["hello%", "%world"]}}, [])

      assert Ecto.Adapters.SQL.to_sql(:all, EctoShorts.Config.repo(), expected) ===
               Ecto.Adapters.SQL.to_sql(:all, EctoShorts.Config.repo(), q2)
    end

    test "excludes records where the field contains the text using negated like" do
      expected = from p in Post, where: not like(p.title, ^"%hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{not: %{like: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the field contains the text using negated ilike" do
      expected = from p in Post, where: not ilike(p.title, ^"%hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{not: %{ilike: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the field matches any pattern in the negated like list" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: not fragment("? LIKE ANY(?)", p.title, ^patterns)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{title: %{not: %{like: ["hello", "world"]}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "excludes records where the field matches any pattern in the negated ilike list" do
      patterns = ["%hello%", "%world%"]

      expected =
        from(p in Post,
          where: not fragment("? ILIKE ANY(?)", p.title, ^patterns)
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{title: %{not: %{ilike: ["hello", "world"]}}},
          []
        )

      assert_sql(expected, q2)
    end
  end
end
