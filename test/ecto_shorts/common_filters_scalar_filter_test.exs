defmodule EctoShorts.CommonFilters.ScalarFilterTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Comment
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 comparison operators" do
    test "matches records where the field equals the value using ==" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{==: 1}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field equals the value using the eq alias" do
      expected = from p in Post, where: p.id == ^1
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{eq: 1}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field is nil using == nil" do
      expected = from p in Post, where: is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{==: nil}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field is nil using the eq alias" do
      expected = from p in Post, where: is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{eq: nil}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field is not nil using != nil" do
      expected = from p in Post, where: not is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{!=: nil}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field is not nil using the ne alias" do
      expected = from p in Post, where: not is_nil(p.published_at)
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{ne: nil}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field is greater than the value" do
      expected = from p in Post, where: p.views > ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field is greater than or equal to the value" do
      expected = from p in Post, where: p.views >= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{>=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field is less than the value" do
      expected = from p in Post, where: p.views < ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field is less than or equal to the value" do
      expected = from p in Post, where: p.views <= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{<=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field does not equal the value using !=" do
      expected = from p in Post, where: p.views != ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{!=: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field does not equal the value using the ne alias" do
      expected = from p in Post, where: p.views != ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{ne: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the gt alias for greater than" do
      expected = from p in Post, where: p.views > ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{gt: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the gte alias for greater than or equal" do
      expected = from p in Post, where: p.views >= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{gte: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the lt alias for less than" do
      expected = from p in Post, where: p.views < ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{lt: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using the lte alias for less than or equal" do
      expected = from p in Post, where: p.views <= ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{lte: 10}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the field is in the given list" do
      expected = from p in Post, where: p.published in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{in: [true, false]}}, [])

      assert_sql(expected, q2)
    end

    test "matches records where the id is in the given list" do
      expected = from p in Post, where: p.id in ^[1, 2, 3]
      q2 = CommonFilters.convert_params_to_filter(Post, %{id: %{in: [1, 2, 3]}}, [])

      assert_sql(expected, q2)
    end

    test "treats a list value with == as an IN check" do
      expected = from p in Post, where: p.published in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{==: [true, false]}}, [])

      assert_sql(expected, q2)
    end

    test "treats a list value with != as a NOT IN check" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{!=: [true, false]}}, [])

      assert_sql(expected, q2)
    end

    test "preserves struct values like DateTime in the comparison" do
      dt = ~U[2026-01-01 00:00:00Z]
      expected = from p in Post, where: p.published_at >= ^dt
      q2 = CommonFilters.convert_params_to_filter(Post, %{published_at: %{>=: dt}}, [])

      assert_sql(expected, q2)
    end

    test "matches records using quantified default equality shorthand" do
      expected =
        from(p in Post,
          where:
            p.id ==
              all(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{all: %{from: Comment, where: %{published: true}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified select override" do
      expected =
        from(p in Post,
          where:
            p.id ==
              all(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.post_id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{all: %{from: Comment, select: %{field: "post_id"}, where: %{published: true}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using the explicit value wrapper for arithmetic expressions" do
      expected = from(p in Post, where: p.views > p.views + ^10)

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{views: %{>: %{value: %{+: [%{field: "views"}, %{value: 10}]}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "raises for an unsupported nil operator" do
      assert_raise ArgumentError, fn ->
        CommonFilters.convert_params_to_filter(Post, %{published_at: %{>: nil}}, [])
      end
    end
  end

  describe "convert_params_to_filter/3 negation" do
    test "excludes records where the field is in the given list" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{not: %{in: [true, false]}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records when == with a list is wrapped in not" do
      expected = from p in Post, where: p.published not in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{not: %{==: [true, false]}}}, [])

      assert_sql(expected, q2)
    end

    test "includes records when != with a list is wrapped in not" do
      expected = from p in Post, where: p.published in ^[true, false]
      q2 = CommonFilters.convert_params_to_filter(Post, %{published: %{not: %{!=: [true, false]}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the field is greater than the value" do
      expected = from p in Post, where: not (p.views > ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{>: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the field is greater than or equal to the value" do
      expected = from p in Post, where: not (p.views >= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{>=: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the field is less than the value" do
      expected = from p in Post, where: not (p.views < ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{<: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the field is less than or equal to the value" do
      expected = from p in Post, where: not (p.views <= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{<=: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the field equals the value" do
      expected = from p in Post, where: p.views != ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{==: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records using negated quantified equality" do
      expected =
        from(p in Post,
          where:
            not (p.id ==
                   all(
                     from(c in Comment,
                       where: c.published == ^true,
                       select: c.id
                     )
                   ))
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{not: %{all: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "includes records where the field equals the value using double negation" do
      expected = from p in Post, where: p.views == ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{!=: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "includes records where the field equals the value using negated ne alias" do
      expected = from p in Post, where: p.views == ^10
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{ne: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records using negated gt alias" do
      expected = from p in Post, where: not (p.views > ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{gt: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records using negated gte alias" do
      expected = from p in Post, where: not (p.views >= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{gte: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records using negated lt alias" do
      expected = from p in Post, where: not (p.views < ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{lt: 10}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records using negated lte alias" do
      expected = from p in Post, where: not (p.views <= ^10)
      q2 = CommonFilters.convert_params_to_filter(Post, %{views: %{not: %{lte: 10}}}, [])

      assert_sql(expected, q2)
    end
  end

  describe "convert_params_to_filter/3 string matching" do
    test "matches records where the field contains the text using like" do
      expected = from p in Post, where: like(p.title, ^"%hello%")
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{like: "hello"}}, [])

      assert_sql(expected, q2)
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

  describe "convert_params_to_filter/3 string transformations" do
    test "matches records by comparing the lowercased field to the value" do
      expected = from p in Post, where: fragment("lower(?)", p.title) == ^"hello"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{==: %{lower: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "matches records by comparing the uppercased field to the value" do
      expected = from p in Post, where: fragment("upper(?)", p.title) == ^"HELLO"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{==: %{upper: "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the lowercased field equals the value" do
      expected = from p in Post, where: fragment("lower(?)", p.title) != ^"hello"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{!=: %{lower: "hello"}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the uppercased field equals the value" do
      expected = from p in Post, where: fragment("upper(?)", p.title) != ^"HELLO"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{!=: %{upper: "HELLO"}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the lowercased field matches using negated ==" do
      expected = from p in Post, where: fragment("lower(?)", p.title) != ^"hello"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{not: %{==: %{lower: "hello"}}}}, [])

      assert_sql(expected, q2)
    end

    test "excludes records where the uppercased field matches using negated ==" do
      expected = from p in Post, where: fragment("upper(?)", p.title) != ^"HELLO"
      q2 = CommonFilters.convert_params_to_filter(Post, %{title: %{not: %{==: %{upper: "HELLO"}}}}, [])

      assert_sql(expected, q2)
    end
  end
end
