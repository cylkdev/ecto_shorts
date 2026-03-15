defmodule EctoShorts.CommonFilters.ComparisonOperatorsTest do
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
      expected = from p in Post, where: is_nil(p.published) or p.published not in ^[true, false]
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

    test "matches records using quantified any default equality shorthand" do
      expected =
        from(p in Post,
          where:
            p.id ==
              any(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{any: %{from: Comment, where: %{published: true}}}},
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

    test "matches records using quantified any select override" do
      expected =
        from(p in Post,
          where:
            p.id ==
              any(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.post_id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{any: %{from: Comment, select: %{field: "post_id"}, where: %{published: true}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified greater-than all comparison" do
      expected =
        from(p in Post,
          where:
            p.id >
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
          %{id: %{>: %{all: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified greater-than-or-equal all comparison" do
      expected =
        from(p in Post,
          where:
            p.id >=
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
          %{id: %{>=: %{all: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified less-than all comparison" do
      expected =
        from(p in Post,
          where:
            p.id <
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
          %{id: %{<: %{all: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified less-than-or-equal all comparison" do
      expected =
        from(p in Post,
          where:
            p.id <=
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
          %{id: %{<=: %{all: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified explicit equality against all comparison" do
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
          %{id: %{==: %{all: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified explicit inequality against all comparison" do
      expected =
        from(p in Post,
          where:
            p.id !=
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
          %{id: %{!=: %{all: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified greater-than any comparison" do
      expected =
        from(p in Post,
          where:
            p.id >
              any(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{>: %{any: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified greater-than-or-equal any comparison" do
      expected =
        from(p in Post,
          where:
            p.id >=
              any(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{>=: %{any: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified less-than any comparison" do
      expected =
        from(p in Post,
          where:
            p.id <
              any(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{<: %{any: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified less-than-or-equal any comparison" do
      expected =
        from(p in Post,
          where:
            p.id <=
              any(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{<=: %{any: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified explicit equality against any comparison" do
      expected =
        from(p in Post,
          where:
            p.id ==
              any(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{==: %{any: %{from: Comment, where: %{published: true}}}}},
          []
        )

      assert_sql(expected, q2)
    end

    test "matches records using quantified explicit inequality against any comparison" do
      expected =
        from(p in Post,
          where:
            p.id !=
              any(
                from(c in Comment,
                  where: c.published == ^true,
                  select: c.id
                )
              )
        )

      q2 =
        CommonFilters.convert_params_to_filter(
          Post,
          %{id: %{!=: %{any: %{from: Comment, where: %{published: true}}}}},
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
end
