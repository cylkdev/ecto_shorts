defmodule EctoShorts.CommonFiltersTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 preload shapes" do
    test "matches Ecto.Query for a root preload atom" do
      expected = from p in Post, preload: :author

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{preload: :author},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a nested preload keyword list" do
      expected = from p in Post, preload: [comments: :author]

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{preload: [comments: :author]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named join-backed preload" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          preload: [author: a]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                preload: :author
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named nested join-backed preload tuple" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          preload: [author: {a, [posts: :comments]}]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                preload: [author: [posts: :comments]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional nested join-backed preload tuple" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          preload: [author: {a, [posts: :comments]}]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                preload: [author: [posts: :comments]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 group_by shapes" do
    test "matches Ecto.Query for a root group_by atom" do
      expected = from p in Post, group_by: :author_id

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: :author_id},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root group_by list" do
      expected = from p in Post, group_by: [:author_id, :title]

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: [:author_id, :title]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root group_by dynamic list" do
      dynamic_expr = dynamic([p], fragment("lower(?)", p.title))
      expected = from p in Post, group_by: ^[:author_id, dynamic_expr]

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{group_by: [:author_id, dynamic_expr]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding group_by atom" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name
      expected = group_by(source, [author: a], field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                group_by: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding group_by list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name
      expected = group_by(source, [author: a], [field(a, ^field_name)])

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                group_by: [:first_name]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding group_by dynamic list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name
      field_expr = dynamic([author: a], field(a, ^field_name))
      dynamic_expr = dynamic([author: a], fragment("lower(?)", a.first_name))
      expected = group_by(source, [author: a], ^[field_expr, dynamic_expr])

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                group_by: [:first_name, dynamic_expr]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding group_by atom" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      expected = group_by(source, [_, a], field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                group_by: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding group_by list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      expected = group_by(source, [_, a], [field(a, ^field_name)])

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                group_by: [:first_name]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding group_by dynamic list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      field_expr = dynamic([_, a], field(a, ^field_name))
      dynamic_expr = dynamic([_, a], fragment("lower(?)", a.first_name))
      expected = group_by(source, [_, a], ^[field_expr, dynamic_expr])

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                group_by: [:first_name, dynamic_expr]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 distinct shapes" do
    test "matches Ecto.Query for a root boolean distinct" do
      expected = from p in Post, distinct: true

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{distinct: true},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root distinct atom" do
      expected = from p in Post, distinct: :title

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{distinct: :title},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root distinct ordered keyword list" do
      expected = from p in Post, distinct: [desc: :title]

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{distinct: [desc: :title]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding distinct atom" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name
      expected = distinct(source, [author: a], field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                distinct: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding distinct ordered keyword list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name
      expected = distinct(source, [author: a], desc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                distinct: [desc: :first_name]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding distinct atom" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      expected = distinct(source, [_, a], field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                distinct: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding distinct ordered keyword list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      expected = distinct(source, [_, a], desc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                distinct: [desc: :first_name]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 order modifier shapes" do
    test "matches Ecto.Query for a root prepend_order_by atom" do
      expected = prepend_order_by(Post, [], desc: :title)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{prepend_order_by: :title},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root prepend_order_by ordered keyword list" do
      expected =
        prepend_order_by(
          Post,
          [],
          asc: :published_at,
          desc: :title
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{prepend_order_by: [asc: :published_at, desc: :title]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding prepend_order_by atom" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name
      expected = prepend_order_by(source, [author: a], desc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                prepend_order_by: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding prepend_order_by ordered keyword list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name
      expected = prepend_order_by(source, [author: a], asc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                prepend_order_by: [asc: :first_name]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding prepend_order_by atom" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      expected = prepend_order_by(source, [_, a], desc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                prepend_order_by: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding prepend_order_by ordered keyword list" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name
      expected = prepend_order_by(source, [_, a], asc: field(a, ^field_name))

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                prepend_order_by: [asc: :first_name]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for reverse_order on an existing ordered query" do
      source = from p in Post, order_by: [asc: p.title]
      expected = reverse_order(source)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{reverse_order: true},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for reverse_order after local order_by params" do
      expected =
        Post
        |> order_by([], desc: :title)
        |> reverse_order()

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{order_by: :title, reverse_order: true},
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 subquery shapes" do
    test "matches Ecto.Query for a root subquery map payload" do
      expected =
        Post
        |> where([p], p.id == ^2)
        |> subquery()

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{subquery: %{id: 2}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root subquery keyword payload" do
      expected =
        Post
        |> where([p], p.id == ^2)
        |> where([p], p.title == ^"Hello")
        |> subquery()

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{subquery: [id: 2, title: "Hello"]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for terminal subquery wrapping after local filters" do
      expected =
        Post
        |> order_by([], desc: :title)
        |> where([p], p.id == ^2)
        |> subquery()

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{order_by: :title, subquery: %{id: 2}},
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 update shapes" do
    test "matches Ecto.Query for a root update set payload" do
      updates = [set: [title: "After"]]
      expected = update(Post, [], ^updates)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{update: [set: [title: "After"]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root update inc payload" do
      updates = [inc: [views: 1]]
      expected = update(Post, [], ^updates)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{update: [inc: [views: 1]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root combined update payload" do
      updates = [set: [title: "After"], inc: [views: 1]]
      expected = update(Post, [], ^updates)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{update: [set: [title: "After"], inc: [views: 1]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root update map payload" do
      updates = [set: [title: "After"]]
      expected = update(Post, [], ^updates)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{update: %{set: %{title: "After"}}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding update payload" do
      source =
        from(p in Post,
          join: u in assoc(p, :author),
          as: :author
        )

      updates = [set: [title: dynamic([author: u], u.first_name)]]
      expected = update(source, [author: u], ^updates)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                update: [set: [title: dynamic([author: u], u.first_name)]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding update payload" do
      source =
        from(p in Post,
          join: u in assoc(p, :author)
        )

      updates = [set: [title: dynamic([_, u], u.first_name)]]
      expected = update(source, [_, u], ^updates)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                update: [set: [title: dynamic([_, u], u.first_name)]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 limit shapes" do
    test "matches Ecto.Query for a root integer limit" do
      expected = limit(Post, ^10)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{limit: 10},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when limit overrides a previous limit" do
      expected = limit(Post, ^10)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{limit: 10},
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 having shapes" do
    test "matches Ecto.Query for a root aggregate having" do
      source = from p in Post, group_by: p.author_id
      expected = from p in Post, group_by: p.author_id, having: avg(p.views) > ^100

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{having: %{views: %{avg: %{>: 100}}}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root negated having" do
      source = from p in Post, group_by: [p.title, p.views]
      expected = from p in Post, group_by: [p.title, p.views], having: not (p.views > ^10)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{having: %{views: %{not: %{>: 10}}}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root dynamic having" do
      source = from p in Post, group_by: p.author_id
      dynamic_expr = dynamic([p], avg(p.views) > 10)
      expected = from p in Post, group_by: p.author_id, having: ^dynamic_expr

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{having: dynamic_expr},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root or_having" do
      source = from p in Post, group_by: [p.title, p.views]
      expected = from p in Post, group_by: [p.title, p.views], or_having: p.views < ^5

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{or_having: %{views: %{<: 5}}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding aggregate having" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: a.first_name
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: a.first_name,
          having: avg(a.age) > ^10
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                having: %{age: %{avg: %{>: 10}}}
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding or_having" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: [a.first_name, a.age]
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          group_by: [a.first_name, a.age],
          or_having: a.age < ^5
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                or_having: %{age: %{<: 5}}
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding aggregate having" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          group_by: a.first_name
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          group_by: a.first_name,
          having: avg(a.age) > ^10
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                having: %{age: %{avg: %{>: 10}}}
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding or_having" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          group_by: [a.first_name, a.age]
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          group_by: [a.first_name, a.age],
          or_having: a.age < ^5
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                or_having: %{age: %{<: 5}}
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end
end
