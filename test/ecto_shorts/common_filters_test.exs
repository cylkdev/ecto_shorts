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

  describe "convert_params_to_filter/3 offset shapes" do
    test "matches Ecto.Query for a root integer offset" do
      expected = offset(Post, ^5)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{offset: 5},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when offset overrides a previous offset" do
      expected = offset(Post, ^5)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{offset: 5},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding offset payload" do
      source =
        from(p in Post,
          join: u in assoc(p, :author),
          as: :author
        )

      expected = offset(source, [author: u], ^5)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                offset: 5
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding offset payload" do
      source =
        from(p in Post,
          join: u in assoc(p, :author)
        )

      expected = offset(source, [_, u], ^5)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                offset: 5
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 lock shapes" do
    test "matches Ecto.Query for a root string lock" do
      expected = lock(Post, "FOR UPDATE")

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{lock: "FOR UPDATE"},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root for_update alias lock" do
      expected = lock(Post, "FOR UPDATE")

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{lock: %{name: :for_update}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root map alias lock" do
      expected = lock(Post, "FOR SHARE")

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{lock: %{name: :for_share}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding lock payload" do
      source =
        from(p in Post,
          join: u in assoc(p, :author),
          as: :author
        )

      expected = lock(source, [author: u], "FOR UPDATE")

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                lock: "FOR UPDATE"
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding lock payload" do
      source =
        from(p in Post,
          join: u in assoc(p, :author)
        )

      expected = lock(source, [_, u], "FOR UPDATE")

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                lock: "FOR UPDATE"
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 exclude shapes" do
    test "matches Ecto.Query for excluding where" do
      source = from(p in Post, where: p.published == ^true)
      expected = exclude(source, :where)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :where},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding order_by" do
      source = from(p in Post, order_by: [desc: p.title])
      expected = exclude(source, :order_by)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :order_by},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding group_by" do
      source = from(p in Post, group_by: p.author_id)
      expected = exclude(source, :group_by)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :group_by},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding having" do
      source = from(p in Post, group_by: p.author_id, having: avg(p.views) > ^10)
      expected = exclude(source, :having)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :having},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding distinct" do
      source = from(p in Post, distinct: p.title)
      expected = exclude(source, :distinct)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :distinct},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding select" do
      source = from(p in Post, select: p.title)
      expected = exclude(source, :select)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :select},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding combinations" do
      other_query = from(p in Post, where: p.published == ^false)
      source = union(Post, ^other_query)
      expected = exclude(source, :combinations)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :combinations},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding with_ctes" do
      cte_query = from(p in Post, where: p.published == ^true)
      source = with_cte(Post, "published_posts", as: ^cte_query)
      expected = exclude(source, :with_ctes)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :with_ctes},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding limit" do
      source = limit(Post, ^10)
      expected = exclude(source, :limit)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :limit},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding offset" do
      source = offset(Post, ^5)
      expected = exclude(source, :offset)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :offset},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding lock" do
      source = lock(Post, "FOR UPDATE")
      expected = exclude(source, :lock)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :lock},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding preload" do
      source = from(p in Post, preload: :author)
      expected = exclude(source, :preload)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :preload},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding update" do
      updates = [set: [title: "After"]]
      source = update(Post, [], ^updates)
      expected = exclude(source, :update)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :update},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding windows" do
      source =
        from(p in Post,
          windows: [
            post_window: [
              partition_by: p.author_id,
              order_by: [desc: p.inserted_at]
            ]
          ]
        )

      expected = exclude(source, :windows)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :windows},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding multiple fields with a list payload" do
      source =
        Post
        |> limit(^10)
        |> offset(^5)

      expected = exclude(source, [:limit, :offset])

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: [:limit, :offset]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding joins" do
      source = from(p in Post, join: u in assoc(p, :author))
      expected = exclude(source, :join)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :join},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding inner_join" do
      source = from(p in Post, inner_join: u in assoc(p, :author))
      expected = exclude(source, :inner_join)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :inner_join},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding cross_join" do
      source = from(p in Post, cross_join: u in "users")
      expected = exclude(source, :cross_join)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :cross_join},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding cross_lateral_join" do
      source = from(p in Post, cross_lateral_join: u in fragment("SELECT 1 AS id"))
      expected = exclude(source, :cross_lateral_join)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :cross_lateral_join},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding left_join" do
      source = from(p in Post, left_join: u in assoc(p, :author))
      expected = exclude(source, :left_join)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :left_join},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding right_join" do
      source = from(p in Post, right_join: u in "users", on: true)
      expected = exclude(source, :right_join)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :right_join},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding full_join" do
      source = from(p in Post, full_join: u in "users", on: true)
      expected = exclude(source, :full_join)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :full_join},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding inner_lateral_join" do
      source = from(p in Post, inner_lateral_join: u in fragment("SELECT 1 AS id"), on: true)
      expected = exclude(source, :inner_lateral_join)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :inner_lateral_join},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding left_lateral_join" do
      source = from(p in Post, left_lateral_join: u in fragment("SELECT 1 AS id"), on: true)
      expected = exclude(source, :left_lateral_join)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: :left_lateral_join},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for excluding specific windows by name" do
      source =
        from(p in Post,
          windows: [
            post_window: [partition_by: p.author_id],
            author_window: [order_by: [desc: p.inserted_at]]
          ]
        )

      expected = exclude(source, {:windows, [:post_window]})

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{exclude: {:windows, [:post_window]}},
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
