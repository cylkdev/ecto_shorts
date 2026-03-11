defmodule EctoShorts.CommonFiltersTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  import ExUnit.CaptureLog
  alias EctoShorts.CommonFilters
  alias EctoShorts.Schema.Post

  import Ecto.Query

  describe "convert_params_to_filter/3 select shapes" do
    test "matches Ecto.Query for a root select field atom" do
      expected = from(p in Post, select: p.title)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: :title},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for selecting the full root binding" do
      expected = from(p in Post, select: p)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: true},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root select map field list" do
      expected = from(p in Post, select: map(p, [:id, :title]))

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: {:map, [:id, :title]}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root select map alias mapping" do
      expected =
        from(p in Post,
          select: %{post_id: p.id, post_title: p.title}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: {:map, %{post_id: :id, post_title: :title}}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root select struct projection" do
      expected = from(p in Post, select: struct(p, [:id, :title]))

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{select: {:struct, [:id, :title]}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding select field" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          select: a.first_name
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                select: :first_name
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding select map alias mapping" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          select: %{author_name: a.first_name}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                select: %{author_name: :first_name}
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when select overwrites an existing select" do
      source = from(p in Post, select: p.title)
      expected = from(p in Post, select: p.id)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              source,
              %{select: :id},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Replacing existing select expression before applying :select filter"
    end
  end

  describe "convert_params_to_filter/3 select_merge shapes" do
    test "matches Ecto.Query for a root select_merge keyword alias mapping" do
      source = from(p in Post, select: %{})

      expected =
        from(p in Post,
          select: %{},
          select_merge: %{post_title: p.title}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{select_merge: [post_title: :title]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root select_merge map alias mapping" do
      source = from(p in Post, select: %{})

      expected =
        from(p in Post,
          select: %{},
          select_merge: %{post_id: p.id, post_title: p.title}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{select_merge: %{post_id: :id, post_title: :title}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root select_merge map tuple alias mapping" do
      source = from(p in Post, select: %{})

      expected =
        from(p in Post,
          select: %{},
          select_merge: %{post_title: p.title}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{select_merge: {:map, %{post_title: :title}}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding select_merge alias mapping" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          select: %{}
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          select: %{},
          select_merge: %{author_name: a.first_name}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                select_merge: %{author_name: :first_name}
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding select_merge alias mapping" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          select: %{}
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          select: %{},
          select_merge: %{author_name: a.first_name}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                select_merge: %{author_name: :first_name}
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when select_merge merges onto an existing select" do
      source = from(p in Post, select: %{post_id: p.id})

      expected =
        from(p in Post,
          select: %{post_id: p.id},
          select_merge: %{post_title: p.title}
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{select_merge: %{post_title: :title}},
          []
        )

      assert_query(expected, actual)
    end
  end

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

  describe "convert_params_to_filter/3 join shapes" do
    test "matches Ecto.Query for an association join payload" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [association: [source: :author, as: :author]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for an association shorthand join payload" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{join: [author: [as: :author]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a schema join payload" do
      expected =
        from(p in Post,
          join: u in EctoShorts.Schema.User,
          as: :user,
          on: p.author_id == ^1
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              schema: [
                source: EctoShorts.Schema.User,
                as: :user,
                on: %{author_id: 1}
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a table join payload" do
      expected =
        from(p in Post,
          join: u in "users",
          as: :user,
          on: p.author_id == ^1
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              table: [
                source: "users",
                as: :user,
                on: %{author_id: 1}
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a query join payload" do
      user_query = from(u in EctoShorts.Schema.User, where: u.age > ^18)

      expected =
        from(p in Post,
          join: u in ^user_query,
          as: :user,
          on: p.author_id == ^1
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              query: [
                source: user_query,
                as: :user,
                on: %{author_id: 1}
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a subquery join payload built from params" do
      user_query = from(u in EctoShorts.Schema.User, where: u.age > ^18)

      expected =
        from(p in Post,
          join: u in subquery(user_query),
          as: :user,
          on: p.author_id == ^1
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              subquery: [
                source: [from: EctoShorts.Schema.User, age: {:>, 18}],
                as: :user,
                on: %{author_id: 1}
              ]
            ]
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding association join payload" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author,
          join: ap in assoc(a, :posts),
          as: :author_posts
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                join: [
                  association: [source: :posts, as: :author_posts]
                ]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a fragment join payload through the provider contract" do
      active_users =
        from(u in fragment("SELECT * FROM users WHERE age >= ?", ^18), select: u)

      expected =
        from(p in Post,
          join: u in ^active_users,
          as: :users,
          on: true
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: [name: :active_users, values: %{min_age: 18}],
                as: :users,
                on: true
              ]
            ]
          },
          query_provider: EctoShorts.TestQueryProvider
        )

      assert_query(expected, actual)
    end

    test "keeps the query unchanged when the fragment provider returns nil" do
      expected = from(p in Post)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{
            join: [
              fragment: [
                source: [
                  name: :active_users,
                  values: %{min_age: 18}
                ],
                as: :users,
                on: true
              ]
            ]
          },
          query_provider: EctoShorts.CommonFilters.QueryProviders.NoOp
        )

      assert_query(expected, actual)
    end

    test "keeps the query unchanged when the fragment provider returns an error" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{
                join: [
                  fragment: [
                    source: [
                      name: :error_fragment,
                      values: %{}
                    ],
                    as: :users,
                    on: true
                  ]
                ]
              },
              query_provider: EctoShorts.TestQueryProvider
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Join source callback returned error for key :error_fragment: :forced_error"
    end

    test "keeps the query unchanged when the fragment provider returns a raw source" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{
                join: [
                  fragment: [
                    source: [name: :legacy_active_users, values: %{min_age: 18}],
                    as: :users,
                    on: true
                  ]
                ]
              },
              query_provider: EctoShorts.TestQueryProvider
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected join source callback to return {:ok, source} | {:error, reason} | nil"
    end
  end

  describe "convert_params_to_filter/3 with_named_binding shapes" do
    test "matches Ecto.Query for the documented with_named_binding workflow" do
      expected =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when with_named_binding no-ops on an existing binding" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{with_named_binding: %{author: %{join: [association: [source: :author, as: :author]]}}},
          []
        )

      assert_query(source, actual)
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

  describe "convert_params_to_filter/3 last shapes" do
    test "matches Ecto.Query for a root integer last payload" do
      expected =
        Post
        |> exclude(:order_by)
        |> order_by([], desc: :id)
        |> limit(^2)
        |> subquery()
        |> order_by([], asc: :id)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{last: 2},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root keyword last payload" do
      expected =
        Post
        |> exclude(:order_by)
        |> order_by([], desc: :title)
        |> limit(^2)
        |> subquery()
        |> order_by([], asc: :title)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{last: [title: 2]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for an explicit id last payload" do
      expected =
        Post
        |> exclude(:order_by)
        |> order_by([], desc: :id)
        |> limit(^2)
        |> subquery()
        |> order_by([], asc: :id)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{last: %{id: 2}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for an explicit title last payload" do
      expected =
        Post
        |> exclude(:order_by)
        |> order_by([], desc: :title)
        |> limit(^2)
        |> subquery()
        |> order_by([], asc: :title)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{last: %{title: 2}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for terminal last wrapping after local filters" do
      expected =
        Post
        |> where([p], p.published == ^true)
        |> exclude(:order_by)
        |> order_by([], desc: :id)
        |> limit(^2)
        |> subquery()
        |> order_by([], asc: :id)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          [last: 2, published: true],
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when last replaces a previous order_by" do
      expected =
        Post
        |> order_by([], desc: :title)
        |> exclude(:order_by)
        |> order_by([], desc: :id)
        |> limit(^2)
        |> subquery()
        |> order_by([], asc: :id)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          [last: 2, order_by: :title],
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

  describe "convert_params_to_filter/3 first shapes" do
    test "matches Ecto.Query for a root integer first" do
      expected = limit(Post, ^10)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{first: 10},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a named binding first payload" do
      source =
        from(p in Post,
          join: u in assoc(p, :author),
          as: :author
        )

      expected = limit(source, [author: u], ^5)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                first: 5
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a positional binding first payload" do
      source =
        from(p in Post,
          join: u in assoc(p, :author)
        )

      expected = limit(source, [_, u], ^5)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                first: 5
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 put_query_prefix shapes" do
    test "matches Ecto.Query for a root string prefix" do
      expected = put_query_prefix(Post, "tenant_1")

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{put_query_prefix: "tenant_1"},
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 recursive_ctes shapes" do
    test "matches Ecto.Query for recursive_ctes true" do
      expected = recursive_ctes(Post, true)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{recursive_ctes: true},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for recursive_ctes false" do
      expected = recursive_ctes(Post, false)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{recursive_ctes: false},
          []
        )

      assert_query(expected, actual)
    end
  end

  describe "convert_params_to_filter/3 with_cte shapes" do
    test "matches Ecto.Query for with_cte with a prebuilt query" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = with_cte(Post, "published_posts", as: ^cte_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [published_posts: [as: cte_query]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for with_cte with a prebuilt subquery" do
      cte_query =
        Post
        |> where([p], p.published == ^true)
        |> subquery()

      expected = with_cte(Post, "published_posts", as: ^cte_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [published_posts: [as: cte_query]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for with_cte with filter params using the default source" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = with_cte(Post, "published_posts", as: ^cte_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [published_posts: [as: [published: true]]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for with_cte with filter params using an explicit from source" do
      cte_source = from(p in Post)
      cte_query = from(p in Post, where: p.published == ^true)
      expected = with_cte(Post, "published_posts", as: ^cte_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: [published_posts: [as: [from: cte_source, published: true]]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for with_cte with materialized false" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = with_cte(Post, "published_posts", as: ^cte_query, materialized: false)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_cte: %{published_posts: %{as: [published: true], materialized: false}}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when recursive_ctes is applied before with_cte" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = Post |> recursive_ctes(true) |> with_cte("published_posts", as: ^cte_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          [recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]],
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when with_cte is applied before recursive_ctes" do
      cte_query = from(p in Post, where: p.published == ^true)
      expected = Post |> with_cte("published_posts", as: ^cte_query) |> recursive_ctes(true)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          [with_cte: [published_posts: [as: cte_query]], recursive_ctes: true],
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for with_cte nested under a named binding" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      cte_query = from(p in Post, where: p.published == ^true)
      expected = with_cte(source, "published_posts", as: ^cte_query)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                with_cte: [published_posts: [as: cte_query]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for with_cte nested under a positional binding" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      cte_query = from(p in Post, where: p.published == ^true)
      expected = with_cte(source, "published_posts", as: ^cte_query)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                with_cte: [published_posts: [as: cte_query]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for ordered keyword-list CTE dependencies" do
      published_posts_query = from(p in Post, where: p.published == ^true)
      recent_posts_query = from(p in "published_posts", where: p.title == ^"recent")

      expected =
        Post
        |> with_cte("published_posts", as: ^published_posts_query)
        |> with_cte("recent_published_posts", as: ^recent_posts_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          [
            with_cte: [
              published_posts: [as: [published: true]],
              recent_published_posts: [as: recent_posts_query]
            ]
          ],
          []
        )

      assert_query(expected, actual)
    end

    test "keeps the query unchanged when with_cte params are invalid" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_cte: "invalid"},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :with_cte params to be a map or keyword list"
    end

    test "keeps the query unchanged when a with_cte :as payload is invalid" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_cte: [published_posts: [as: 123]]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~
               "Expected CTE :as query params for \"published_posts\" to be a query, subquery, or keyword/map payload"
    end
  end

  describe "convert_params_to_filter/3 windows shapes" do
    test "matches Ecto.Query for a root windows partition_by atom" do
      field_name = :author_id

      expected =
        windows(Post, [p], post_window: [partition_by: [field(p, ^field_name)], order_by: []])

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [partition_by: :author_id]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root windows partition_by list and order_by keyword list" do
      partition_field = :author_id
      order_field = :inserted_at

      expected =
        windows(Post, [p],
          post_window: [
            partition_by: [field(p, ^partition_field), field(p, ^:title)],
            order_by: [desc: field(p, ^order_field)]
          ]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [partition_by: [:author_id, :title], order_by: [desc: :inserted_at]]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a root windows frame dynamic expression" do
      frame_expr = dynamic([], fragment("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"))

      expected =
        windows(Post, [p], post_window: [partition_by: [], order_by: [], frame: ^frame_expr])

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{windows: [post_window: [frame: frame_expr]]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for windows nested under a named binding" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          as: :author
        )

      field_name = :first_name

      expected =
        windows(source, [author: a],
          author_window: [
            partition_by: [field(a, ^field_name)],
            order_by: [desc: field(a, ^field_name)]
          ]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              author: %{
                windows: [author_window: [partition_by: :first_name, order_by: [desc: :first_name]]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for windows nested under a positional binding" do
      source =
        from(p in Post,
          join: a in assoc(p, :author)
        )

      field_name = :first_name

      expected =
        windows(source, [p, a],
          author_window: [
            partition_by: [field(a, ^field_name)],
            order_by: [desc: field(a, ^field_name)]
          ]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                windows: [author_window: [partition_by: :first_name, order_by: [desc: :first_name]]]
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for multiple windows in one payload" do
      author_field = :author_id
      inserted_at_field = :inserted_at

      expected =
        Post
        |> windows([p],
          post_window: [partition_by: [field(p, ^author_field)], order_by: []]
        )
        |> windows([p],
          recent_window: [partition_by: [], order_by: [desc: field(p, ^inserted_at_field)]]
        )

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          [
            windows: [
              post_window: [partition_by: :author_id],
              recent_window: [order_by: [desc: :inserted_at]]
            ]
          ],
          []
        )

      assert_query(expected, actual)
    end

    test "keeps the query unchanged when windows params are invalid" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{windows: "invalid"},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :windows params to be a map or keyword list"
    end

    test "keeps the query unchanged when a window definition is invalid" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{windows: [post_window: "invalid"]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected window definition for :post_window to be a map or keyword list"
    end

    test "keeps the query unchanged when a window name is invalid" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              [windows: [{"post_window", [partition_by: :author_id]}]],
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected window name to be an atom"
    end

    test "keeps the query unchanged when frame is not a dynamic expression" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{windows: [post_window: [frame: "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"]]},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :frame for :post_window to be an Ecto dynamic expression"
    end
  end

  describe "convert_params_to_filter/3 with_ties shapes" do
    test "matches Ecto.Query for root with_ties true with existing limit and order_by" do
      expected =
        Post
        |> order_by([], desc: :inserted_at)
        |> limit(^1)
        |> with_ties(true)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          [order_by: {:desc, :inserted_at}, limit: 1, with_ties: true],
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for root with_ties false with existing limit and order_by" do
      expected =
        Post
        |> order_by([], desc: :inserted_at)
        |> limit(^1)
        |> with_ties(false)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          [order_by: {:desc, :inserted_at}, limit: 1, with_ties: false],
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when with_ties true adds the default limit" do
      expected =
        Post
        |> order_by([], desc: :inserted_at)
        |> limit(^1000)
        |> with_ties(true)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          [order_by: {:desc, :inserted_at}, with_ties: true],
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query when with_ties true adds default primary key ordering" do
      expected =
        Post
        |> limit(^1000)
        |> order_by([], asc: :id)
        |> with_ties(true)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_ties: true},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a keyword limit payload" do
      expected =
        Post
        |> limit(^10)
        |> order_by([], asc: :id)
        |> with_ties(true)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_ties: [limit: 10]},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a map limit payload" do
      expected =
        Post
        |> limit(^10)
        |> order_by([], asc: :id)
        |> with_ties(true)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{with_ties: %{limit: 10}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for named binding with_ties true" do
      source =
        from(p in Post,
          as: :post,
          order_by: [desc: p.inserted_at],
          limit: 1
        )

      expected = with_ties(source, [post: p], true)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            as: %{
              post: %{
                with_ties: true
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for positional binding with_ties true" do
      source =
        from(p in Post,
          join: a in assoc(p, :author),
          order_by: [desc: p.inserted_at],
          limit: 1
        )

      expected = with_ties(source, [p, a], true)

      actual =
        CommonFilters.convert_params_to_filter(
          source,
          %{
            at: %{
              2 => %{
                with_ties: true
              }
            }
          },
          []
        )

      assert_query(expected, actual)
    end

    test "keeps the query unchanged when with_ties payload is invalid" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_ties: "invalid"},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :with_ties value to be a boolean or keyword/map payload"
    end

    test "keeps the query unchanged when with_ties limit is invalid" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_ties: %{limit: "ten"}},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :with_ties :limit to be an integer or nil"
    end

    test "keeps the query unchanged when with_ties payload has unsupported keys" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{with_ties: %{foo: :bar}},
              []
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Expected :with_ties params to only include :limit"
    end
  end

  describe "convert_params_to_filter/3 set operation shapes" do
    test "matches Ecto.Query for union with filter params" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = union(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{union: %{published: false}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for union with a prebuilt query" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = union(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{union: other_query},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for union_all with filter params" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = union_all(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{union_all: %{published: false}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for union_all with a prebuilt query" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = union_all(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{union_all: other_query},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for intersect with filter params" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = intersect(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{intersect: %{published: false}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for intersect with a prebuilt query" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = intersect(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{intersect: other_query},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for intersect_all with filter params" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = intersect_all(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{intersect_all: %{published: false}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for intersect_all with a prebuilt query" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = intersect_all(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{intersect_all: other_query},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for except with filter params" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = except(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{except: %{published: false}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for except with a prebuilt query" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = except(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{except: other_query},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for except_all with filter params" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = except_all(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{except_all: %{published: false}},
          []
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for except_all with a prebuilt query" do
      other_query = from(p in Post, where: p.published == ^false)
      expected = except_all(Post, ^other_query)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{except_all: other_query},
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

    test "matches Ecto.Query for a provider-backed lock" do
      expected = lock(Post, "FOR UPDATE")

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{lock: %{name: :provider_for_update}},
          query_provider: EctoShorts.TestQueryProvider
        )

      assert_query(expected, actual)
    end

    test "matches Ecto.Query for a provider-backed lock with values" do
      expected = from(p in Post, lock: fragment("FOR UPDATE SKIP LOCKED"))

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{lock: %{name: :for_update_with_clause, values: %{clause: "SKIP LOCKED"}}},
          query_provider: EctoShorts.TestQueryProvider
        )

      assert_query(expected, actual)
    end

    test "keeps the query unchanged when the lock provider returns nil" do
      expected = from(p in Post)

      actual =
        CommonFilters.convert_params_to_filter(
          Post,
          %{lock: %{name: :provider_for_update}},
          query_provider: EctoShorts.CommonFilters.QueryProviders.NoOp
        )

      assert_query(expected, actual)
    end

    test "keeps the query unchanged when the lock provider returns an error" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{lock: %{name: :error_fragment}},
              query_provider: EctoShorts.TestQueryProvider
            )

          assert_query(expected, actual)
        end)

      assert log =~ "Lock expression callback returned error for :error_fragment: :forced_error"
    end

    test "keeps the query unchanged when the lock provider returns a raw expression" do
      expected = from(p in Post)

      log =
        capture_log(fn ->
          actual =
            CommonFilters.convert_params_to_filter(
              Post,
              %{lock: %{name: :legacy_for_update}},
              query_provider: EctoShorts.TestQueryProvider
            )

          assert_query(expected, actual)
        end)

      assert log =~
               "Expected lock expression callback to return {:ok, function} | {:error, reason} | nil"
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
