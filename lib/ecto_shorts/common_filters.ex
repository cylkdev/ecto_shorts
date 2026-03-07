defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Builds `Ecto.Query` structs from maps and keyword lists.

  ## Getting started

  The simplest way to build a query is to call `convert_params_to_filter/3` with
  parameters you want to filter by.

  For example:

      EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{published: true, limit: 10})
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.published == ^true, limit: ^10>

  Parameters can be a map or keyword list and in some cases a list of either.
  Maps and keyword lists are interchangeable. Keyword lists give control over
  the order of operations and are more flexible for complex queries.

  ## Schema filters

  Pass a map or keyword list where each key is a schema field, and the API will
  filter records that match those values:

      EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{title: "hello"})
      EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{published: true})
      EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{id: 1})

  Each key-value pair is a field filter. The key is the field on the schema to
  check, and the value is what it must match. When the query is built, each
  field filter becomes a `WHERE` condition in the resulting `Ecto.Query`.

  Schema filters are type-aware when a dynamic expression adapter is available.
  Without an adapter or when performing a schemaless query, all comparisons
  use a default scalar comparison.

  For example, if the field is an array type:

      %{published: [true, false]}

  the resulting expression is an equality check:

      p.published == ^[true, false]

  and if the field is a scalar type, the same parameter would be treated as a
  membership check:

      p.published in ^[true, false]

  ## Comparison operators

  Comparison operators control how a field is compared instead of the
  default equality check. Nest the operator as a key inside the field map:

      %{field_name: %{operator: value}}

  Examples:

      %{views: %{>: 10}}      # WHERE views > 10
      %{views: %{gte: 10}}    # WHERE views >= 10
      %{id: %{eq: 1}}         # WHERE id = 1
      %{views: %{ne: 10}}     # WHERE views != 10

  **Supported operators**

  Each operator has a symbol form and a word form:

    * `:==` / `:eq` - equal
    * `:!=` / `:ne` - not equal
    * `:>` / `:gt` - greater than
    * `:>=` / `:gte` - greater than or equal
    * `:<` / `:lt` - less than
    * `:<=` / `:lte` - less than or equal
    * `:in` - value is in a list

  Some comparison operators have different behaviours depending on the field type.

  When a `list` value is used with an equality operator:

  * If the field is a scalar type, the list is treated as a membership check.
  * If the field is an array type, the list is treated as an equality check.

  Example:

      %{published: %{==: [true, false]}}   # WHERE published IN (true, false)
      %{published: %{!=: [true, false]}}   # WHERE published NOT IN (true, false)

  This membership check can also be written explicitly using the `:in` operator:

      %{published: %{in: [true, false]}}   # WHERE published IN (true, false)
      %{id: %{in: [1, 2, 3]}}              # WHERE id IN (1, 2, 3)

  When a `nil` value is used with an equality operator, it is treated as a null check.

      %{published_at: %{==: nil}}          # WHERE published_at IS NULL
      %{published_at: %{!=: nil}}          # WHERE published_at IS NOT NULL

  ## Negation

  Use the `:not` key to flip the condition of an expression.

      %{views: %{not: %{>: 10}}}                 # WHERE NOT (views > 10)
      %{views: %{not: %{>=: 10}}}                # WHERE NOT (views >= 10)
      %{views: %{not: %{==: 10}}}                # WHERE views != 10
      %{published: %{not: %{in: [true, false]}}} # WHERE published NOT IN (...)
      %{views: %{not: %{avg: %{>: 10}}}}         # HAVING NOT (avg(views) > 10)

  If the condition would be true, `:not` makes it false. If the condition would
  be false, `:not` makes it true.

  ## String matching

  Use the `:like` and `:ilike` operators to perform pattern matching on strings.

      %{title: %{like: "hello"}}       # WHERE title LIKE '%hello%'
      %{title: %{ilike: "hello"}}      # WHERE title ILIKE '%hello%'

  The values for `:like` and `:ilike` are wrapped with `%` wildcards (e.g. "%hello%")
  which makes searches more relaxed. This allows the search to "match anything that
  contains this text anywhere," instead of an exact match.

  You can pass a list of strings to match on any of them:

      %{title: %{like: ["hello", "world"]}}    # WHERE title LIKE ANY(...)

  You can negate the entire condition with `:not`:

      %{title: %{not: %{like: "hello"}}}    # WHERE NOT (title LIKE '%hello%')

  ## String transformations

  The `:lower` and `:upper` keys transform the field value before comparing:

      %{title: %{==: %{lower: "hello"}}}    # WHERE lower(title) = 'hello'
      %{title: %{!=: %{upper: "HELLO"}}}    # WHERE upper(title) != 'HELLO'

  ## Logical operators

  The `:and` and `:or` operators combine multiple conditions. They work
  at two levels: field-level and top-level.

  ### Field-level `:and` and `:or`

  At the field level, `:and` and `:or` combine multiple conditions on the
  same field.

  Find posts where views are between `100` and `500` (both must match):

      %{
        views: %{
          and: [
            %{gte: 100},  # views >= 100
            %{lte: 500}   # views <= 500
          ]
        }
      }

      # WHERE views >= 100 AND views <= 500

  Find posts where status is either "draft" or "scheduled" (either can match):

      %{
        status: %{
          or: [
            %{eq: "draft"},
            %{eq: "scheduled"}
          ]
        }
      }

      # WHERE status = 'draft' OR status = 'scheduled'

  ### Top-level `:and` and `:or`

  At the top level, `:and` and `:or` combine groups of field filters.
  Wrap each group in a list:

      %{
        or: [
          [published: true, views: 20],
          [published: false, views: 10]
        ]
      }
      # WHERE (published = true AND views = 20) OR (published = false AND views = 10)

      %{
        and:[
          [published: true, views: 20],
          [title: "hello", views: 15]
        ]
      }
      # WHERE (published = true AND views = 20) AND (title = 'hello' AND views = 15)

  You can mix top-level `:or` with regular field filters:

      [
        title: "test",
        or: [
          [published: true, views: 20],
          [published: false, views: 10]
        ]
      ]
      # WHERE title = 'test' AND ((published = true AND views = 20) OR (published = false AND views = 10))

  ## The `:where` and `:or_where` keys

  Use `:where` and `:or_where` for explicit control over how conditions are grouped.
  The `:where` clauses are processed before the `:or_where` clause, regardless of
  key order. By default, keys that do not have a special meaning are treated as
  implicit `:where` filters, so these two are equivalent:

      %{published: true, or_where: %{published: false}}
      %{where: %{published: true}, or_where: %{published: false}}
      # Both produce: WHERE published = true OR published = false

  Explicit `:where` accepts a map or keyword list of filters:

      %{where: %{published: true, views: 10}}
      %{where: [published: true, views: 10]}

  ## Aggregate functions

  Aggregate keys (`:avg`, `:count`, `:sum`, `:max`, `:min`) wrap a comparison operator.
  They are typically used inside a `:having` clause:

      %{group_by: :author_id, having: %{views: %{avg: %{>: 10}}}}
      %{group_by: :author_id, having: %{views: %{count: %{>: 10}}}}

  If an operator is not specified after using an aggregate key, it's treated as an
  equality check:

      %{views: %{avg: 10}}   # same as %{views: %{avg: %{==: 10}}}

  Negate an aggregate by wrapping it with `:not`:

      %{views: %{not: %{avg: %{>: 10}}}}   # HAVING NOT (avg(views) > 10)

  ## Arithmetic expressions

  Arithmetic operators (`:+`, `:-`, `:*`, `:/`) take a two-element list and
  appear inside comparison operators:

      %{views: %{>: %{+: [:views, 10]}}}    # WHERE views > views + 10
      %{views: %{>: %{*: [:views, 2]}}}     # WHERE views > views * 2

  ## Datetime expressions

  Datetime expressions let the database compute time-based values at query
  time (for example "now minus 1 day" or "this field plus 1 day"). The
  comparison is anchored to the database clock, avoiding drift between
  application and database time.

  Use the key `:datetime` or `:date` inside a comparison operator:

      %{
        inserted_at: %{
          >=: %{
            datetime: %{
              add: %{
                field: :inserted_at,
                count: 1,
                interval: "day"
              }
            }
          }
        }
      }
      # WHERE inserted_at >= datetime_add(inserted_at, 1, 'day')

      %{
        inserted_at: %{
          >: %{
            datetime: %{
              ago: %{
                count: 1,
                interval: "day"
              }
            }
          }
        }
      }
      # WHERE inserted_at > (now - 1 day)

      %{
        inserted_at: %{
          >: %{
            datetime: %{
              from_now: %{
                count: 1,
                interval: "day"
              }
            }
          }
        }
      }
      # WHERE inserted_at > (now + 1 day)

  ### :add

  Use `:add` when the date/time you want to compare to is based on another field
  (often the same field).

      %{
        inserted_at: %{
          >=: %{
            datetime: %{
              add: %{field: :inserted_at, count: 1, interval: "day"}
            }
          }
        }
      }
      # WHERE inserted_at >= datetime_add(inserted_at, 1, 'day')

  * `field` - the date/time field used as the base value.
  * `count` - how many units to add (use a negative value to subtract).
  * `interval` - the unit as a string (for example "day", "hour", "minute").

  ### :ago

  `:ago` computes "current time minus X".

      %{
        inserted_at: %{
          >: %{
            datetime: %{
              ago: %{count: 1, interval: "day"}
            }
          }
        }
      }
      # WHERE inserted_at > (now - 1 day)

  ### :from_now

  `:from_now` computes "current time plus X".

      %{
        inserted_at: %{
          >: %{
            datetime: %{
              from_now: %{count: 1, interval: "day"}
            }
          }
        }
      }
      # WHERE inserted_at > (now + 1 day)

  ### Picking `:datetime` vs `:date`

  * `:datetime` - timestamp comparison (includes time-of-day).
  * `:date` - date-only comparison (calendar dates).

  In both cases the value is treated as a date/time expression so the query can
  compute it instead of treating it like a literal.

  ## Subquery and set comparisons

  The `:all` and `:any` keys wrap a comparison operator and
  a subquery expression:

      subquery_expr = from(c in "comments", select: c.post_id)

      %{id: %{all: %{>: subquery_expr}}}    # WHERE id > ALL(subquery)
      %{id: %{any: %{>: subquery_expr}}}    # WHERE id > ANY(subquery)

  When no comparison operator is provided, they default to equality:

      %{id: %{all: subquery_expr}}    # WHERE id = ALL(subquery)

  You can pass a `:from` payload instead of a pre-built subquery:

      %{id: %{all: %{from: Post, id: 1}}}

  ## Array fields

  Array fields (like `{:array, :string}`) support special operators
  for array membership and equality:

      %{tags: "elixir"}                    # WHERE 'elixir' = ANY(tags)
      %{tags: %{in: ["elixir"]}}           # WHERE tags @> ARRAY['elixir']
      %{tags: %{==: ["elixir", "erlang"]}} # WHERE tags = ARRAY['elixir', 'erlang']
      %{tags: %{!=: ["elixir"]}}           # WHERE tags != ARRAY['elixir']

  Use `:all` with `:in` to require all values:

      %{tags: %{all: %{in: ["elixir", "erlang"]}}}   # WHERE tags @> ARRAY['elixir', 'erlang']

  Negate with `:not`:

      %{tags: %{not: %{in: ["elixir"]}}}
      %{tags: %{not: %{all: %{in: ["elixir", "erlang"]}}}}

  Array fields also support comparison, string matching, string
  transforms, and aggregate operators - all following the same
  nesting rules as scalar fields.

  > NOTE: the `:all` operator is overloaded. Wrapping a comparison
  > (e.g. `%{all: %{>: subquery}}`) it means subquery set comparison.
  > With `:in` on an array field (e.g. `%{tags: %{all: %{in: [...]}}}`)
  > it means "contains all values".

  ## Query operations

  Query operation keys map directly to Ecto query operations. Avoid
  using schema field names that collide with these keys.

  | Key(s)                                                                           | Purpose                        | Section                                                      |
  |----------------------------------------------------------------------------------|--------------------------------|--------------------------------------------------------------|
  | `:select`, `:select_merge`                                                       | Choose which columns to return | [Select](#module-select)                                     |
  | `:order_by`, `:prepend_order_by`, `:reverse_order`                               | Sort results                   | [Ordering](#module-ordering)                                 |
  | `:group_by`, `:having`, `:or_having`                                             | Group and filter aggregates    | [Grouping and having](#module-grouping-and-having)           |
  | `:limit`, `:offset`, `:first`, `:last`                                           | Paginate results               | [Pagination](#module-pagination)                             |
  | `:distinct`                                                                      | Remove duplicate rows          | [Distinct](#module-distinct)                                 |
  | `:join`                                                                          | Join other tables              | [Joins](#module-joins)                                       |
  | `:preload`                                                                       | Preload associations           | [Preload](#module-preload)                                   |
  | `:union`, `:union_all`, `:except`, `:except_all`, `:intersect`, `:intersect_all` | Combine queries                | [Set operations](#module-set-operations)                     |
  | `:subquery`                                                                      | Wrap query as a subquery       | [Subquery](#module-subquery)                                 |
  | `:lock`                                                                          | Row-level locking              | [Lock](#module-lock)                                         |
  | `:recursive_ctes`, `:with_cte`                                                   | Common table expressions       | [Common table expressions](#module-common-table-expressions) |
  | `:with_named_binding`                                                            | Add named bindings             | [Named bindings](#module-named-bindings)                     |
  | `:windows`                                                                       | Window functions               | [Windows](#module-windows)                                   |
  | `:with_ties`                                                                     | Include tied rows              | [With ties](#module-with-ties)                               |
  | `:update`                                                                        | Bulk update expressions        | [Update](#module-update)                                     |
  | `:exclude`                                                                       | Remove a clause from the query | [Exclude](#module-exclude)                                   |
  | `:put_query_prefix`                                                              | Set the query prefix           | [Query prefix](#module-query-prefix)                         |

  ### Select

      %{select: true}                          # SELECT p.*
      %{select: :id}                           # SELECT p.id
      %{select: [:id, :title]}                 # SELECT p.id, p.title
      %{select: %{map: [:id, :title]}}         # SELECT map(p, [:id, :title])
      %{select: %{map: %{custom_id: :id}}}     # SELECT p.id AS custom_id
      %{select: %{struct: [:id]}}              # SELECT struct(p, [:id])

      %{select_merge: %{map: %{custom_id: :id}}}

  Combine both:

      [select: %{map: [:id]}, select_merge: %{map: %{post_title: :title}}]

  ### Ordering

  > #### Default direction {: .info}
  >
  > When you pass a bare field atom (e.g. `:title`), the default sort direction
  > is **descending** (`:desc`). To sort ascending, use `[asc: :title]`.

      %{order_by: :title}                           # ORDER BY title DESC
      %{order_by: [desc: :title]}                   # ORDER BY title DESC
      %{order_by: [asc: :title, desc: :id]}         # ORDER BY title ASC, id DESC
      %{prepend_order_by: :title}
      %{reverse_order: true}

  ### Grouping and having

      %{group_by: :author_id}
      %{group_by: [:author_id, :published]}

  The `:having` and `:or_having` keys accept the same filter language
  used for `:where`:

      %{having: %{views: %{avg: %{>: 10}}}}
      %{having: dynamic([p], p.views > ^10)}
      %{or_having: %{views: %{<: 5}}}

  ### Pagination

      %{limit: 10}
      %{offset: 5}
      %{first: 10}            # delegates to :limit

  The `:last` key wraps the entire query in a subquery that reverses
  the order and takes the last N rows:

      %{last: 2}                 # last 2 rows by primary key
      %{last: %{title: 2}}      # last 2 rows ordered by :title

  ### Distinct

      %{distinct: true}
      %{distinct: :title}
      %{distinct: [desc: :title]}

  ### Joins

  The `:join` key supports multiple join sources:

      %{join: [author: [as: :author]]}                                          # association
      %{join: [schema: [source: User, as: :user_join, on: true]]}               # schema
      %{join: [table: [source: "users", as: :users_table, on: true]]}           # table
      %{join: [query: [source: user_query, as: :adult_users, on: true]]}        # query
      %{join: [subquery: [source: user_query, as: :name, on: true]]}            # subquery
      %{join: [fragment: [source: %{name: :active_users, values: [min_age: 21]}, as: :active_users, on: true]]}

  ### Association shorthand

  When a key matches an association name, you can filter on the
  associated schema using a keyword list. This creates a join and
  applies the filters to the joined binding automatically:

      %{author: [as: :author, first_name: "John"]}
      %{author: [first_name: "John"]}
      %{author: [as: :author, type: :left, first_name: "John"]}

  The optional keys `:as`, `:on`, and `:type` configure the join.
  All remaining keys are treated as field filters on the associated schema.

  Both maps and keyword lists are accepted:

      %{author: [as: :author, first_name: "John"]}
      %{author: %{as: :author, first_name: "John"}}

  ### Preload

      %{preload: :author}
      %{preload: [:author]}
      %{preload: [author: [:posts]]}
      %{preload: [bind: [as: [example: :author]]]}

  ### Set operations

      %{union: %{published: false}}
      %{union_all: %{published: false}}
      %{except: %{published: false}}
      %{intersect: %{published: false}}

  You can also pass a pre-built query:

      %{except: from(p in Post, where: p.published == ^false)}

  ### Subquery

  The `:subquery` key wraps the entire query (with all filters
  applied) as a subquery:

      %{subquery: %{id: 2}}

  ### Lock

      %{lock: fn query -> from(p in query, lock: "FOR UPDATE") end}
      %{lock: %{name: :for_share}}
      %{lock: %{name: :for_update_with_clause, values: [clause: "SKIP LOCKED"]}}

  ### Common table expressions

      %{recursive_ctes: true}
      %{with_cte: [published_posts: [as: cte_query]]}
      %{with_cte: [published_posts: [as: %{from: %{query: Post, published: true}}]]}

  ### Named bindings

      %{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}]}

  ### Windows

      %{windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]}

  ### With ties

      %{with_ties: true}
      %{with_ties: %{bind: %{as: %{post: true}}}}
      %{with_ties: %{bind: %{at: %{1 => true}}}}

  ### Update

      %{update: [set: [title: "After"], inc: [views: 1]]}
      %{update: %{set: %{title: "After"}}}

  ### Exclude

      %{exclude: :order_by}
      %{exclude: [:order_by, :limit]}

  ### Query prefix

      %{put_query_prefix: "tenant_a"}

  ## Binding selectors

  The `:bind` key targets a specific binding in a query. Each bind entry
  is a flat map with an `:as` or `:at` key identifying the target and
  all other keys treated as filters or query operations.

  Named bindings use `:as`:

      # Given a query: from(p in Post, as: :post)
      %{bind: %{as: :post, published: true}}

  Positional bindings use `:at`:

      %{bind: %{at: 1, published: true}}

  The `:at` key also accepts `:first` and `:last`:

      %{bind: %{at: :first, published: true}}      # targets the root from binding
      %{bind: %{at: :last, first_name: "John"}}     # targets the last join, or from if no joins

  Target multiple bindings with a list:

      %{bind: [%{as: :post, published: true}, %{as: :author, first_name: "John"}]}

  Binding selectors work with query operations too:

      %{bind: %{as: :author, order_by: %{asc: :first_name}}}
      %{bind: %{at: 2, group_by: :first_name}}
      %{bind: %{at: :last, order_by: %{asc: :first_name}}}

  ## Convenience filters

  These built-in keys provide common shortcuts:

      %{ids: [1, 2, 3]}                          # WHERE id IN (1, 2, 3)
      %{after: 10}                               # WHERE id > 10
      %{before: 10}                              # WHERE id < 10
      %{start_date: ~U[2026-01-01 00:00:00Z]}    # WHERE inserted_at >= value
      %{end_date: ~U[2026-12-31 23:59:59Z]}      # WHERE inserted_at <= value

  ## Dynamic expressions

  The `:dynamic` key accepts a raw dynamic struct for expressions
  that cannot be represented with the data-driven filter keys above.
  Prefer filter keys when possible - they are composable and produce
  predictable behaviour.

      %{dynamic: dynamic([p], p.views > ^10)}
      %{where: %{dynamic: dynamic([p], p.published == ^true)}}
      %{or_where: %{dynamic: dynamic([p], p.views > ^100)}}

  ## Exists

  The `:exists` key checks for the existence of rows in a subquery.
  It accepts a pre-built subquery expression or a filter payload:

      %{where: %{exists: subquery_expr}}
      %{where: %{exists: %{not: subquery_expr}}}              # NOT EXISTS(...)
      %{where: %{exists: %{from: Post, id: 1}}}
      %{where: %{exists: %{not: %{from: Post, id: 1}}}}
      %{where: %{exists: %{first_name: "John"}}}              # uses parent source

  Wrapping the value with `:not` produces a `NOT EXISTS(...)` condition.

  When the `:from` key is present, its value identifies the source for the
  subquery. When `:from` is absent, the parent source is used.

  When an `:exists` payload does not include an explicit `:select`,
  `select: true` is applied automatically. This keeps `EXISTS`
  subqueries valid without requiring a separate pre-build step.

  ## The :from key

  The `:from` key lets you specify the queryable source inside the
  params instead of passing it as the first argument. This is useful
  when the entire query description comes from data (for example an
  HTTP request body) and is not known at compile time.

  The `:from` value is the source directly - a schema module, table
  name string, or `{table, schema}` tuple. All sibling keys are
  treated as filters:

      %{from: Post, id: 1}
      %{from: "posts", id: 1, select: [:id]}

  For example, these two calls produce the same query:

      # Passing source as the first argument and filters in the params:
      EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{id: 1})

      # Passing source inside the params:
      EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{from: EctoShorts.Schema.Post, id: 1})

  When both a first argument and a `:from` key are present, the `:from`
  value wins for field resolution.

  The `:from` key also appears inside nested payloads (`:all`, `:any`,
  `:exists`, `:with_cte`) where it identifies the source for an inline
  subquery to build.

  ## Schemaless queries

  A schemaless query is a query against a bare table name string with no
  Ecto schema module. This is useful when you want to query a table that
  has no corresponding schema, or when you want to work with raw table
  names directly. See the Ecto guide on
  [schemaless queries](https://hexdocs.pm/ecto/schemaless-queries.html)
  for background.

  ### Passing a schemaless source

  There are three ways to perform a schemaless query.

  **1. Direct table name**

  Pass the table name string or a `{table_name, nil}` tuple as the first
  argument:

      EctoShorts.CommonFilters.convert_params_to_filter("posts", %{select: [:id, :title], published: true})
      # from p0 in "posts", where: p0.published == ^true, select: [:id, :title]

      EctoShorts.CommonFilters.convert_params_to_filter({"posts", nil}, %{select: [:id, :title], published: true})
      # same result

  **2. Via the `:from` key**

  Set the `:from` key inside the params. The value is the source
  directly. When `:from` is present it overrides the first argument
  for field resolution:

      EctoShorts.CommonFilters.convert_params_to_filter(EctoShorts.Schema.Post, %{from: "posts", id: 1})

  ### `:select` and schemaless queries

  Ecto requires an explicit `:select` clause to execute a query on a bare
  table string because there is no schema to infer which columns to return.

  When the resolved source is schemaless (a bare table string or a
  `{table, nil}` tuple), the system adds `select: true` automatically if
  you did not include a `:select`. This applies to all schemaless sources
  regardless of whether the source was passed directly or resolved through
  the `:from` key:

      # Direct table name - select: true is added automatically:
      EctoShorts.CommonFilters.convert_params_to_filter("posts", %{published: true})

      # Via :from - select: true is also added automatically:
      EctoShorts.CommonFilters.convert_params_to_filter(Post, %{from: "posts"})

  You can override the default by passing your own `:select`:

      EctoShorts.CommonFilters.convert_params_to_filter("posts", %{select: [:id, :title]})

  ### No field validation

  Without a schema there is no list of known fields. Any atom key you pass
  is accepted as a field name and forwarded to the query. This means typos
  in field names will not be caught at query-build time - they will only
  surface as database errors at execution time.

  ### No type-aware filtering

  Schema-backed queries use schema reflection to distinguish array fields
  from scalar fields, which changes how list values are compared (equality
  vs. membership). Without a schema, all comparisons use the default
  scalar behaviour. For example, `%{tags: ["a", "b"]}` produces
  `tags IN ('a', 'b')` instead of the array-equality check you would get
  with a schema that declares `tags` as `{:array, :string}`.

  ## Nested Expressions

  CommonFilters is a data mapper. Instead of composing Ecto queries by
  hand, you describe what you want with plain maps or keyword lists and
  CommonFilters translates that data into the correct Ecto calls. It
  handles the internal specifics of Ecto so you do not have to.

  The filtering language is built around a slot chain. Each filter you
  write is a chain of nested params, and each nesting level fills one
  slot. The slots are processed from outermost to innermost:

      field -> negation -> quantifier -> aggregate -> operator -> value expression

  Not every slot is required. When you leave a slot out, we fill in a
  sensible default. The examples below show how adding one slot at a
  time builds up more complex filters:

  **Field only** - equality check (operator defaults to `==`):

      %{title: "hello"}                  # WHERE title = 'hello'

  **Field + operator** - explicit comparison:

      %{views: %{>: 10}}                 # WHERE views > 10

  **Field + negation + operator** - negated comparison:

      %{views: %{not: %{>: 10}}}         # WHERE NOT (views > 10)

  **Field + aggregate + operator** - aggregate comparison:

      %{views: %{avg: %{>: 10}}}         # HAVING avg(views) > 10

  **All five slots** - the full chain in one filter:

      %{views: %{not: %{avg: %{>: %{+: [:views, 10]}}}}}

  Reading from the outside in:

  1. `:views` is the **field**
  2. `:not` is the **negation**
  3. `:avg` is the **aggregate**
  4. `:>` is the **operator**
  5. `%{+: [:views, 10]}` is the **value expression**

  ## Processing order

  Due to internal specifics of how Ecto.Query resolves operations, certain keys
  must be processed in a specific order to ensure queries are composed correctly.
  Keys are sorted into a fixed order:

    1. Explicit `:where` keys.
    2. Implicit field filters (e.g. `published: true`) and query operations
       like `:limit`, `:order_by`, `:join`, `:preload`.
    3. Explicit `:or_where` keys.
    4. Terminal filters like `:last` and `:subquery`.

  A terminal filter is a filter that runs at the end of processing because it
  changes or wraps the entire query, not just one condition.

  Input order does not matter. `%{last: 2, published: true, limit: 10}`
  produces the same query regardless of key order.

  ## Error handling

  This API follows a warn-and-skip model.

  Invalid filter data never raises an exception:

    * Unknown keys that are not schema fields or reserved keys are
      logged as warnings and skipped

    * Invalid payloads (wrong types, malformed maps) are logged as
      warnings and the failing operation is skipped

    * The rest of the query continues building normally - only the
      invalid entry is dropped

  This makes the module safe to use with user-provided data where
  some keys may be unexpected.
  """

  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonQuery

  alias EctoShorts.CommonFilters.{
    BindingParams,
    Distinct,
    Filter,
    GroupBy,
    Having,
    Join,
    OrderBy,
    Preload,
    Select,
    SubQuery,
    WithCte,
    WithNamedBinding,
    WithTies,
    Windows,
    Update
  }

  alias EctoShorts.Config
  alias EctoShorts.Logger
  alias EctoShorts.SchemaHelpers

  @logger_prefix "EctoShorts.CommonFilters"
  @binding_params_prefix "EctoShorts.CommonFilters.BindingParams"

  @binding_selector_key :bind
  @binding_modes [:as, :at]
  @default_binding_selector {:as, nil}

  @where :where
  @where_filters [:where, :or_where]

  @subquery_operators [:exists]

  @query_builder_modules %{
    distinct: Distinct,
    group_by: GroupBy,
    having: Having,
    or_having: Having,
    join: Join,
    order_by: OrderBy,
    prepend_order_by: OrderBy,
    preload: Preload,
    select: Select,
    select_merge: Select,
    update: Update,
    windows: Windows,
    with_cte: WithCte,
    with_named_binding: WithNamedBinding,
    with_ties: WithTies
  }

  @query_filters [
    :distinct,
    :except,
    :except_all,
    :exclude,
    :first,
    :group_by,
    :having,
    :or_having,
    :intersect,
    :intersect_all,
    :union,
    :union_all,
    :join,
    :last,
    :lock,
    :limit,
    :offset,
    :put_query_prefix,
    :order_by,
    :windows,
    :reverse_order,
    :prepend_order_by,
    :preload,
    :recursive_ctes,
    :select,
    :select_merge,
    :subquery,
    :with_cte,
    :with_named_binding,
    :with_ties,
    :update
  ]

  @doc """
  Converts filter params into an `Ecto.Query`.

  ## Arguments

    * `params` - a map or keyword list of filters and query operations.
    * `opts` - optional keyword list forwarded to all sub-query builders.

  Schema field keys become `WHERE` conditions. Query operation keys
  (`:limit`, `:order_by`, `:join`, etc.) become the corresponding Ecto
  operations. Unknown keys log a warning and are skipped. Invalid
  payloads log a warning and are skipped - the rest of the query
  continues building.

  See the [moduledoc](`m:EctoShorts.CommonFilters`) for the full
  filter language reference.

  ## Options

    * `:repo` (default: `EctoShorts.Config.repo/0`) - the `Ecto.Repo`
      module used to resolve the dynamic expression adapter.
    * `:dynamic_adapter` - a module implementing
      `EctoShorts.Dynamic`. Defaults to the adapter resolved
      from `:repo`.

  ## Examples

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{title: "Hello"}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.title == ^"Hello">

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{published: true, or_where: %{published: false}}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post,
        where: p0.published == ^true or p0.published == ^false>

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{limit: 5, order_by: %{desc: :inserted_at}}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post,
        order_by: [desc: p0.inserted_at], limit: ^5>

  See also `EctoShorts.Actions.all/3`, `EctoShorts.Dynamics`, and `EctoShorts.CommonFilters.Having`.
  """
  @spec convert_params_to_filter(
          source :: module() | {binary(), module()} | Ecto.Query.t(),
          params :: map() | keyword(),
          opts :: keyword()
        ) :: Ecto.Query.t()
  def convert_params_to_filter(source, params, opts \\ [])

  def convert_params_to_filter(source, params, opts) when is_map(params) do
    convert_params_to_filter(source, Map.to_list(params), opts)
  end

  def convert_params_to_filter(source, params, opts) do
    if Keyword.keyword?(params) do
      schema_source = CommonSchema.normalize_source(source)

      base_params =
        if not source_has_schema?(schema_source) and not Keyword.has_key?(params, :select) do
          Keyword.put(params, :select, true)
        else
          params
        end

      reduce_filter_params(schema_source, CommonSchema.to_query(source), base_params, opts)
    else
      Enum.reduce(params, CommonSchema.to_query(source), fn entry, query_acc ->
        reduce_filter_params(source, query_acc, entry, opts)
      end)
    end
  end

  defp reduce_filter_params(schema_source, query, params, opts) do
    normalized_params = normalize_filter_params(params)

    if is_map(normalized_params) or Keyword.keyword?(normalized_params) do
      apply_filters(
        schema_source,
        query,
        @default_binding_selector,
        @where,
        normalized_params,
        opts
      )
    else
      Logger.warning(@logger_prefix, "Expected params to be a map or list, got: #{inspect(params)}")
      query
    end
  end

  defp apply_filters(
         schema_source,
         query,
         _binding_selector,
         filter_op,
         {@binding_selector_key, bind_params},
         opts
       ) do
    bind_params
    |> BindingParams.normalize_bind_params(query)
    |> Enum.reduce(query, fn
      {{:as, nil}, []}, q ->
        q

      {{:as, nil}, _value}, q ->
        Logger.warning(
          @binding_params_prefix,
          "Expected :as value to be a non-nil atom, got: nil"
        )

        q

      {{:as, bind_alias}, filters}, q when is_atom(bind_alias) ->
        if CommonQuery.get_query_binding_source(q, bind_alias) do
          apply_filters(schema_source, q, {:as, bind_alias}, filter_op, filters, opts)
        else
          Logger.warning(
            @binding_params_prefix,
            "Named binding #{inspect(bind_alias)} does not exist in the query"
          )

          q
        end

      {{:as, bind_alias}, _filters}, q ->
        Logger.warning(
          @binding_params_prefix,
          "Expected :as value to be a non-nil atom, got: #{inspect(bind_alias)}"
        )

        q

      {{:at, bind_index}, filters}, q when is_integer(bind_index) ->
        max = Config.max_binding_positions()
        actual_count = CommonQuery.query_binding_count(q)

        cond do
          bind_index > max ->
            Logger.warning(
              @binding_params_prefix,
              "Binding position #{bind_index} exceeds the configured :max_binding_positions (#{max}). " <>
                "Increase :max_binding_positions in your config and recompile to support more positional bindings."
            )

            q

          bind_index < 1 ->
            Logger.warning(
              @binding_params_prefix,
              "Binding position must be >= 1, got: #{bind_index}"
            )

            q

          bind_index > actual_count ->
            Logger.warning(
              @binding_params_prefix,
              "Binding position #{bind_index} exceeds the number of bindings in the query (#{actual_count}). " <>
                "The query has #{actual_count} binding(s). Add a join to create more bindings."
            )

            q

          true ->
            apply_filters(
              schema_source,
              q,
              {:at, bind_index},
              filter_op,
              filters,
              opts
            )
        end
    end)
  end

  defp apply_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       ) do
    cond do
      key in @where_filters ->
        if (is_map(value) and not is_struct(value)) or is_list(value) do
          Enum.reduce(value, query, fn entry, query_acc ->
            build_schema_filters(schema_source, query_acc, binding_selector, key, entry, opts)
          end)
        else
          Logger.warning(
            @logger_prefix,
            "Expected params for #{filter_op} to be a map or keyword list, got: #{inspect(value)}"
          )

          query
        end

      key in [:and, :or] ->
        build_query(schema_source, query, binding_selector, filter_op, {key, value}, opts)

      key in @query_filters ->
        build_query(schema_source, query, binding_selector, key, value, opts)

      true ->
        case CommonSchema.get_schema_reflection(schema_source, :associations) do
          nil ->
            build_schema_filters(
              schema_source,
              query,
              binding_selector,
              filter_op,
              {key, value},
              opts
            )

          assocs ->
            if key in assocs do
              apply_join(
                schema_source,
                query,
                binding_selector,
                filter_op,
                key,
                value,
                opts
              )
            else
              build_schema_filters(
                schema_source,
                query,
                binding_selector,
                filter_op,
                {key, value},
                opts
              )
            end
        end
    end
  end

  defp apply_filters(schema_source, query, binding_selector, filter_op, params, opts) do
    cond do
      is_map(params) and not is_struct(params) ->
        apply_filters(
          schema_source,
          query,
          binding_selector,
          filter_op,
          Map.to_list(params),
          opts
        )

      Keyword.keyword?(params) ->
        Enum.reduce(params, query, fn {key, value}, query_acc ->
          apply_filters(
            schema_source,
            query_acc,
            binding_selector,
            filter_op,
            {key, value},
            opts
          )
        end)

      is_list(params) ->
        build_query(schema_source, query, binding_selector, filter_op, params, opts)

      true ->
        build_query(schema_source, query, binding_selector, filter_op, params, opts)
    end
  end

  defp apply_join(
         schema_source,
         query,
         binding_selector,
         filter,
         assoc_key,
         params,
         opts
       ) do
    params =
      if is_map(params) do
        Map.to_list(params)
      else
        params
      end

    if Keyword.keyword?(params) do
      assoc_schema =
        case schema_source do
          {_, parent_schema} -> SchemaHelpers.get_related_schema(parent_schema, assoc_key)
          parent_schema -> SchemaHelpers.get_related_schema(parent_schema, assoc_key)
        end

      joined_query =
        build_query(
          schema_source,
          query,
          binding_selector,
          :join,
          [{assoc_key, params}],
          opts
        )

      {join_binding_mode, join_binding_target} =
        if Keyword.has_key?(params, :as) do
          {:as, Keyword.fetch!(params, :as)}
        else
          {:at, CommonQuery.query_binding_count(joined_query)}
        end

      apply_filters(
        assoc_schema,
        joined_query,
        {join_binding_mode, join_binding_target},
        filter,
        Keyword.drop(params, [:as, :on, :type]),
        opts
      )
    else
      Logger.warning(
        @logger_prefix,
        "Expected association params for #{inspect(assoc_key)} to be a map or keyword list, got: #{inspect(params)}"
      )

      query
    end
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       ) do
    cond do
      is_map(value) and not is_struct(value) ->
        apply_filters(
          schema_source,
          query,
          binding_selector,
          filter_op,
          {key, Map.to_list(value)},
          opts
        )

      key in @subquery_operators ->
        build_query(
          schema_source,
          query,
          binding_selector,
          filter_op,
          {key, value},
          opts
        )

      key in [:and, :or] ->
        build_query(schema_source, query, binding_selector, filter_op, {key, value}, opts)

      Keyword.keyword?(value) ->
        Enum.reduce(value, query, fn entry, query_acc ->
          apply_filters(
            schema_source,
            query_acc,
            binding_selector,
            filter_op,
            {key, entry},
            opts
          )
        end)

      true ->
        build_query(
          schema_source,
          query,
          binding_selector,
          filter_op,
          {key, value},
          opts
        )
    end
  end

  defp build_query(schema_source, query, binding_selector, :subquery, params, opts)
       when is_map(params) or is_list(params) do
    binding_source = resolve_binding_source(schema_source, query, binding_selector)

    inner_query =
      apply_filters(
        schema_source,
        query,
        binding_selector,
        @where,
        params,
        opts
      )

    SubQuery.build(
      binding_source,
      :subquery,
      inner_query,
      binding_selector,
      params,
      opts
    )
  end

  defp build_query(_schema_source, query, _binding_selector, :subquery, params, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :subquery params to be a map or keyword list, got: #{inspect(params)}"
    )

    query
  end

  defp build_query(schema_source, query, binding_selector, filter_op, params, opts) do
    binding_source = resolve_binding_source(schema_source, query, binding_selector)

    case @query_builder_modules do
      %{^filter_op => module} ->
        module.build(binding_source, filter_op, query, binding_selector, params, opts)

      _ ->
        Filter.build(binding_source, filter_op, query, binding_selector, params, opts)
    end
  end

  defp resolve_binding_source(schema_source, query, binding_selector) do
    case binding_selector do
      {:as, nil} ->
        schema_source

      {mode, binding_target} when mode in @binding_modes ->
        CommonQuery.get_query_binding_source(query, binding_target)
    end
  end

  defp source_has_schema?({_, nil}), do: false
  defp source_has_schema?({_, mod}) when is_atom(mod), do: true

  defp normalize_filter_params({k, v}) when is_map(v) or is_list(v) do
    {k, normalize_filter_params(v)}
  end

  defp normalize_filter_params(map) when is_map(map) and not is_struct(map) do
    map
    |> Map.to_list()
    |> normalize_filter_params()
  end

  defp normalize_filter_params(list) when is_list(list) do
    if Keyword.keyword?(list) do
      list
      |> sort_params()
      |> Enum.map(fn {k, v} -> {k, normalize_filter_params(v)} end)
    else
      list
    end
  end

  defp normalize_filter_params(term) do
    term
  end

  defp sort_params(params) do
    where_filters = Keyword.take(params, [:where])
    or_where_filters = Keyword.take(params, [:or_where])
    terminal_filters = Enum.filter(params, fn {key, _val} -> key in [:last, :subquery] end)

    # Regular field filters should be processed with where_filters
    # since they can contain implicit WHERE clauses and must come
    # before or_where filters
    other_filters = Keyword.drop(params, [:where, :or_where, :last, :subquery])

    where_filters
    |> Kernel.++(other_filters)
    |> Kernel.++(or_where_filters)
    |> Kernel.++(terminal_filters)
  end
end
