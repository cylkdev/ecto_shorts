defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"
  @moduledoc """
  Converts maps and keyword lists of filter params into `Ecto.Query` structs
  using a data-driven filtering language.

  Use this module when you want to build Ecto queries from plain data
  structures instead of writing Ecto query macros by hand. Pass a schema
  module (or `{source, schema}` tuple, or an existing `Ecto.Query`) together
  with a map or keyword list of params to `convert_params_to_filter/3` and
  receive an `Ecto.Query` back. Do not use this module to execute queries —
  see `EctoShorts.Actions` for that. `EctoShorts.Dynamics` handles expression
  compilation under the hood.

  The single entry point is `convert_params_to_filter/3`.

  ## Getting started

  Here is a simple example:

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{title: "Hello", published: true, limit: 10}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post,
        where: p0.title == ^"Hello" and p0.published == ^true,
        limit: ^10>

  The map parameters are automatically converted into Ecto query clauses.
  The `title` and `published` fields become `WHERE` conditions, while
  `limit` becomes a query operation.

  ## How the filtering language works

  The filtering language is **data-driven** and follows a fixed
  **order of precedence**. Think of every filter map as filling a row
  of **slots** — certain keys may only appear in specific slots, and
  the system always processes them in the same order regardless of
  the order they appear in your map.

  There are two levels of precedence:

  * **Top-level processing order** — controls which params are applied
    to the query first when multiple keys appear in the same map.

  * **Field-level expression slots** — controls how keys nest inside
    a single field expression.

  ### Top-level processing order

  When you pass a map with multiple keys, the system sorts them into
  a fixed processing order before building the query:

      ┌───────────────────┐   ┌────────────┐   ┌──────────────────┐   ┌──────────────────┐
      │ where / fields    │ → │ or_where   │ → │ query operations │ → │ terminal filters │
      └───────────────────┘   └────────────┘   └──────────────────┘   └──────────────────┘

  * **where / fields** — Explicit `:where` keys and bare field keys
    (like `published: true`) are processed first. They produce
    `WHERE ... AND ...` conditions.

  * **or_where** — Explicit `:or_where` keys are processed second.
    They produce `OR` conditions.

  * **query operations** — Reserved keys like `:limit`, `:order_by`,
    `:join`, `:preload`, etc. are processed next.

  * **terminal filters** — `:last` and `:subquery` are always
    processed last because they wrap the entire query built so far.

  For example, given this input:

      %{published: true, or_where: %{published: false}, limit: 10, last: 2}

  The processing order is:

  1. `published: true` → adds `WHERE published = true`
  2. `or_where: %{published: false}` → adds `OR published = false`
  3. `limit: 10` → adds `LIMIT 10`
  4. `last: 2` → wraps the query in a subquery that takes the last 2 rows

  Input order does not matter — `%{last: 2, published: true, limit: 10,
  or_where: %{published: false}}` produces the same query.

  ### Field-level expression slots

  Each field expression is a chain of nested maps. Every key type
  occupies exactly one **slot** in the chain. The slots are processed
  from outermost to innermost:

      ┌───────┐   ┌─────┐   ┌───────────┐   ┌──────────┐   ┌──────────────────┐
      │ field │ → │ not │ → │ aggregate │ → │ operator │ → │ value expression │
      └───────┘   └─────┘   └───────────┘   └──────────┘   └──────────────────┘

  * **Field** — the outermost key. A schema field name like `:views`
    or `:title`.

  * **Negation** (`:not`) — optional wrapper that negates everything
    inside it.

  * **Aggregate** (`:avg`, `:count`, `:sum`, `:max`, `:min`) —
    optional wrapper that applies an aggregate function. Typically
    used inside `:having`.

  * **Operator** — the comparison or matching key. Symbol operators:
    `:==`, `:!=`, `:>`, `:>=`, `:<`, `:<=`. Word aliases: `:eq`,
    `:gt`, `:gte`, `:lt`, `:lte`. Membership: `:in`. String matching:
    `:like`, `:ilike`.

  * **Value expression** — the innermost value. Can be a literal, or
    a special expression key: `:lower` / `:upper` (string transforms),
    `:+` / `:-` / `:*` / `:/` (arithmetic), `:all` / `:any` (subquery
    set comparison), `:datetime` / `:date` (date/time helpers).

  Here is an example that fills every slot:

      # field   not    aggregate  operator  value expression
      %{views: %{not: %{avg:     %{>:      %{+: [:views, 10]}}}}}

  Not every slot needs to be filled. A simple equality filter only
  uses the field slot and a literal value:

      %{title: "hello"}

  A comparison adds the operator slot:

      %{views: %{>: 10}}

  A negated comparison adds the not slot:

      %{views: %{not: %{>: 10}}}

  ## Field equality

  The simplest filter. When a field key maps directly to a value, it
  produces a `WHERE field = value` condition:

      %{title: "hello"}
      %{published: true}
      %{id: 1}

  When the value is `nil`, it produces `IS NULL`:

      %{published_at: nil}

  When the value is a non-keyword list, it produces `IN`:

      %{published: [true, false]}

  You can pass params as maps, keyword lists, or lists of either:

      # keyword list
      [id: 1]

      # list of maps (each entry becomes a separate WHERE clause)
      [%{id: 1}, %{published: true}]

      # list of keyword lists
      [[id: 1], [published: true]]

  ## Comparison operators

  Wrap a field value in a map with an operator key to use a specific
  comparison. **Symbol operators:**

      %{id: %{==: 1}}          # WHERE id = 1
      %{views: %{!=: 10}}      # WHERE views != 10
      %{views: %{>: 10}}       # WHERE views > 10
      %{views: %{>=: 10}}      # WHERE views >= 10
      %{views: %{<: 10}}       # WHERE views < 10
      %{views: %{<=: 10}}      # WHERE views <= 10

  **Word aliases** produce the same SQL:

      %{id: %{eq: 1}}          # same as %{id: %{==: 1}}
      %{views: %{gt: 10}}      # same as %{views: %{>: 10}}
      %{views: %{gte: 10}}     # same as %{views: %{>=: 10}}
      %{views: %{lt: 10}}      # same as %{views: %{<: 10}}
      %{views: %{lte: 10}}     # same as %{views: %{<=: 10}}

  **Membership** with `:in`:

      %{published: %{in: [true, false]}}   # WHERE published IN (true, false)
      %{id: %{in: [1, 2, 3]}}             # WHERE id IN (1, 2, 3)

  **List coercion** — `:==` with a list coerces to `IN`, `:!=` with a
  list coerces to `NOT IN`:

      %{published: %{==: [true, false]}}   # WHERE published IN (true, false)
      %{published: %{!=: [true, false]}}   # WHERE published NOT IN (true, false)

  **Nil comparisons** — `:==` with `nil` produces `IS NULL`, `:!=`
  with `nil` produces `IS NOT NULL`. Only `:==`, `:eq`, and `:!=` are
  valid operators for `nil`:

      %{published_at: %{==: nil}}   # WHERE published_at IS NULL
      %{published_at: %{!=: nil}}   # WHERE published_at IS NOT NULL

  ## Negation

  The `:not` key wraps and negates any expression inside it. It
  occupies the **negation slot** in the field-level chain:

      %{views: %{not: %{>: 10}}}              # WHERE NOT (views > 10)
      %{views: %{not: %{>=: 10}}}             # WHERE NOT (views >= 10)
      %{views: %{not: %{==: 10}}}             # WHERE views != 10
      %{views: %{not: %{!=: 10}}}             # WHERE views == 10

  Negating `:in`:

      %{published: %{not: %{in: [true, false]}}}   # WHERE published NOT IN (...)

  Negating `:==` / `:!=` with lists:

      %{published: %{not: %{==: [true, false]}}}    # WHERE published NOT IN (...)
      %{published: %{not: %{!=: [true, false]}}}    # WHERE published IN (...)

  Negating aggregates:

      %{views: %{not: %{avg: %{>: 10}}}}   # HAVING NOT (avg(views) > 10)

  ## String matching

  The `:like` and `:ilike` operators perform pattern matching. The
  system automatically wraps patterns with `%` wildcards:

      %{title: %{like: "hello"}}       # WHERE title LIKE '%hello%'
      %{title: %{ilike: "hello"}}      # WHERE title ILIKE '%hello%'

  Pass a list of patterns to match any of them:

      %{title: %{like: ["hello", "world"]}}    # WHERE title LIKE ANY(...)
      %{title: %{ilike: ["hello", "world"]}}   # WHERE title ILIKE ANY(...)

  Negate with `:not`:

      %{title: %{not: %{like: "hello"}}}                # WHERE NOT (title LIKE '%hello%')
      %{title: %{not: %{ilike: ["hello", "world"]}}}    # WHERE NOT (title ILIKE ANY(...))

  ## String transformations

  The `:lower` and `:upper` keys appear inside the **value expression
  slot** of a comparison operator. They transform the field before
  comparing:

      %{title: %{==: %{lower: "hello"}}}    # WHERE lower(title) = 'hello'
      %{title: %{!=: %{upper: "HELLO"}}}    # WHERE upper(title) != 'HELLO'

  Combine with negation:

      %{title: %{not: %{==: %{lower: "hello"}}}}   # WHERE lower(title) != 'hello'

  ## Aggregate functions

  Aggregate keys (`:avg`, `:count`, `:sum`, `:max`, `:min`) occupy the
  **aggregate slot** and wrap a comparison operator. They are typically
  used inside a `:having` clause:

      %{group_by: :author_id, having: %{views: %{avg: %{>: 10}}}}
      %{group_by: :author_id, having: %{views: %{count: %{>: 10}}}}
      %{group_by: :author_id, having: %{views: %{sum: %{>: 10}}}}

  When an aggregate receives a non-operator value directly, it defaults
  to `:==`:

      %{views: %{avg: 10}}   # equivalent to %{views: %{avg: %{==: 10}}}

  Negate an aggregate by wrapping it with `:not`:

      %{views: %{not: %{avg: %{>: 10}}}}   # HAVING NOT (avg(views) > 10)

  ## Arithmetic expressions

  Arithmetic operators (`:+`, `:-`, `:*`, `:/`) appear inside the
  **value expression slot** of a comparison. They take a two-element
  list `[field_or_value, field_or_value]`:

      %{views: %{>: %{+: [:views, 10]}}}    # WHERE views > views + 10
      %{views: %{>: %{-: [:views, 10]}}}    # WHERE views > views - 10
      %{views: %{>: %{*: [:views, 2]}}}     # WHERE views > views * 2
      %{views: %{>: %{/: [:views, 2]}}}     # WHERE views > views / 2

  Combine with negation:

      %{views: %{not: %{>: %{+: [:views, 10]}}}}   # WHERE NOT (views > views + 10)

  ## Date and time expressions

  Date/time type wrappers (`:datetime`, `:date`) occupy the **value
  expression slot** and wrap an operation (`:add`, `:ago`, `:from_now`):

  **`:add`** — adds an interval to a field value:

      %{inserted_at: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}
      # WHERE inserted_at >= datetime_add(inserted_at, 1, 'day')

      %{inserted_at: %{>=: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}
      # WHERE inserted_at >= date_add(inserted_at, 1, 'day')

  **`:ago`** — compares against a point in the past:

      %{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: "day"}}}}}
      # WHERE inserted_at > (now - 1 day)

  **`:from_now`** — compares against a point in the future:

      %{inserted_at: %{>: %{datetime: %{from_now: %{count: 1, interval: "day"}}}}}
      # WHERE inserted_at > (now + 1 day)

  Combine with negation:

      %{inserted_at: %{not: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}}

  ## Subquery and set comparisons

  The `:all` and `:any` keys appear inside the **value expression slot**
  of a comparison operator. They wrap a subquery expression:

      subquery_expr = from(c in "comments", select: c.post_id)

      %{id: %{>: %{all: subquery_expr}}}    # WHERE id > ALL(subquery)
      %{id: %{>: %{any: subquery_expr}}}    # WHERE id > ANY(subquery)

  When no comparison operator is provided, they default to `:==`:

      %{id: %{all: subquery_expr}}    # WHERE id = ALL(subquery)
      %{id: %{any: subquery_expr}}    # WHERE id = ANY(subquery)

  You can pass a query-builder payload instead of a pre-built subquery:

      %{id: %{>: %{all: [source: Post, query: %{id: 1}]}}}

  Combine with negation:

      %{id: %{not: %{>: %{all: subquery_expr}}}}   # WHERE NOT (id > ALL(subquery))
      %{id: %{not: %{all: subquery_expr}}}          # WHERE id != ALL(subquery)

  ## Logical operators

  ### Field-level `:and` and `:or`

  At the field level, `:and` and `:or` combine multiple conditions on
  the same field. They wrap a keyword list of operator expressions:

      %{views: %{and: [>: 10, <: 20]}}             # WHERE views > 10 AND views < 20
      %{views: %{or: [>: 10, <: 5]}}               # WHERE views > 10 OR views < 5
      %{published: %{or: [==: true, ==: false]}}    # WHERE published = true OR published = false

  An empty list is a no-op — the query is returned unchanged:

      %{views: %{and: []}}

  ### Top-level `:and` and `:or`

  At the top level, `:and` and `:or` combine multiple complete filter
  conditions. They wrap a list of keyword lists (or maps):

      %{or: [[published: true, views: 20], [published: false, views: 10]]}
      # WHERE (published = true AND views = 20) OR (published = false AND views = 10)

      %{and: [[published: true, views: 20], [title: "hello", views: 15]]}
      # WHERE (published = true AND views = 20) AND (title = 'hello' AND views = 15)

  You can mix top-level `:or` with regular field filters:

      [title: "test", or: [[published: true, views: 20], [published: false, views: 10]]]
      # WHERE title = 'test' AND ((published AND views = 20) OR (NOT published AND views = 10))

  Nested field-level operators work inside top-level logical operators:

      %{or: [[views: %{>: 10}, published: true], [views: %{<: 5}, published: false]]}

  ## Schema filter precedence

  The `:where` and `:or_where` keys give explicit control over how
  conditions are grouped. The system processes them in a fixed order:

  1. Explicit `:where` and bare field filters are processed **first**.
  2. Explicit `:or_where` is processed **second**.

  Bare field filters behave as implicit `:where` filters:

      # These two are equivalent:
      %{published: true, or_where: %{published: false}}
      %{where: %{published: true}, or_where: %{published: false}}
      # Both produce: WHERE published = true OR published = false

  This ordering is deterministic regardless of map key order:

      # Even though or_where appears first in the keyword list,
      # the field filter published: true is processed first:
      [or_where: %{views: %{or: [>: 10, <: 5]}}, published: true]
      # Produces: WHERE published = true OR (views > 10 OR views < 5)

  Explicit `:where` accepts a map or keyword list of filters:

      %{where: %{published: true, views: 10}}
      %{where: [published: true, views: 10]}

  ## Array fields

  Array fields (like `{:array, :string}`) support special operators
  that check array membership or equality:

      %{tags: "elixir"}                    # WHERE 'elixir' = ANY(tags)
      %{tags: %{in: ["elixir"]}}           # WHERE tags @> ARRAY['elixir']
      %{tags: %{==: ["elixir", "erlang"]}} # WHERE tags = ARRAY['elixir', 'erlang']
      %{tags: %{!=: ["elixir"]}}           # WHERE tags != ARRAY['elixir']

  Use `:all` with `:in` to require all values:

      %{tags: %{all: %{in: ["elixir", "erlang"]}}}   # WHERE tags @> ARRAY['elixir', 'erlang']

  Negate with `:not`:

      %{tags: %{not: %{in: ["elixir"]}}}                  # negated containment
      %{tags: %{not: %{all: %{in: ["elixir", "erlang"]}}}} # negated "contains all"

  Array fields also support comparison, string matching, string
  transforms, and aggregate operators — all following the same
  slot-based nesting rules:

      %{tags: %{count: %{>: 0}}}           # HAVING count(tags) > 0
      %{tags: %{like: "elixir"}}           # WHERE tags LIKE '%elixir%'
      %{tags: %{==: %{lower: "elixir"}}}   # WHERE lower(tags) = 'elixir'

  > #### Overloaded `:all` {: .warning}
  >
  > The `:all` operator is overloaded. Its meaning depends on context:
  >
  > * Inside a comparison value (e.g. `%{>: %{all: subquery}}`) — subquery
  >   set comparison (see ["Subquery and set comparisons"](#module-subquery-and-set-comparisons)).
  > * With `:in` on an array field (e.g. `%{tags: %{all: %{in: [...]}}}`) —
  >   array "contains all values" check.

  ## Binding selectors

  The `:bind` key targets a specific binding in a query. This is
  useful for queries with joins where you need to filter or apply
  operations on a specific binding.

  **Named bindings** use `:as`:

      # Given a query: from(p in Post, as: :post)
      %{bind: %{as: %{post: %{published: true}}}}
      # Produces: WHERE p.published = true (on the :post binding)

  **Positional bindings** use `:at`:

      %{bind: %{at: %{1 => %{published: true}}}}
      # Targets binding at position 1

  **Multiple bindings** in a single call:

      # Given a query with :post and :author named bindings:
      %{bind: %{as: [post: %{published: true}, author: %{first_name: "John"}]}}

  Binding selectors also work with **query operations** — not just
  field filters. You can target `:order_by`, `:group_by`, `:having`,
  `:distinct`, `:windows`, `:update`, and other operations at a
  specific binding:

      %{bind: %{as: %{author: %{order_by: %{asc: :first_name}}}}}
      %{bind: %{at: %{2 => %{group_by: :first_name}}}}

  You can read a binding selector as a sentence. For example:

      %{bind: %{as: %{post: %{title: "Hello"}}}}

  reads as: "Bind the query to the named binding `:post`, and return
  the records where the title equals `\"Hello\"`."

  ## Query operations

  Query operation keys are **reserved**. They short-circuit normal
  filter conversion and map directly to Ecto query operations. Avoid
  using schema field names or association names that match any of
  these keys.

  ### Select

      %{select: true}                          # SELECT p.*
      %{select: :id}                           # SELECT p.id
      %{select: [:id, :title]}                 # SELECT p.id, p.title
      %{select: %{map: [:id, :title]}}         # SELECT map(p, [:id, :title])
      %{select: %{map: %{custom_id: :id}}}     # SELECT p.id AS custom_id
      %{select: %{struct: [:id]}}              # SELECT struct(p, [:id])

      %{select_merge: %{map: %{custom_id: :id}}}
      %{select_merge: %{map: [:id, :title]}}

  Combine both:

      [select: %{map: [:id]}, select_merge: %{map: %{post_title: :title}}]

  ### Ordering

      %{order_by: :title}                           # ORDER BY title DESC
      %{order_by: [desc: :title]}                   # ORDER BY title DESC
      %{order_by: [asc: :title, desc: :id]}         # ORDER BY title ASC, id DESC
      %{order_by: %{desc: :title}}                  # ORDER BY title DESC
      %{prepend_order_by: :title}                   # prepends to existing order_by
      %{prepend_order_by: [asc: :published_at, desc: :title]}
      %{reverse_order: true}                        # reverses existing order_by

  ### Grouping

      %{group_by: :author_id}
      %{group_by: [:author_id, :published]}

  ### Having

  The `:having` and `:or_having` keys accept the same filter language
  used for `:where` — including comparison operators, aggregates,
  logical operators, and dynamic expressions:

      %{having: %{published: true}}
      %{having: %{views: %{>: 10}}}
      %{having: %{views: %{avg: %{>: 10}}}}
      %{having: dynamic([p], p.views > ^10)}
      %{having: [and: [published: true, views: %{>: 10}]]}
      %{having: [or: [views: %{>: 10}, views: %{<: 5}]]}
      %{or_having: %{views: %{<: 5}}}

  ### Pagination

      %{limit: 10}
      %{offset: 5}
      %{first: 10}            # delegates to :limit

  The `:last` key is a **terminal filter** — it wraps the entire query
  in a subquery that reverses the order and takes the last N rows:

      %{last: 2}                 # last 2 rows by primary key
      %{last: %{title: 2}}      # last 2 rows ordered by :title
      %{last: [title: 2]}       # same as above
      %{last: {nil, 2}}         # explicit nil uses primary key

  ### Distinct

      %{distinct: true}
      %{distinct: false}
      %{distinct: :title}
      %{distinct: [desc: :title]}
      %{distinct: %{desc: :title}}

  ### Joins

  The `:join` key supports multiple join sources:

      # Association join
      %{join: [author: [as: :author]]}

      # Schema join
      %{join: [schema: [source: User, as: :user_join, on: true]]}

      # Table join
      %{join: [table: [source: "users", as: :users_table, on: true]]}

      # Query join
      %{join: [query: [source: user_query, as: :adult_users, on: true]]}

      # Subquery join
      %{join: [subquery: [source: user_query, as: :name, on: true]]}
      %{join: [subquery: [source: [source: User, query: [age: [>=: 18]]], as: :name, on: true]]}

      # Fragment join
      %{join: [fragment: [source: %{name: :active_users, values: [min_age: 21]}, as: :active_users, on: true]]}

      # Multiple joins in one call
      %{join: [author: [as: :author], table: [source: "users", as: :users_table, on: true]]}

  ### Association shorthand

  When a key matches an association name, you can filter on the
  associated schema using a keyword list. This automatically creates
  a join and applies the filters to the joined binding:

      %{author: [as: :author, first_name: "John"]}
      %{author: [first_name: "John"]}
      %{author: [as: :author, type: :left, first_name: "John"]}
      %{author: [as: :author, on: true, first_name: "John"]}

  The optional keys `:as`, `:on`, and `:type` configure the join.
  All remaining keys are treated as filters on the associated schema.

  ### Preload

      %{preload: :author}
      %{preload: [:author]}
      %{preload: [author: [:posts]]}

  Preloads also support binding selectors:

      %{preload: [bind: [as: [example: :author]]]}
      %{preload: [bind: [at: %{2 => :author}]]}
      %{preload: [bind: [as: [author: :author]], posts: [:comments]]}

  ### Set operations

      %{union: %{published: false}}
      %{union_all: %{published: false}}
      %{except: %{published: false}}
      %{except_all: %{published: false}}
      %{intersect: %{published: false}}
      %{intersect_all: %{published: false}}

  You can also pass a pre-built query:

      %{except: from(p in Post, where: p.published == ^false)}

  ### Subquery

  The `:subquery` key is a **terminal filter** — it wraps the entire
  query (with all filters applied) as a subquery:

      %{subquery: %{id: 2}}
      %{subquery: [id: 2]}

  ### Lock

      %{lock: fn query -> from(p in query, lock: "FOR UPDATE") end}
      %{lock: %{name: :for_share, values: []}}
      %{lock: [name: :for_share, values: []]}

  ### Common Table Expressions (CTEs)

      %{recursive_ctes: true}
      %{with_cte: [published_posts: [as: cte_query]]}
      %{with_cte: %{published_posts: %{as: cte_query}}}
      %{with_cte: [published_posts: [as: cte_query, materialized: false, operation: :all]]}
      %{with_cte: [published_posts: [as: %{source: Post, query: %{published: true}}]]}

  ### Named bindings

      %{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}]}
      %{with_named_binding: %{author: %{join: [association: [source: :author, as: :author]]}}}

  ### Windows

      %{windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]}
      %{windows: %{post_window: %{partition_by: :author_id, order_by: [desc: :inserted_at]}}}

  ### With ties

      %{with_ties: true}
      %{with_ties: false}
      %{with_ties: %{bind: %{as: %{post: true}}}}
      %{with_ties: %{bind: %{at: %{1 => true}}}}

  ### Update

      %{update: [set: [title: "After"], inc: [views: 1]]}
      %{update: %{set: %{title: "After"}}}

  ### Exclude

  Removes existing query expressions:

      %{exclude: :order_by}
      %{exclude: [:order_by, :limit]}

  ### Query prefix

      %{put_query_prefix: "tenant_a"}

  ## Custom convenience filters

  These built-in keys are not standard Ecto operations but provide
  common shortcuts:

      %{ids: [1, 2, 3]}                          # WHERE id IN (1, 2, 3)
      %{after: 10}                               # WHERE id > 10
      %{before: 10}                              # WHERE id < 10
      %{start_date: ~U[2026-01-01 00:00:00Z]}    # WHERE inserted_at >= value
      %{end_date: ~U[2026-12-31 23:59:59Z]}      # WHERE inserted_at <= value

  ## Dynamic expressions

  The `:dynamic` key accepts an `Ecto.Query.DynamicExpr` and applies
  it directly:

      %{dynamic: dynamic([p], p.views > ^10)}

  Inside `:where` or `:or_where`:

      %{where: %{dynamic: dynamic([p], p.published == ^true)}}
      %{or_where: %{dynamic: dynamic([p], p.views > ^100)}}

  ## Exists

  The `:exists` key checks for the existence of rows in a subquery.
  Use it inside `:where`:

      %{where: %{exists: subquery_expr}}
      %{where: %{exists: %{not: subquery_expr}}}

  ## Source and query params

  The `:source` and `:query` meta-keys allow overriding the schema
  source and merging additional query params:

      [source: Post, id: 1]
      [source: Post, query: %{id: 1}]
      [source: "posts", query: %{select: [:id]}]
      [query: %{id: 1}, published: true]

  When the source is a bare table name string (no schema module), and
  no `:select` is provided, the system automatically adds
  `select: true` to ensure the query has a select clause.

  ## Error handling

  This module follows a **warn-and-skip** model. Invalid filter data
  never raises an exception:

  * **Unknown keys** that are not schema fields or reserved keys are
    logged as warnings and skipped.

  * **Invalid payloads** (wrong types, malformed maps) are logged as
    warnings and the failing operation is skipped.

  * The rest of the query continues building normally. Only the
    invalid entry is dropped.

  This makes the module safe to use with user-provided data where
  some keys may be unexpected.

  See also `EctoShorts.Actions` for executing queries,
  `EctoShorts.Dynamics` for expression compilation, and
  `EctoShorts.CommonFilters.Having` for advanced having clauses.
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

  alias EctoShorts.Logger
  alias EctoShorts.SchemaHelpers

  @logger_prefix "EctoShorts.CommonFilters"

  @binding_selector_key :bind
  @default_binding_selector {:as, nil}

  @where :where
  @map_payload_helper_operators [:datetime_add, :date_add, :from_now, :ago]
  @schema_filters [:where, :or_where]
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

  `source` is a schema module, `{source, schema}` tuple, or an existing
  `Ecto.Query`. `params` is a map or keyword list of filter params.
  `opts` are forwarded to all sub-query builders.

  Schema field keys become `WHERE` conditions. Reserved query operation
  keys (`:limit`, `:order_by`, `:join`, etc.) become the corresponding
  Ecto query operations. Unknown keys log a warning and are skipped.
  Invalid payloads also log a warning and are skipped — the rest of the
  query continues building.

  Returns an `Ecto.Query` struct with all params applied.

  See the [moduledoc](`m:EctoShorts.CommonFilters`) for the complete
  filtering language reference, including the
  [slot-based expression model](#module-field-level-expression-slots),
  [top-level processing order](#module-top-level-processing-order),
  and all supported [query operations](#module-query-operations).

  ## Options

  * `:repo` (default: `EctoShorts.Config.repo/0`) - the `Ecto.Repo`
    module used to resolve the dynamic expression adapter.
  * `:dynamic_adapter` - a module implementing
    `EctoShorts.Dynamics.Adapter` for this call. Defaults to the
    adapter resolved from `:repo`.
  * `:query_fields` - list of field atoms to limit which fields are
    accepted as schema filters.

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
      ...>   %{limit: 5, order_by: {:desc, :inserted_at}}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post,
        order_by: [desc: p0.inserted_at], limit: ^5>

  See also `EctoShorts.Actions.all/3`, `EctoShorts.Dynamics`, and
  `EctoShorts.CommonFilters.Having`.
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

  def convert_params_to_filter(source, entries, opts) when is_list(entries) do
    query = CommonSchema.to_query(source)

    has_source_or_query? =
      Keyword.has_key?(entries, :source) or Keyword.has_key?(entries, :query)

    if Keyword.keyword?(entries) do
      {schema_source, params} = Keyword.pop(entries, :source, source)

      {other_params, params} = Keyword.pop(params, :query, [])

      normalized_source = CommonSchema.normalize_source(schema_source)

      merged_params =
        other_params
        |> ensure_kw()
        |> Keyword.merge(params)

      merged_params =
        case {has_source_or_query?, normalized_source} do
          {true, {_table, nil}} ->
            if Keyword.has_key?(merged_params, :select) do
              merged_params
            else
              Keyword.put(merged_params, :select, true)
            end

          _ ->
            merged_params
        end

      do_convert(normalized_source, query, merged_params, opts)
    else
      Enum.reduce(entries, query, fn entry, query_acc ->
        do_convert(source, query_acc, entry, opts)
      end)
    end
  end

  defp ensure_kw(map) when is_map(map), do: Map.to_list(map)

  defp ensure_kw(list) when is_list(list) do
    if Keyword.keyword?(list) do
      list
    else
      Logger.warning(@logger_prefix, "Expected params to be a keyword list, got: #{inspect(list)}")
      []
    end
  end

  defp ensure_kw(term) do
    Logger.warning(@logger_prefix, "Expected params to be a keyword list, got: #{inspect(term)}")
    []
  end

  defp do_convert(schema_source, query, params, opts) do
    normalized_params = normalize_filter_params(params)

    if is_map(normalized_params) or Keyword.keyword?(normalized_params) do
      create_schema_filter(
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

  @doc false
  def create_schema_filter(
        schema_source,
        query,
        binding_selector,
        _filter_op,
        {key, value},
        opts
      )
      when key in @schema_filters do
    reduce_schema_filter_params(schema_source, query, binding_selector, key, value, opts)
  end

  def create_schema_filter(
        schema_source,
        query,
        binding_selector,
        filter_op,
        {@binding_selector_key, bind_params},
        opts
      ) do
    BindingParams.build_binding_params(
      schema_source,
      query,
      binding_selector,
      filter_op,
      bind_params,
      opts
    )
  end

  def create_schema_filter(
        schema_source,
        query,
        binding_selector,
        filter_op,
        {key, value},
        opts
      ) do
    query_filters = Keyword.get(opts, :query_filters, @query_filters)

    if key in query_filters do
      build_query(schema_source, query, binding_selector, key, value, opts)
    else
      reduce_default_filter_params(
        schema_source,
        query,
        binding_selector,
        filter_op,
        key,
        value,
        opts
      )
    end
  end

  def create_schema_filter(schema_source, query, binding_selector, filter_op, params, opts)
      when is_map(params) do
    create_schema_filter(
      schema_source,
      query,
      binding_selector,
      filter_op,
      Map.to_list(params),
      opts
    )
  end

  def create_schema_filter(schema_source, query, binding_selector, filter_op, params, opts)
      when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {key, value}, query_acc ->
        create_schema_filter(
          schema_source,
          query_acc,
          binding_selector,
          filter_op,
          {key, value},
          opts
        )
      end)
    else
      build_query(schema_source, query, binding_selector, filter_op, params, opts)
    end
  end

  @doc false
  def create_schema_filter(schema_source, query, binding_selector, filter_op, params, opts) do
    build_query(schema_source, query, binding_selector, filter_op, params, opts)
  end

  defp reduce_schema_filter_params(schema_source, query, binding_selector, filter_op, value, opts) do
    if is_map(value) or is_list(value) do
      Enum.reduce(value, query, fn entry, query_acc ->
        build_schema_filters(schema_source, query_acc, binding_selector, filter_op, entry, opts)
      end)
    else
      Logger.warning(
        @logger_prefix,
        "Expected params for #{filter_op} to be a map or keyword list, got: #{inspect(value)}"
      )

      query
    end
  end

  defp reduce_default_filter_params(
         schema_source,
         query,
         binding_selector,
         filter_op,
         key,
         value,
         opts
       ) do
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
          build_join_filters(
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

  defp build_join_filters(
         schema_source,
         query,
         binding_selector,
         filter,
         assoc_key,
         params,
         opts
       ) do
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
          {:as, Keyword.get(params, :as, nil)}
        else
          {:at, CommonQuery.query_binding_count(joined_query)}
        end

      create_schema_filter(
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
        "Expected association params for #{inspect(assoc_key)} to be a keyword list, got: #{inspect(params)}"
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
       )
       when is_map(value) and not is_struct(value) and
              (is_map_key(value, :datetime) or is_map_key(value, :date)) do
    build_query(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, value},
      opts
    )
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_map(value) and not is_struct(value) do
    create_schema_filter(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, Map.to_list(value)},
      opts
    )
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_list(value) do
    if Keyword.keyword?(value) do
      Enum.reduce(value, query, fn {key2, value2}, query_acc ->
        create_schema_filter(
          schema_source,
          query_acc,
          binding_selector,
          filter_op,
          {key, {key2, value2}},
          opts
        )
      end)
    else
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

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       ) do
    build_query(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, value},
      opts
    )
  end

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

  defp build_query(schema_source, query, binding_selector, :subquery, params, opts)
       when is_map(params) or is_list(params) do
    binding_source = to_binding_source(schema_source, query, binding_selector)

    filtered_query =
      create_schema_filter(
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
      filtered_query,
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
    binding_source = to_binding_source(schema_source, query, binding_selector)
    module = Map.get(@query_builder_modules, filter_op, Filter)
    module.build(binding_source, filter_op, query, binding_selector, params, opts)
  end

  defp to_binding_source(schema_source, _query, {:as, nil}) do
    schema_source
  end

  defp to_binding_source(_schema_source, query, {_binding_mode, binding_target}) do
    CommonQuery.get_query_binding_source(query, binding_target)
  end

  defp normalize_filter_params({k, v})
       when k in @map_payload_helper_operators and is_map(v) and not is_struct(v) do
    {k, v}
  end

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
    rest = Keyword.drop(params, [:where, :or_where, :last, :subquery])

    where_filters
    |> Kernel.++(rest)
    |> Kernel.++(or_where_filters)
    |> Kernel.++(terminal_filters)
  end
end
