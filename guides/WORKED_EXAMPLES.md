## Field Equality Examples

%{id: 1}

%{published: true}

%{title: "hello"}

%{published_at: ~U[2026-01-01 00:00:00Z]}

%{published_at: nil}

%{published: [true, false]}

[id: 1]

[%{id: 1}]

[[id: 1]]

[%{id: 1}, %{published: true}]

[[id: 1], [published: true]]

## Comparison Operators Examples

%{id: %{==: 1}}

%{id: %{eq: 1}}

%{published_at: %{==: nil}}

%{published_at: %{eq: nil}}

%{published_at: %{!=: nil}}

%{views: %{>: 10}}

%{views: %{>=: 10}}

%{views: %{<: 10}}

%{views: %{<=: 10}}

%{views: %{!=: 10}}

%{views: %{gt: 10}}

%{views: %{gte: 10}}

%{views: %{lt: 10}}

%{views: %{lte: 10}}

%{published: %{in: [true, false]}}

%{published: %{==: [true, false]}}

%{published: %{!=: [true, false]}}

## Negation Examples

%{published: %{not: %{in: [true, false]}}}

%{published: %{not: %{==: [true, false]}}}

%{published: %{not: %{!=: [true, false]}}}

%{views: %{not: %{>: 10}}}

%{views: %{not: %{>=: 10}}}

%{views: %{not: %{<: 10}}}

%{views: %{not: %{<=: 10}}}

%{views: %{not: %{==: 10}}}

%{views: %{not: %{!=: 10}}}

%{views: %{not: %{gt: 10}}}

%{views: %{not: %{gte: 10}}}

%{views: %{not: %{lt: 10}}}

%{views: %{not: %{lte: 10}}}

## String Matching Examples

%{title: %{like: "hello"}}

%{title: %{ilike: "hello"}}

%{title: %{like: ["hello", "world"]}}

%{title: %{ilike: ["hello", "world"]}}

%{title: %{not: %{like: "hello"}}}

%{title: %{not: %{ilike: "hello"}}}

%{title: %{not: %{like: ["hello", "world"]}}}

%{title: %{not: %{ilike: ["hello", "world"]}}}

## String Transformations Examples

%{title: %{==: %{lower: "hello"}}}

%{title: %{==: %{upper: "HELLO"}}}

%{title: %{!=: %{lower: "hello"}}}

%{title: %{!=: %{upper: "HELLO"}}}

%{title: %{not: %{==: %{lower: "hello"}}}}

%{title: %{not: %{==: %{upper: "HELLO"}}}}

## Aggregate Functions Examples

%{views: %{avg: %{>: 10}}}

%{views: %{avg: %{>=: 10}}}

%{views: %{avg: %{<: 10}}}

%{views: %{avg: %{<=: 10}}}

%{views: %{avg: %{==: 10}}}

%{views: %{avg: %{!=: 10}}}

%{views: %{avg: %{gt: 10}}}

%{views: %{avg: %{gte: 10}}}

%{views: %{avg: %{lt: 10}}}

%{views: %{avg: %{lte: 10}}}

%{views: %{avg: %{eq: 10}}}

%{views: %{count: %{>: 10}}}

%{views: %{max: %{>: 10}}}

%{views: %{min: %{>: 10}}}

%{views: %{sum: %{>: 10}}}

%{views: %{not: %{avg: %{>: 10}}}}

%{views: %{not: %{avg: %{==: 10}}}}

## Subquery and Set Comparison Examples

%{id: %{>: %{all: subquery_expr}}}

%{id: %{>=: %{all: subquery_expr}}}

%{id: %{<: %{all: subquery_expr}}}

%{id: %{<=: %{all: subquery_expr}}}

%{id: %{==: %{all: subquery_expr}}}

%{id: %{!=: %{all: subquery_expr}}}

%{id: %{all: subquery_expr}}

%{id: %{>: %{all: %{from: %{query: Post, id: 1}}}}}

%{id: %{not: %{>: %{all: subquery_expr}}}}

%{id: %{not: %{all: subquery_expr}}}

%{id: %{>: %{any: subquery_expr}}}

%{id: %{any: subquery_expr}}

%{id: %{not: %{>: %{any: subquery_expr}}}}

%{id: %{not: %{any: subquery_expr}}}

## Arithmetic Expressions Examples

%{views: %{>: %{+: [:views, 10]}}}

%{views: %{>: %{-: [:views, 10]}}}

%{views: %{>: %{*: [:views, 2]}}}

%{views: %{>: %{/: [:views, 2]}}}

%{views: %{>=: %{+: [:views, 10]}}}

%{views: %{<: %{+: [:views, 10]}}}

%{views: %{<=: %{+: [:views, 10]}}}

%{views: %{==: %{+: [:views, 10]}}}

%{views: %{!=: %{+: [:views, 10]}}}

%{views: %{not: %{>: %{+: [:views, 10]}}}}

## Date/Time Expressions Examples

%{inserted_at: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}

%{inserted_at: %{==: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}

%{inserted_at: %{>=: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}

%{inserted_at: %{==: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}

%{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: "day"}}}}}

%{inserted_at: %{==: %{datetime: %{ago: %{count: 1, interval: "day"}}}}}

%{inserted_at: %{>: %{datetime: %{from_now: %{count: 1, interval: "day"}}}}}

%{inserted_at: %{==: %{datetime: %{from_now: %{count: 1, interval: "day"}}}}}

%{inserted_at: %{not: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}}

%{inserted_at: %{not: %{==: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}}

## Logical Operators - Field Level Examples

%{views: %{and: [>: 10, <: 20]}}

%{views: %{and: []}}

%{published: %{and: [==: true, !=: false]}}

%{published: %{or: [==: true, ==: false]}}

## Logical Operators - Top Level Examples

%{or: [[published: true, views: 20], [published: false, views: 10]]}

%{and: [[published: true, views: 20], [title: "hello", views: 15]]}

%{or: [[published: %{or: [==: true, ==: false]}], [published: %{or: [==: true, ==: false]}]]}

%{or: [[published: true, views: 20]]}

%{or: [[views: %{>: 10}, published: true], [views: %{<: 5}, published: false]]}

%{or: []}

%{and: []}

[title: "test", or: [[published: true, views: 20], [published: false, views: 10]]]

## Binding Selectors Examples

%{bind: %{as: %{post: %{published: true}}}}

%{bind: %{as: [post: %{published: true}]}}

%{bind: %{as: [post: %{published: true}, author: %{first_name: "John"}]}}

%{bind: %{at: %{1 => %{published: true}}}}

%{bind: %{at: %{1 => [published: true]}}}

%{bind: [as: [post: %{published: true}]]}

%{bind: [at: %{1 => %{published: true}}]}

## Schema Filter Precedence Examples

%{where: %{published: true}}

%{where: [published: true]}

%{where: %{published: true, views: 10}}

%{or_where: %{published: false}}

%{or_where: [published: false]}

%{where: %{published: true}, or_where: %{published: false}}

[or_where: %{views: %{or: [>: 10, <: 5]}}, published: true]

## Terminal Filters Examples

%{last: 2}

%{last: %{title: 2}}

%{last: [title: 2]}

%{last: {nil, 2}}

%{last: %{id: 2}}

%{subquery: %{id: 2}}

%{subquery: [id: 2]}

## Association Joins Examples

%{author: [as: :author, first_name: "John"]}

%{author: [first_name: "John"]}

%{author: [as: :author, on: true, first_name: "John"]}

%{author: [as: :author, type: :left, first_name: "John"]}

## Array Fields Examples

%{tags: "elixir"}

%{tags: %{==: "elixir"}}

%{tags: %{!=: "elixir"}}

%{tags: %{in: "elixir"}}

%{tags: ["elixir", "erlang"]}

%{tags: %{==: ["elixir", "erlang"]}}

%{tags: %{!=: ["elixir"]}}

%{tags: nil}

%{tags: %{==: nil}}

%{tags: %{!=: nil}}

%{tags: %{in: ["elixir"]}}

%{tags: %{in: ["elixir", "erlang"]}}

%{tags: %{all: %{in: ["elixir", "erlang"]}}}

%{tags: %{not: %{==: ["elixir"]}}}

%{tags: %{not: %{in: ["elixir"]}}}

%{tags: %{not: %{in: "elixir"}}}

%{tags: %{not: %{all: %{in: ["elixir", "erlang"]}}}}

%{tags: %{>: "elixir"}}

%{tags: %{>=: "elixir"}}

%{tags: %{<: "elixir"}}

%{tags: %{<=: "elixir"}}

%{tags: %{gt: "elixir"}}

%{tags: %{gte: "elixir"}}

%{tags: %{lt: "elixir"}}

%{tags: %{lte: "elixir"}}

%{tags: %{eq: "elixir"}}

%{tags: %{not: %{>: "elixir"}}}

%{tags: %{not: %{>=: "elixir"}}}

%{tags: %{not: %{<: "elixir"}}}

%{tags: %{not: %{<=: "elixir"}}}

%{tags: %{like: "elixir"}}

%{tags: %{ilike: "elixir"}}

%{tags: %{like: ["elixir", "erlang"]}}

%{tags: %{ilike: ["elixir", "erlang"]}}

%{tags: %{not: %{like: "elixir"}}}

%{tags: %{not: %{ilike: "elixir"}}}

%{tags: %{not: %{like: ["elixir", "erlang"]}}}

%{tags: %{not: %{ilike: ["elixir", "erlang"]}}}

%{tags: %{==: %{lower: "elixir"}}}

%{tags: %{==: %{upper: "ELIXIR"}}}

%{tags: %{!=: %{lower: "elixir"}}}

%{tags: %{!=: %{upper: "ELIXIR"}}}

%{tags: %{not: %{==: %{lower: "elixir"}}}}

%{tags: %{not: %{==: %{upper: "ELIXIR"}}}}

%{tags: %{count: %{>: 0}}}

%{tags: %{count: %{==: 0}}}

## Dynamic Examples

%{dynamic: dynamic([p], p.views > ^10)}

%{where: %{dynamic: dynamic([p], p.published === ^true)}}

%{or_where: %{dynamic: dynamic([p], p.views > ^100)}}

## Exists Examples

%{where: %{exists: subquery_expr}}

%{where: %{exists: %{not: subquery_expr}}}

## Schema-Level Config

### The :from Key

[published: true, subquery: %{id: 2}]

%{from: %{query: Post, id: 1}, published: true}

%{from: %{query: Post, id: 1}}

[from: [query: Post, id: 1]]

%{from: %{query: "posts", id: 1}}

%{from: %{query: "posts", select: [:id]}}

### Select Examples

%{select: true}

%{select: :id}

%{select: [:id, :title]}

%{select: %{map: [:id, :title]}}

%{select: %{map: %{custom_id: :id}}}

%{select: %{struct: [:id]}}

### Select Merge Examples

%{select_merge: %{map: %{custom_id: :id}}}

%{select_merge: %{map: [:id, :title]}}

[select: %{map: [:id]}, select_merge: %{map: %{post_title: :title}}]

### Distinct Examples

%{distinct: true}

%{distinct: false}

%{distinct: :title}

%{distinct: [desc: :title]}

%{distinct: %{desc: :title}}

[distinct: :title, order_by: :id]

### GroupBy Examples

%{group_by: :author_id}

%{group_by: [:author_id, :published]}

## Having Examples

%{having: %{published: true}}

%{having: %{views: %{>: 10}}}

%{having: %{views: %{avg: %{>: 10}}}}

%{having: dynamic([p], p.views > ^10)}

%{having: [and: [published: true, views: %{>: 10}]]}

%{having: [or: [views: %{>: 10}, views: %{<: 5}]]}

## OrHaving Examples

%{or_having: %{views: %{<: 5}}}

### OrderBy Examples

%{order_by: :title}

%{order_by: [desc: :title]}

%{order_by: [asc: :title, desc: :id]}

%{order_by: %{desc: :title}}

### Prepend OrderBy Examples

%{prepend_order_by: :title}

%{prepend_order_by: [asc: :published_at, desc: :title]}

### Limit, Offset, First, After, and Before Examples

%{after: 10}

%{before: 10}

%{limit: 10}

%{offset: 5}

%{first: 10}

%{limit: 10, offset: 5}

### Reverse Order Examples

%{reverse_order: true}

### Exclude Examples

%{exclude: :order_by}

%{exclude: [:order_by, :limit]}

### Put Query Prefix Examples

%{put_query_prefix: "tenant_a"}

[put_query_prefix: "tenant_a", put_query_prefix: "tenant_b"]

### Start Date, End Date, and IDs Examples

%{start_date: ~U[2026-01-01 00:00:00Z]}

%{end_date: ~U[2026-12-31 23:59:59Z]}

%{ids: [1, 2, 3]}

### Set Operations Examples

%{except: %{published: false}}

%{except: from(p in Post, where: p.published === ^false)}

%{except_all: %{published: false}}

%{intersect: %{published: false}}

%{intersect_all: %{published: false}}

%{union: %{published: false}}

%{union_all: %{published: false}}

### Lock Examples

%{lock: fn query -> from(p in query, lock: "FOR UPDATE") end}

%{lock: %{name: :for_share, values: []}}

%{lock: [name: :for_share, values: []]}

### CTE Examples

%{recursive_ctes: true}

%{recursive_ctes: false}

%{with_cte: [published_posts: [as: cte_query]]}

%{with_cte: %{published_posts: %{as: cte_query}}}

%{with_cte: [published_posts: [as: cte_query, materialized: false, operation: :all]]}

%{with_cte: [published_posts: [as: %{from: %{query: Post, published: true}}]]}

%{with_cte: [published_posts: [as: [from: [query: Post, id: 1]]]]}

[recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]]

### Named Bindings Examples

%{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}]}

%{with_named_binding: %{author: %{join: [association: [source: :author, as: :author]]}}}

%{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}, users_table: %{join: [table: [source: "users", as: :users_table, on: true]]}]}

### With Ties Examples

%{with_ties: true}

%{with_ties: false}

%{with_ties: %{bind: %{as: %{post: true}}}}

%{with_ties: %{bind: %{at: %{1 => true}}}}

### Update Examples

%{update: [set: [title: "After"], inc: [views: 1]]}

%{update: %{set: %{title: "After"}}}

### Windows Examples

%{windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]}

%{windows: %{post_window: %{partition_by: :author_id, order_by: [desc: :inserted_at]}}}

### Preload Examples

%{preload: :author}

%{preload: [:author]}

%{preload: [author: [:posts]]}

%{preload: [bind: [as: [example: :author]]]}

%{preload: [bind: [at: %{2 => :author}]]}

%{preload: [bind: [at: %{2 => :author}], posts: [:comments]]}

%{preload: [bind: [as: [author: :author]], posts: [:comments]]}

%{preload: [bind: [as: [author: :author], at: %{2 => :author}], posts: [:comments]]}

### Join Examples

%{join: [author: [as: :author]]}

%{join: [schema: [source: User, as: :user_join, on: true]]}

%{join: [table: [source: "users", as: :users_table, on: true]]}

%{join: [query: [source: user_query, as: :adult_users, on: true]]}

%{join: [subquery: [source: user_query, as: :name, on: true]]}

%{join: [subquery: [source: %{from: %{query: User, age: %{>=: 18}}}, as: :name, on: true]]}

%{join: [subquery: [source: %{from: %{published: true}}, as: :name, on: true]]}

%{join: [fragment: [source: %{name: :active_users, values: [min_age: 21]}, as: :active_users, on: true]]}

%{join: [fragment: [source: %{name: :active_users, values: [min_age: 21]}, hints: :test_index, as: :active_users, on: true]]}

%{join: [author: [as: :author], table: [source: "users", as: :users_table, on: true]]}

### Combined Examples

%{published: true, limit: 10, offset: 5}

%{group_by: :published, having: %{published: true}}

%{group_by: :views, having: %{views: %{>: 10}}, or_having: %{views: %{<: 5}}}

%{group_by: :id, having: %{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: "day"}}}}}}

%{where: %{title: "test"}, or_where: %{or: [[published: true, views: 20], [published: false, views: 10]]}}

%{where: [published: true, views: %{or: [>: 10, <: 5]}]}
