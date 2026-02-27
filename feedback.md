Data Structure rules and contraints:

- A user would only ever use enums. All cases where the data structure is modeled
using a tuple is wrong.

Do not do this:

```elixir
%{title: %{==: {:lower, "hello"}}}
```

Do this:

```
%{title: %{==: %{lower: "hello"}}}
```

Note that instead of `{:lower, "hello"}` a map of `%{lower: "hello"}` is used. When you use a tuple it's highlighting you are leaking implementation details.

## Corrected Inputs

This section contains the correct input shapes that should be supported. Use these to fix the bad assumptions:

### Argument 1: source

The source must be one of these types:

Every distinct shape the first argument accepts:

- `module()`
- `String.t()`
- `Ecto.Query.t()`
- `{String.t() | nil, module() | nil}`
- `{String.t(), module()}`

### Argument 2: params — Top-Level Container Shapes

Every distinct shape the second argument accepts at the outermost level:

- `%{id: 1}`
- `[id: 1]`
- `[%{id: 1}]`
- `[[id: 1]]`
- `[%{id: 1}, %{published: true}]`
- `[[id: 1], [published: true]]`

### Argument 2: params — Meta-Keys

These keys are extracted before any filtering occurs. Only valid inside a keyword list (or map converted to keyword list).

- `[source: Post, query: %{id: 1}]`
- `[source: Post, query: [id: 1]]`
- `[source: "posts", query: %{id: 1}]`
- `%{source: Post, query: %{id: 1}}`
- `[source: Post, id: 1]`
- `[query: %{id: 1}, published: true]`
- `[source: "posts", query: %{select: [:id]}]`

### Argument 2: params — Schema Filter Keys

- `%{where: %{published: true}}`
- `%{where: [published: true]}`
- `%{where: %{published: true, views: 10}}`
- `%{or_where: %{published: false}}`
- `%{or_where: [published: false]}`
- `%{where: %{published: true}, or_where: %{published: false}}`

### Argument 2: params — Binding Selector

Binding select must always appear in the order `:bind` -> `:as | :at` -> `:atom | integer()` -> `map() | keyword()`

- `%{bind: %{as: %{post: %{published: true}}}, author: %{first_name: "John"}}`
- `%{bind: %{as: %{post: %{published: true}}}}`
- `%{bind: %{as: [post: %{published: true}]}}`
- `%{bind: %{at: %{1 => %{published: true}}}}`
- `%{bind: %{at: [{1, %{published: true}}]}}`

### Argument 2: params — Field Values for Scalar Fields

Given a schema with scalar fields like `:id` (integer), `:title` (string), `:published` (boolean), `:views` (integer), `:published_at` (utc_datetime):

**Equality (implicit ==)**

- `%{id: 1}`
- `%{published: true}`
- `%{title: "hello"}`
- `%{published_at: ~U[2026-01-01 00:00:00Z]}`

**Explicit == operator**

- `%{id: %{==: 1}}`
- `%{id: %{eq: 1}}`

**Nil (IS NULL / IS NOT NULL)**

- `%{published_at: nil}`
- `%{published_at: %{==: nil}}`
- `%{published_at: %{eq: nil}}`
- `%{published_at: %{!=: nil}}`

**Comparison operators**

- `%{views: %{>: 10}}`
- `%{views: %{>=: 10}}`
- `%{views: %{<: 10}}`
- `%{views: %{<=: 10}}`
- `%{views: %{!=: 10}}`

**Comparison alias operators**

- `%{views: %{gt: 10}}`
- `%{views: %{gte: 10}}`
- `%{views: %{lt: 10}}`
- `%{views: %{lte: 10}}`

**IN (list value)**

- `%{published: [true, false]}`
- `%{published: %{in: [true, false]}}`
- `%{published: %{==: [true, false]}}`

**NOT IN**

- `%{published: %{!=: [true, false]}}`
- `%{published: %{not: %{in: [true, false]}}}`
- `%{published: %{not: %{==: [true, false]}}}`

**Negated operators**

- `%{views: %{not: %{>: 10}}}`
- `%{views: %{not: %{>=: 10}}}`
- `%{views: %{not: %{<: 10}}}`
- `%{views: %{not: %{<=: 10}}}`
- `%{views: %{not: %{==: 10}}}`
- `%{views: %{not: %{!=: 10}}}`
- `%{views: %{not: %{gt: 10}}}`
- `%{views: %{not: %{gte: 10}}}`
- `%{views: %{not: %{lt: 10}}}`
- `%{views: %{not: %{lte: 10}}}`

**Negated NOT IN → IN**

- `%{published: %{not: %{!=: [true, false]}}}`

**LIKE / ILIKE (scalar string)**

- `%{title: %{like: "hello"}}`
- `%{title: %{ilike: "hello"}}`

**LIKE / ILIKE (list of strings)**

- `%{title: %{like: ["hello", "world"]}}`
- `%{title: %{ilike: ["hello", "world"]}}`

**Negated LIKE / ILIKE**

- `%{title: %{not: %{like: "hello"}}}`
- `%{title: %{not: %{ilike: "hello"}}}`
- `%{title: %{not: %{like: ["hello", "world"]}}}`
- `%{title: %{not: %{ilike: ["hello", "world"]}}}`

**LOWER / UPPER (case-insensitive)**

- `%{title: %{lower: "hello"}}`
- `%{title: %{upper: "HELLO"}}`
- `%{title: %{==: %{lower: "hello"}}}`
- `%{title: %{==: %{upper: "HELLO"}}}`
- `%{title: %{!=: %{lower: "hello"}}}`
- `%{title: %{!=: %{upper: "HELLO"}}}`

**Negated LOWER / UPPER**

- `%{title: %{not: %{lower: "hello"}}}`
- `%{title: %{not: %{upper: "HELLO"}}}`

**Field-level boolean AND / OR**

- `%{views: %{and: [>: 10, <: 20]}}`
- `%{published: %{or: [==: true, ==: false]}}`
- `%{views: %{and: []}}`

**Multiple conditions on one field (keyword list value)**

- `%{published: [==: true, !=: false]}`

**Aggregate operators (typically used under :having)**

- `%{views: %{avg: %{>: 10}}}`
- `%{views: %{avg: %{>=: 10}}}`
- `%{views: %{avg: %{<: 10}}}`
- `%{views: %{avg: %{<=: 10}}}`
- `%{views: %{avg: %{==: 10}}}`
- `%{views: %{avg: %{!=: 10}}}`
- `%{views: %{avg: 10}}`
- `%{views: %{count: %{>: 10}}}`
- `%{views: %{max: %{>: 10}}}`
- `%{views: %{min: %{>: 10}}}`
- `%{views: %{sum: %{>: 10}}}`
- `%{views: %{or: [avg: %{>: 10}, avg: %{<: 20}]}}`

**Negated aggregate operators**

- `%{views: %{not: %{avg: %{>: 10}}}}`
- `%{views: %{not: %{avg: 10}}}`

**Aggregate with alias comparison operators**

- `%{views: %{avg: %{gt: 10}}}`
- `%{views: %{avg: %{gte: 10}}}`
- `%{views: %{avg: %{lt: 10}}}`
- `%{views: %{avg: %{lte: 10}}}`
- `%{views: %{avg: %{eq: 10}}}`

**Scalar :all subquery helper**

- `%{id: %{all: %{>: subquery_expr}}}`
- `%{id: %{all: %{>=: subquery_expr}}}`
- `%{id: %{all: %{<: subquery_expr}}}`
- `%{id: %{all: %{<=: subquery_expr}}}`
- `%{id: %{all: %{==: subquery_expr}}}`
- `%{id: %{all: %{!=: subquery_expr}}}`
- `%{id: %{all: subquery_expr}}`
- `%{id: %{all: %{>: %{source: Post, query: %{id: 1}}}}}`
- `%{id: %{all: %{>: [source: Post, query: %{id: 1}]}}}`

**Negated scalar :all**

- `%{id: %{not: %{all: %{>: subquery_expr}}}}`
- `%{id: %{not: %{all: subquery_expr}}}`

**Scalar :any subquery helper**

- `%{id: %{any: %{>: subquery_expr}}}`
- `%{id: %{any: subquery_expr}}`

**Negated scalar :any**

- `%{id: %{not: %{any: %{>: subquery_expr}}}}`
- `%{id: %{not: %{any: subquery_expr}}}`

**Arithmetic helper expressions**

- `%{views: %{>: %{+: [:views, 10]}}}`
- `%{views: %{>: %{-: [:views, 10]}}}`
- `%{views: %{>: %{*: [:views, 2]}}}`
- `%{views: %{>: %{/: [:views, 2]}}}`
- `%{views: %{>=: %{+: [:views, 10]}}}`
- `%{views: %{<: %{+: [:views, 10]}}}`
- `%{views: %{<=: %{+: [:views, 10]}}}`
- `%{views: %{==: %{+: [:views, 10]}}}`
- `%{views: %{!=: %{+: [:views, 10]}}}`

**Negated arithmetic**

- `%{views: %{not: %{>: %{+: [:views, 10]}}}}`

**Date/time helper expressions — :datetime_add**

- `%{inserted_at: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}`

- `%{inserted_at: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}`
- `%{inserted_at: %{not: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}`

**Date/time helper expressions — :date_add**

- `%{inserted_at: %{>=: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}`
- `%{inserted_at: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}`

**Date/time helper expressions — :ago**

- `%{inserted_at: %{>: %{ago: %{count: 1, interval: "day"}}}}`
- `%{inserted_at: %{ago: %{count: 1, interval: "day"}}}`

**Date/time helper expressions — :from_now**

- `%{inserted_at: %{>: %{from_now: %{count: 1, interval: "day"}}}}`
- `%{inserted_at: %{from_now: %{count: 1, interval: "day"}}}`

**Dynamic expressions**

- `%{dynamic: dynamic([p], p.views > ^10)}`
- `%{where: %{dynamic: dynamic([p], p.published == ^true)}}`
- `%{or_where: %{dynamic: dynamic([p], p.views > ^100)}}`

**Exists expressions**

- `%{where: %{exists: subquery_expr}}`
- `%{where: %{exists: %{not: subquery_expr}}}`

### Argument 2: params — Field Values for Array Fields

Given a schema with an array field like `:tags` (`{:array, :string}`):

**Scalar value (membership — "elixir" in tags)**

- `%{tags: "elixir"}`
- `%{tags: %{==: "elixir"}}`
- `%{tags: %{in: "elixir"}}`

**List value (array equality — tags == ["elixir", "erlang"])**

- `%{tags: ["elixir", "erlang"]}`
- `%{tags: %{==: ["elixir", "erlang"]}}`

**Array NOT equality**

- `%{tags: %{!=: ["elixir"]}}`
- `%{tags: %{not: %{==: ["elixir"]}}}`

**Nil**

- `%{tags: nil}`
- `%{tags: %{==: nil}}`
- `%{tags: %{!=: nil}}`

**Overlap (any element matches — && operator)**

- `%{tags: %{in: ["elixir"]}}`
- `%{tags: %{in: ["elixir", "erlang"]}}`

**NOT overlap**

- `%{tags: %{not: %{in: ["elixir"]}}}`

**Contains all (@> operator)**

- `%{tags: %{in: %{all: ["elixir", "erlang"]}}}`

**NOT contains all**

- `%{tags: %{not: %{in: %{all: ["elixir", "erlang"]}}}}`

**Scalar != (NOT membership)**

- `%{tags: %{!=: "elixir"}}`
- `%{tags: %{not: %{in: "elixir"}}}`

**Comparison operators (ANY semantics)**

- `%{tags: %{>: "elixir"}}`
- `%{tags: %{>=: "elixir"}}`
- `%{tags: %{<: "elixir"}}`
- `%{tags: %{<=: "elixir"}}`

**Negated comparison (NOT ANY)**

- `%{tags: %{not: %{>: "elixir"}}}`
- `%{tags: %{not: %{>=: "elixir"}}}`
- `%{tags: %{not: %{<: "elixir"}}}`
- `%{tags: %{not: %{<=: "elixir"}}}`

**LIKE / ILIKE (EXISTS + unnest)**

- `%{tags: %{like: "elixir"}}`
- `%{tags: %{ilike: "elixir"}}`
- `%{tags: %{like: ["elixir", "erlang"]}}`
- `%{tags: %{ilike: ["elixir", "erlang"]}}`

**Negated LIKE / ILIKE**

- `%{tags: %{not: %{like: "elixir"}}}`
- `%{tags: %{not: %{ilike: "elixir"}}}`
- `%{tags: %{not: %{like: ["elixir", "erlang"]}}}`
- `%{tags: %{not: %{ilike: ["elixir", "erlang"]}}}`

**LOWER / UPPER (EXISTS + unnest + lower/upper)**

- `%{tags: %{lower: "elixir"}}`
- `%{tags: %{upper: "ELIXIR"}}`
- `%{tags: %{==: %{lower: "elixir"}}}`
- `%{tags: %{==: %{upper: "ELIXIR"}}}`
- `%{tags: %{!=: %{lower: "elixir"}}}`
- `%{tags: %{!=: %{upper: "ELIXIR"}}}`

**Negated LOWER / UPPER**

- `%{tags: %{not: %{lower: "elixir"}}}`
- `%{tags: %{not: %{upper: "ELIXIR"}}}`

**Alias operators on arrays**

- `%{tags: %{gt: "elixir"}}`
- `%{tags: %{gte: "elixir"}}`
- `%{tags: %{lt: "elixir"}}`
- `%{tags: %{lte: "elixir"}}`
- `%{tags: %{eq: "elixir"}}`

**Aggregate operators on arrays**

- `%{tags: %{count: %{>: 0}}}`
- `%{tags: %{count: 0}}`

### Argument 2: params — Common/Custom Filter Keys

These are dispatched through the adapter's `@operators` list and have hardcoded field semantics:

- `%{ids: [1, 2, 3]}`
- `%{after: 10}`
- `%{before: 10}`
- `%{start_date: ~U[2026-01-01 00:00:00Z]}`
- `%{end_date: ~U[2026-12-31 23:59:59Z]}`

### Argument 2: params — Composite Boolean Operators (top-level :and / :or)

- `%{or: [[published: true, views: 20], [published: false, views: 10]]}`
- `%{and: [[published: true, views: 20], [title: "hello", views: 15]]}`
- `%{or: [[published: %{or: [==: true, ==: false]}], [published: %{or: [==: true, ==: false]}]]}`
- `%{or: [[published: true, views: 20]]}`
- `%{or: []}`
- `%{and: []}`
- `%{or: [[views: %{>: 10}, published: true], [views: %{<: 5}, published: false]]}`

### Argument 2: params — Association Auto-Join

When a key matches a schema association name (e.g. `:author`, `:comments`):

- `%{author: [as: :author, first_name: "John"]}`
- `%{author: [first_name: "John"]}`
- `%{author: [as: :author, on: true, first_name: "John"]}`
- `%{author: [as: :author, type: :left, first_name: "John"]}`

### Argument 2: params — Query Filter Keys

**:select**

- `%{select: true}`
- `%{select: :id}`
- `%{select: [:id, :title]}`
- `%{select: %{map: [:id, :title]}}`
- `%{select: %{map: %{custom_id: :id}}}`
- `%{select: %{struct: [:id]}}`

**:select_merge**

- `%{select_merge: %{map: %{custom_id: :id}}}`
- `%{select_merge: %{map: [:id, :title]}}`
- `[select: %{map: [:id]}, select_merge: %{map: %{post_title: :title}}]`

**:distinct**

- `%{distinct: true}`
- `%{distinct: false}`
- `%{distinct: :title}`
- `%{distinct: [desc: :title]}`
- `%{distinct: %{desc: :title}}`

**:group_by**

- `%{group_by: :author_id}`
- `%{group_by: [:author_id, :published]}`

**:having / :or_having**

- `%{having: %{published: true}}`
- `%{having: %{views: %{>: 10}}}`
- `%{having: %{views: %{avg: %{>: 10}}}}`
- `%{having: dynamic([p], p.views > ^10)}`
- `%{having: [and: [published: true, views: %{>: 10}]]}`
- `%{having: [or: [views: %{>: 10}, views: %{<: 5}]]}`
- `%{or_having: %{views: %{<: 5}}}`

**:order_by**

- `%{order_by: :title}`
- `%{order_by: [desc: :title]}`
- `%{order_by: [asc: :title, desc: :id]}`
- `%{order_by: %{desc: :title}}`

**:prepend_order_by**

- `%{prepend_order_by: :title}`
- `%{prepend_order_by: [asc: :published_at, desc: :title]}`

**:limit / :offset / :first**

- `%{limit: 10}`
- `%{offset: 5}`
- `%{first: 10}`
- `%{limit: 10, offset: 5}`

**:last**

- `%{last: 2}`
- `%{last: %{title: 2}}`
- `%{last: [title: 2]}`
- `%{last: {nil, 2}}`
- `%{last: {:id, 2}}`

**:exclude**

- `%{exclude: :order_by}`
- `%{exclude: [:order_by, :limit]}`

**:reverse_order**

- `%{reverse_order: true}`

**:put_query_prefix**

- `%{put_query_prefix: "tenant_a"}`
- `[put_query_prefix: "tenant_a", put_query_prefix: "tenant_b"]`

**:subquery**

- `%{subquery: %{id: 2}}`
- `%{subquery: [id: 2]}`
- `[published: true, subquery: %{id: 2}]`

**:except / :except_all**

- `%{except: %{published: false}}`
- `%{except: from(p in Post, where: p.published == ^false)}`
- `%{except_all: %{published: false}}`

**:intersect / :intersect_all**

- `%{intersect: %{published: false}}`
- `%{intersect_all: %{published: false}}`

**:union / :union_all**

- `%{union: %{published: false}}`
- `%{union_all: %{published: false}}`

**:lock**

- `%{lock: fn query -> from(p in query, lock: "FOR UPDATE") end}`
- `%{lock: %{name: :for_share, values: []}}`
- `%{lock: [name: :for_share, values: []]}`

**:recursive_ctes**

- `%{recursive_ctes: true}`
- `%{recursive_ctes: false}`

**:with_cte**

- `%{with_cte: [published_posts: [as: cte_query]]}`
- `%{with_cte: %{published_posts: %{as: cte_query}}}`
- `%{with_cte: [published_posts: [as: cte_query, materialized: false, operation: :all]]}`
- `%{with_cte: [published_posts: [as: %{source: Post, query: %{published: true}}]]}`
- `%{with_cte: [published_posts: [as: [source: Post, query: [id: 1]]]]}`

**:with_named_binding**

- `%{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}]}`
- `%{with_named_binding: %{author: %{join: [association: [source: :author, as: :author]]}}}`
- `%{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}, users_table: %{join: [table: [source: "users", as: :users_table, on: true]]}]}`

**:with_ties**

- `%{with_ties: true}`
- `%{with_ties: false}`
- `%{with_ties: %{bind: %{as: %{post: true}}}}`
- `%{with_ties: %{bind: %{at: %{1 => true}}}}`

**:update**

- `%{update: [set: [title: "After"], inc: [views: 1]]}`
- `%{update: %{set: %{title: "After"}}}`

**:windows**

- `%{windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]}`
- `%{windows: %{post_window: %{partition_by: :author_id, order_by: [desc: :inserted_at]}}}`

**:preload**

- `%{preload: :author}`
- `%{preload: [:author]}`
- `%{preload: [author: [:posts]]}`
- `%{preload: [bind: [as: [example: :author]]]}`
- `%{preload: [bind: [at: %{2 => :author}]]}`
- `%{preload: [bind: [at: %{2 => :author}], posts: [:comments]]}`
- `%{preload: [bind: [as: [author: :author]], posts: [:comments]]}`
- `%{preload: [bind: [as: [author: :author], at: %{2 => :author}], posts: [:comments]]}`

**:join**

- `%{join: [author: [as: :author]]}`
- `%{join: [schema: [source: User, as: :user_join, on: true]]}`
- `%{join: [table: [source: "users", as: :users_table, on: true]]}`
- `%{join: [query: [source: user_query, as: :adult_users, on: true]]}`
- `%{join: [subquery: [source: user_query, as: :name, on: true]]}`
- `%{join: [subquery: [source: [source: User, query: [age: [>=: 18]]], as: :name, on: true]]}`
- `%{join: [subquery: [source: [query: [published: true]], as: :name, on: true]]}`
- `%{join: [fragment: [source: %{name: :active_users, values: [min_age: 21]}, as: :active_users, on: true]]}`
- `%{join: [fragment: [source: %{name: :active_users, values: [min_age: 21]}, hints: :test_index, as: :active_users, on: true]]}`
- `%{join: [author: [as: :author], table: [source: "users", as: :users_table, on: true]]}`

### Argument 2: params — Binding Selector Combinations

The `:bind` key can wrap any of the above query filter or field params, targeting a specific binding:

**Named binding (:as) with field filters**

- `%{bind: %{as: %{post: %{published: true}}}}`
- `%{bind: %{as: %{post: %{published: %{==: true}}}}}`
- `%{bind: %{as: %{post: %{published: [true, false]}}}}`

**Named binding (:as) with query filters**

- `%{bind: %{as: %{post: %{select: :id}}}}`
- `%{bind: %{as: %{post: %{select_merge: %{map: %{custom_id: :id}}}}}}`
- `%{bind: %{as: %{author: %{order_by: %{asc: :first_name}}}}}`
- `%{bind: %{as: %{author: %{prepend_order_by: %{asc: :first_name}}}}}`
- `%{bind: %{as: %{author: %{group_by: :first_name}}}}`
- `%{bind: %{as: %{author: %{group_by: :first_name, having: %{first_name: "John"}}}}}`
- `%{bind: %{as: %{author: %{group_by: [:first_name, :age], having: [and: [first_name: "John", age: %{>: 30}]]}}}}`
- `%{bind: %{as: %{author: %{group_by: [:first_name, :age], or_having: %{age: %{>: 30}}}}}}`
- `%{bind: %{as: %{author: %{group_by: :first_name, having: dynamic([_p, a], a.first_name == ^"John")}}}}`
- `%{bind: %{as: %{author: %{windows: [author_window: [partition_by: :first_name, order_by: [asc: :age]]]}}}}`
- `%{bind: %{as: %{author: %{distinct: :first_name}}}}`
- `%{bind: %{as: %{author: %{update: [set: [first_name: dynamic_title]]}}}}`
- `%{bind: %{as: %{post: %{join: [author: [as: :author]]}}}}`
- `%{bind: %{as: %{post: %{subquery: %{id: 2}}}}}`

**Positional binding (:at) with field filters**

- `%{bind: %{at: %{1 => %{published: true}}}}`
- `%{bind: %{at: %{2 => %{first_name: "John"}}}}`

**Positional binding (:at) with query filters**

- `%{bind: %{at: %{2 => %{order_by: %{asc: :first_name}}}}}`
- `%{bind: %{at: %{2 => %{prepend_order_by: %{asc: :first_name}}}}}`
- `%{bind: %{at: %{2 => %{group_by: :first_name, having: dynamic([_p, a], a.first_name == ^"John")}}}}`
- `%{bind: %{at: %{2 => %{windows: [author_window: [partition_by: :first_name, order_by: [asc: :age]]]}}}}`
- `%{bind: %{at: %{2 => %{update: [set: [first_name: dynamic_title]]}}}}`

**Multiple bindings in one call**

- `%{bind: %{as: [post: %{published: true}, author: %{first_name: "John"}]}}`
