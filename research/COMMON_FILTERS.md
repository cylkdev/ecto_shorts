Run the tests with: `mix run test/examples/run.exs`

## Join Directives

- `:join`: explicit join
  - `:association`: (association join type)
  - `:schema`: (schema join type)
  - `:table`: (table join type)
  - `:query`: (query join type)
  - `:fragment`: (fragment join type)
  - `:on`: (join condition)
  - `:type`: (join type - :left, etc.)

### Examples

    [author: [as: :author, first_name: "John"]]
    [author: [first_name: "John"]]
    [author: [as: :author, on: true, first_name: "John"]]
    [author: [as: :author, type: :left, first_name: "John"]]
    [join: [author: [as: :author]]]
    [join: [schema: [source: User, as: :user_join, on: true]]]
    [join: [table: [source: "users", as: :users_table, on: true]]]
    [join: [query: [source: user_query, as: :named_users, on: true]]]
    [join: [subquery: [source: user_query, as: :named_users_subquery, on: true]]]
    [join: [subquery: [source: [from: [query: User, first_name: "John"]], as: :named_users_subquery, on: true]]]
    [join: [subquery: [source: [from: [published: true]], as: :published_posts_subquery, on: true]]]
    [join: [fragment: [source: [name: :active_posts, values: [min_views: 0]], as: :active_posts, on: true]]]
    [join: [fragment: [source: [name: :active_posts, values: [min_views: 0]], hints: :test_index, as: :active_posts, on: true]]]
    [join: [author: [as: :author], table: [source: "users", as: :users_table, on: true]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[author: [as: :author, first_name: "John"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the `:author` association as the named binding `:author` and filters by `as(:author).first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, where: as(:author).first_name == "John", select: p)`

test name: "Rule Statement 1: association join with named binding and filter"

**Rule Statement 2:**

**Given** filter params: `[author: [first_name: "John"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the `:author` association without a named binding and filters by `a.first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), where: a.first_name == "John", select: p)`

test name: "Rule Statement 2: association join without named binding"

**Rule Statement 3:**

**Given** filter params: `[author: [as: :author, on: true, first_name: "John"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the `:author` association as the named binding `:author` with `on: true` and filters by `as(:author).first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, on: true, where: as(:author).first_name == "John", select: p)`

test name: "Rule Statement 3: association join with explicit on condition"

**Rule Statement 4:**

**Given** filter params: `[author: [as: :author, type: :left, first_name: "John"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query applies a `left_join` for the `:author` association as the named binding `:author` and filters by `as(:author).first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, left_join: a in assoc(p, :author), as: :author, where: as(:author).first_name == "John", select: p)`

test name: "Rule Statement 4: association join with left join qualifier"

**Rule Statement 5:**

**Given** filter params: `[join: [author: [as: :author]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query adds the canonical association join for `:author` as the named binding `:author`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, select: p)`

test name: "Rule Statement 5: canonical association join"

**Rule Statement 6:**

**Given** filter params: `[join: [schema: [source: User, as: :user_join, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the schema source as the named binding `:user_join` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: u in EctoShorts.TestUser, as: :user_join, on: true, select: {p.title, u.first_name})`

test name: "Rule Statement 6: canonical schema join"

**Rule Statement 7:**

**Given** filter params: `[join: [table: [source: "users", as: :users_table, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the table source as the named binding `:users_table` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: u in "users", as: :users_table, on: true, select: {p.title, field(u, :first_name)})`

test name: "Rule Statement 7: canonical table join"

**Rule Statement 8:**

**Given** filter params: `[join: [query: [source: user_query, as: :named_users, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the provided query source as the named binding `:named_users` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `user_query = from(u in EctoShorts.TestUser, where: u.first_name == "John"); from(p in EctoShorts.TestPost, join: u in ^user_query, as: :named_users, on: true, select: {p.title, u.first_name})`

test name: "Rule Statement 8: canonical query join"

**Rule Statement 9:**

**Given** filter params: `[join: [subquery: [source: user_query, as: :named_users_subquery, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the provided query source as a subquery named binding `:named_users_subquery` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `user_query = from(u in EctoShorts.TestUser, where: u.first_name == "John"); from(p in EctoShorts.TestPost, join: u in subquery(user_query), as: :named_users_subquery, on: true, select: {p.title, u.first_name})`

test name: "Rule Statement 9: canonical subquery join"

**Rule Statement 10:**

**Given** filter params: `[join: [subquery: [source: [from: [query: User, first_name: "John"]], as: :named_users_subquery, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query builds the subquery source from the explicit `:from` payload and joins it as the named binding `:named_users_subquery` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `named_users_query = from(u in EctoShorts.TestUser, where: u.first_name == "John"); from(p in EctoShorts.TestPost, join: u in subquery(named_users_query), as: :named_users_subquery, on: true, select: {p.title, u.first_name})`

test name: "Rule Statement 10: canonical subquery join from filter params with explicit from"

**Rule Statement 11:**

**Given** filter params: `[join: [subquery: [source: [from: [published: true]], as: :published_posts_subquery, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query builds the subquery source from the current schema filter params and joins it as the named binding `:published_posts_subquery`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `pub_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p.id); from(p in EctoShorts.TestPost, join: s in subquery(pub_query), as: :published_posts_subquery, on: s.id == p.id, select: p)`

test name: "Rule Statement 11: canonical subquery join from current schema filter params"

**Rule Statement 12:**

**Given** filter params: `[join: [fragment: [source: [name: :active_posts, values: [min_views: 0]], as: :active_posts, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the fragment source as the named binding `:active_posts` with `on: ap.id == p.id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `active_posts_query = from(ap in fragment("SELECT id FROM posts WHERE views >= ?", ^0), select: %{id: field(ap, :id)}); from(p in EctoShorts.TestPost, join: ap in ^active_posts_query, as: :active_posts, on: ap.id == p.id, select: p)`

test name: "Rule Statement 12: canonical fragment join"

**Rule Statement 13:**

**Given** filter params: `[join: [fragment: [source: [name: :active_posts, values: [min_views: 0]], hints: :test_index, as: :active_posts, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the fragment source as the named binding `:active_posts` and applies the join hints  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `active_posts_query = from(ap in fragment("SELECT id FROM posts WHERE views >= ?", ^0), select: %{id: field(ap, :id)}); from(p in EctoShorts.TestPost, join: ap in ^active_posts_query, as: :active_posts, on: ap.id == p.id, hints: ["USE INDEX(test_index)"], select: p)`

test name: "Rule Statement 13: canonical fragment join with hints"

**Rule Statement 14:**

**Given** filter params: `[join: [author: [as: :author], table: [source: "users", as: :users_table, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query applies the `:author` association join first and then the table join for `"users"` in the same query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, join: u in "users", as: :users_table, on: true, select: {p.title, a.first_name, field(u, :first_name)})`

test name: "Rule Statement 14: canonical multiple joins"

---

## Query Modifier Directives

- `:with_ties`: include ties in limit
- `:update`: update operations
- `:windows`: window functions
- `:preload`: preload associations

### Examples

    [with_ties: true]
    [with_ties: false]
    [with_ties: [bind: [as: :post, value: true]]]
    [with_ties: [bind: [at: 1, value: true]]]
    [update: [set: [title: "After"], inc: [views: 1]]]
    [windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]]
    [preload: :author]
    [preload: [:author]]
    [preload: [author: [:posts]]]
    [preload: [bind: [as: :example, value: :author]]]
    [preload: [bind: [at: 2, value: :author]]]
    [preload: [bind: [at: 2, value: :author], posts: [:comments]]]
    [preload: [bind: [as: :author, value: :author], posts: [:comments]]]
    [preload: [bind: [[as: :author, value: :author], [at: 2, value: :author]], posts: [:comments]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[with_ties: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query enables `with_ties(true)` on a limited descending `:views` query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1) |> with_ties(true)`

test name: "Rule Statement 1: with_ties true"

**Rule Statement 2:**

**Given** filter params: `[with_ties: false]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query disables `with_ties` on a limited descending `:views` query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1) |> with_ties(false)`

test name: "Rule Statement 2: with_ties false"

**Rule Statement 3:**

**Given** filter params: `[with_ties: [bind: [as: :post, value: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query enables `with_ties(true)` while preserving the named `:post` binding  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, as: :post, order_by: [desc: p.views], limit: 1) |> with_ties(true)`

test name: "Rule Statement 3: with_ties named binding"

**Rule Statement 4:**

**Given** filter params: `[with_ties: [bind: [at: 1, value: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query enables `with_ties(true)` while preserving the positional binding selector  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1) |> with_ties(true)`

test name: "Rule Statement 4: with_ties positional binding"

**Rule Statement 5:**

**Given** filter params: `[update: [set: [title: "After"], inc: [views: 1]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query adds the `set` and `inc` update operations to the filtered update query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.id == ^post1.id, update: [set: [title: "After"], inc: [views: 1]])`

test name: "Rule Statement 5: update set and inc"

**Rule Statement 6:**

**Given** filter params: `[windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query defines the `post_window` window with the provided partition and ordering fields  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, windows: [post_window: [partition_by: p.author_id, order_by: [desc: p.inserted_at]]], select: {p.title, over(count(p.id), :post_window)})`

test name: "Rule Statement 6: windows partition_by and order_by"

**Rule Statement 7:**

**Given** filter params: `[preload: :author]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query preloads the `:author` association by name  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.title == "Post", preload: :author, select: p)`

test name: "Rule Statement 7: preload atom"

**Rule Statement 8:**

**Given** filter params: `[preload: [:author]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query preloads the `:author` association from a list payload  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.title == "Post", preload: [:author], select: p)`

test name: "Rule Statement 8: preload list with atom"

**Rule Statement 9:**

**Given** filter params: `[preload: [author: [:posts]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query preloads the `:author` association with nested `:posts`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.title == "Post", preload: [author: [:posts]], select: p)`

test name: "Rule Statement 9: preload nested associations"

**Rule Statement 10:**

**Given** filter params: `[preload: [bind: [as: :example, value: :author]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query preloads the `:author` association from the named `:example` binding  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.title == "Post 1", join: a in assoc(p, :author), as: :example, preload: [author: a], select: p)`

test name: "Rule Statement 10: preload from named binding"

**Rule Statement 11:**

**Given** filter params: `[preload: [bind: [at: 2, value: :author]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query preloads the `:author` association from the positional binding  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.title == "Post 1", join: a in assoc(p, :author), preload: [author: a], select: p)`

test name: "Rule Statement 11: preload from positional binding"

**Rule Statement 12:**

**Given** filter params: `[preload: [bind: [at: 2, value: :author], posts: [:comments]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query preloads the `:author` association from the positional binding with nested `posts: [:comments]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.title == "Post", join: a in assoc(p, :author), preload: [author: {a, [posts: [:comments]]}], select: p)`

test name: "Rule Statement 12: preload from binding and nested"

**Rule Statement 13:**

**Given** filter params: `[preload: [bind: [as: :author, value: :author], posts: [:comments]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query preloads the `:author` association from the named `:author` binding with nested `posts: [:comments]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.title == "Post", join: a in assoc(p, :author), as: :author, preload: [author: {a, [posts: [:comments]]}], select: p)`

test name: "Rule Statement 13: preload from named binding and nested"

**Rule Statement 14:**

**Given** filter params: `[preload: [bind: [[as: :author, value: :author], [at: 2, value: :author]], posts: [:comments]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier directives  
**And** it must check whether the query preloads the `:author` association from the named and positional bindings with shared nested `posts: [:comments]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.title == "Post", join: a in assoc(p, :author), as: :author, preload: [author: {a, [posts: [:comments]]}], preload: [author: {a, [posts: [:comments]]}], select: p)`

test name: "Rule Statement 14: preload from multiple bindings and nested"

---

## Named Binding Directives

- `:with_named_binding`: create named binding

### Examples

    [with_named_binding: [author: [join: [association: [source: :author, as: :author]]]]]
    [with_named_binding: [author: [join: [association: [source: :author, as: :author]]], users_table: [join: [table: [source: "users", as: :users_table, on: true]]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[with_named_binding: [author: [join: [association: [source: :author, as: :author]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided named binding directives  
**And** it must check whether the query adds the `:author` named binding and filters by `as(:author).first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, where: as(:author).first_name == "John")`

test name: "Rule Statement 1: with_named_binding single named binding"

**Rule Statement 2:**

**Given** filter params: `[with_named_binding: [author: [join: [association: [source: :author, as: :author]]], users_table: [join: [table: [source: "users", as: :users_table, on: true]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided named binding directives  
**And** it must check whether the query adds the `:author` and `:users_table` named bindings and returns the matching joined user row  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, join: u in "users", as: :users_table, on: u.id == p.author_id, where: as(:author).first_name == "John", select: {p.title, field(u, :first_name)})`

test name: "Rule Statement 2: with_named_binding multiple named bindings"

---

## CTE Directives

- `:recursive_ctes`: enable recursive CTEs
- `:with_cte`: define CTE

### Examples

    [recursive_ctes: true]
    [recursive_ctes: false]
    [with_cte: [published_posts: [as: cte_query]]]
    [with_cte: [published_posts: [as: cte_query, materialized: false, operation: :all]]]
    [with_cte: [published_posts: [as: [from: [query: Post, published: true]]]]]
    [with_cte: [target_post: [as: [from: [query: Post, id: 1]]]]]
    [recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[recursive_ctes: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query enables recursive CTEs before applying the CTE definition  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p); EctoShorts.TestPost |> recursive_ctes(true) |> with_cte("published_posts", as: ^cte_query)`

test name: "Rule Statement 1: recursive_ctes true"

**Rule Statement 2:**

**Given** filter params: `[recursive_ctes: false]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query remains non-recursive when recursive CTEs are disabled  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost)`

test name: "Rule Statement 2: recursive_ctes false"

**Rule Statement 3:**

**Given** filter params: `[with_cte: [published_posts: [as: cte_query]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query defines the `published_posts` CTE from the provided query and filters published posts from the base query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p); EctoShorts.TestPost |> from(as: :post) |> with_cte("published_posts", as: ^cte_query) |> where([p], p.published == true)`

test name: "Rule Statement 3: with_cte basic"

**Rule Statement 4:**

**Given** filter params: `[with_cte: [published_posts: [as: cte_query, materialized: false, operation: :all]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query defines the `published_posts` CTE as not materialized and filters published posts from the base query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p); EctoShorts.TestPost |> with_cte("published_posts", as: ^cte_query, materialized: false) |> where([p], p.published == true)`

test name: "Rule Statement 4: with_cte not materialized"

**Rule Statement 5:**

**Given** filter params: `[with_cte: [published_posts: [as: [from: [query: Post, published: true]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query builds the `published_posts` CTE from the provided filter params and filters published posts from the base query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p); EctoShorts.TestPost |> with_cte("published_posts", as: ^cte_query) |> where([p], p.published == true)`

test name: "Rule Statement 5: with_cte from filter params published true"

**Rule Statement 6:**

**Given** filter params: `[with_cte: [target_post: [as: [from: [query: Post, id: 1]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query builds the `target_post` CTE from the provided filter params and filters the matching post from the base query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `cte_query = from(p in EctoShorts.TestPost, where: p.id == ^post1.id, select: p); EctoShorts.TestPost |> with_cte("target_post", as: ^cte_query) |> where([p], p.id == ^post1.id)`

test name: "Rule Statement 6: with_cte from filter params id"

**Rule Statement 7:**

**Given** filter params: `[recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query enables recursive CTEs and defines the `published_posts` CTE from the provided query before filtering published posts  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `cte_query = from(p in EctoShorts.TestPost, where: p.published == true, select: p); EctoShorts.TestPost |> recursive_ctes(true) |> with_cte("published_posts", as: ^cte_query) |> where([p], p.published == true)`

test name: "Rule Statement 7: recursive_ctes with cte"

---

## Lock Directives

- `:lock`: query locking

### Examples

    [lock: fn query -> from(p in query, lock: "FOR UPDATE") end]
    [lock: [name: :for_share]]
    [lock: [name: :for_update_with_clause, values: [clause: "SKIP LOCKED"]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[lock: fn query -> from(p in query, lock: "FOR UPDATE") end]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided lock directives  
**And** it must check whether the query uses `lock: "FOR UPDATE"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, lock: "FOR UPDATE")`

test name: "Rule Statement 1: lock for update"

**Rule Statement 2:**

**Given** filter params: `[lock: [name: :for_share]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided lock directives  
**And** it must check whether the query uses `lock: "FOR SHARE"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, lock: "FOR SHARE")`

test name: "Rule Statement 2: lock for share"

**Rule Statement 3:**

**Given** filter params: `[lock: [name: :for_update_with_clause, values: [clause: "SKIP LOCKED"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided lock directives  
**And** it must check whether the query uses `lock: "FOR UPDATE SKIP LOCKED"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, lock: "FOR UPDATE SKIP LOCKED")`

test name: "Rule Statement 3: lock with values"

---

## Set Directives

- `:except`: set except
- `:except_all`: set except all
- `:intersect`: set intersect
- `:intersect_all`: set intersect all
- `:union`: set union
- `:union_all`: set union all

### Examples

    [except: [published: false]]
    [except: from(p in EctoShorts.TestPost, where: p.published == ^false)]
    [except_all: [published: false]]
    [intersect: [published: false]]
    [intersect_all: [published: false]]
    [union: [published: false]]
    [union_all: [published: false]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[except: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `except/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `except_query = from(q in EctoShorts.TestPost, where: q.published == false); from(p in EctoShorts.TestPost) |> except(^except_query)`

test name: "Rule Statement 1: except with filter params"

**Rule Statement 2:**

**Given** filter params: `[except: from(p in EctoShorts.TestPost, where: p.published == ^false)]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must use the provided raw query exactly and apply it with `except/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `except_query = from(p in EctoShorts.TestPost, where: p.published == ^false); from(p in EctoShorts.TestPost) |> except(^except_query)`

test name: "Rule Statement 2: except with raw query"

**Rule Statement 3:**

**Given** filter params: `[except_all: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `except_all/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `except_query = from(q in EctoShorts.TestPost, where: q.published == false); from(p in EctoShorts.TestPost) |> except_all(^except_query)`

test name: "Rule Statement 3: except_all with filter params"

**Rule Statement 4:**

**Given** filter params: `[intersect: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `intersect/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `intersect_query = from(q in EctoShorts.TestPost, where: q.published == false); from(p in EctoShorts.TestPost) |> intersect(^intersect_query)`

test name: "Rule Statement 4: intersect with filter params"

**Rule Statement 5:**

**Given** filter params: `[intersect_all: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `intersect_all/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `intersect_query = from(q in EctoShorts.TestPost, where: q.published == false); from(p in EctoShorts.TestPost) |> intersect_all(^intersect_query)`

test name: "Rule Statement 5: intersect_all with filter params"

**Rule Statement 6:**

**Given** filter params: `[union: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(p in EctoShorts.TestPost, where: p.published == false)` from the provided filter params and apply it with `union/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `query1 = from(p in EctoShorts.TestPost, where: p.published == true); query2 = from(p in EctoShorts.TestPost, where: p.published == false); union(query1, ^query2)`

test name: "Rule Statement 6: union with filter params"

**Rule Statement 7:**

**Given** filter params: `[union_all: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(p in EctoShorts.TestPost, where: p.published == false)` from the provided filter params and apply it with `union_all/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `query1 = from(p in EctoShorts.TestPost, where: p.published == true); query2 = from(p in EctoShorts.TestPost, where: p.published == false); union_all(query1, ^query2)`

test name: "Rule Statement 7: union_all with filter params"

---

## Query Configuration Directives

- `:where`: explicit WHERE clause
- `:or_where`: OR WHERE clause
- `:from`: specify source schema/table
- `:subquery`: wrap the query in a subquery
- `:select`: select fields
- `:select_merge`: merge additional selections
- `:distinct`: distinct results
- `:group_by`: group by fields
- `:having`: having clause
- `:or_having`: OR having clause
- `:order_by`: order results
- `:prepend_order_by`: prepend to existing order
- `:limit`: limit results
- `:offset`: skip results
- `:first`: limit from start
- `:after`: pagination after
- `:before`: pagination before
- `:reverse_order`: reverse ordering
- `:exclude`: exclude query parts
- `:put_query_prefix`: set schema prefix
- `:start_date`: filter by start date
- `:end_date`: filter by end date
- `:ids`: filter by IDs
- `:dynamic`: raw dynamic expression
- `:exists`: exists subquery

### Examples

    [from: Post, id: 1, published: true]
    [select: true]
    [select: :id]
    [select: [:id, :title]]
    [select: [map: [:id, :title]]]
    [select: [map: [custom_id: :id]]]
    [select: [struct: [:id]]]
    [distinct: true]
    [group_by: :author_id]
    [having: [published: true]]
    [order_by: :title]
    [order_by: [desc: :title]]
    [limit: 10]
    [offset: 5]
    [limit: 10, offset: 5]
    [start_date: ~N[2026-01-01 00:00:00]]
    [end_date: ~N[2026-12-31 23:59:59]]
    [ids: [1, 2, 3]]
    [dynamic: dynamic([p], p.views > ^10)]
    [where: [dynamic: dynamic([p], p.published === ^true)]]
    [or_where: [dynamic: dynamic([p], p.views > ^100)]]
    [where: [exists: subquery_expr]]
    [where: [not: [exists: subquery_expr]]]
    [published: true, subquery: [id: 2]]
    [from: Post, id: 1]
    [from: "posts", id: 1]
    [from: "posts", select: [:id]]
    [select_merge: [map: [custom_id: :id]]]
    [select_merge: [map: [:id, :title]]]
    [select: [map: [:id]], select_merge: [map: [post_title: :title]]]
    [distinct: false]
    [distinct: :title]
    [distinct: [desc: :title]]
    [distinct: :title, order_by: :id]
    [group_by: [:author_id, :published]]
    [having: [views: [>: 10]]]
    [having: [views: [avg: [>: 10]]]]
    [having: dynamic([p], p.views > ^10)]
    [having: [and: [published: true, views: [>: 10]]]]
    [having: [or: [views: [>: 10], views: [<: 5]]]]
    [having: [not: [views: [>: 10]]]]
    [or_having: [views: [<: 5]]]
    [or_having: [views: [avg: [<: 5]]]]
    [order_by: [asc: :title, desc: :id]]
    [prepend_order_by: :title]
    [prepend_order_by: [asc: :published_at, desc: :title]]
    [after: 10]
    [before: 10]
    [first: 10]
    [reverse_order: true]
    [exclude: :order_by]
    [exclude: [:order_by, :limit]]
    [put_query_prefix: "tenant_a"]
    [put_query_prefix: "tenant_a", put_query_prefix: "tenant_b"]

### Rule Statements

**Rule Statement 7:**

**Given** filter params: `[from: Post, id: 1, published: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `from(p in EctoShorts.TestPost, where: p.id == 1 and p.published == true)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.id == 1 and p.published == true)`

test name: "Rule Statement 7: from schema with filters"

**Rule Statement 11:**

**Given** filter params: `[select: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: p`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: p`

test name: "Rule Statement 11: select true selects all fields"

**Rule Statement 12:**

**Given** filter params: `[select: :id]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: p.id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: p.id`

test name: "Rule Statement 12: select single field"

**Rule Statement 13:**

**Given** filter params: `[select: [:id, :title]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: [p.id, p.title]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: [p.id, p.title]`

test name: "Rule Statement 13: select list of fields"

**Rule Statement 14:**

**Given** filter params: `[select: [map: [:id, :title]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: %{id: p.id, title: p.title}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: %{id: p.id, title: p.title}`

test name: "Rule Statement 14: select map of fields"

**Rule Statement 15:**

**Given** filter params: `[select: [map: [custom_id: :id]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: %{custom_id: p.id}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: %{custom_id: p.id}`

test name: "Rule Statement 15: select map with renamed key"

**Rule Statement 16:**

**Given** filter params: `[select: [struct: [:id]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: struct(p, [:id])`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: struct(p, [:id])`

test name: "Rule Statement 16: select struct"

**Rule Statement 20:**

**Given** filter params: `[distinct: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: true`

test name: "Rule Statement 20: distinct true"

**Rule Statement 25:**

**Given** filter params: `[group_by: :author_id]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `group_by: p.author_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `group_by: p.author_id`

test name: "Rule Statement 25: group by single field"

**Rule Statement 27:**

**Given** filter params: `[having: [published: true]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: p.published == true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: p.published == true`

test name: "Rule Statement 27: having clause with equality"

**Rule Statement 34:**

**Given** filter params: `[order_by: :title]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `order_by: [asc: p.title]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `order_by: [asc: p.title]`

test name: "Rule Statement 34: order by field ascending"

**Rule Statement 35:**

**Given** filter params: `[order_by: [desc: :title]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `order_by: [desc: p.title]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `order_by: [desc: p.title]`

test name: "Rule Statement 35: order by field descending"

**Rule Statement 41:**

**Given** filter params: `[limit: 10]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `limit: 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `limit: 10`

test name: "Rule Statement 41: limit results"

**Rule Statement 42:**

**Given** filter params: `[offset: 5]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `offset: 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `offset: 5`

test name: "Rule Statement 42: offset results"

**Rule Statement 44:**

**Given** filter params: `[limit: 10, offset: 5]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `limit: 10, offset: 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `limit: 10, offset: 5`

test name: "Rule Statement 44: limit and offset"

**Rule Statement 50:**

**Given** filter params: `[start_date: ~N[2026-01-01 00:00:00]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: p.inserted_at >= ^start_date`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: p.inserted_at >= ^start_date`

test name: "Rule Statement 50: start_date filter"

**Rule Statement 51:**

**Given** filter params: `[end_date: ~N[2026-12-31 23:59:59]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: p.inserted_at <= ^end_date`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: p.inserted_at <= ^end_date`

test name: "Rule Statement 51: end_date filter"

**Rule Statement 52:**

**Given** filter params: `[ids: [1, 2, 3]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: p.id in [1, 2, 3]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: p.id in [1, 2, 3]`

test name: "Rule Statement 52: ids filter"

**Rule Statement 1:**

**Given** filter params: `[dynamic: dynamic([p], p.views > ^10)]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `dynamic([p], p.views > ^10)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `dynamic([p], p.views > ^10)`

test name: "Rule Statement 1: raw dynamic expression"

**Rule Statement 2:**

**Given** filter params: `[where: [dynamic: dynamic([p], p.published === ^true)]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: ^dynamic([p], p.published == ^true)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: ^dynamic([p], p.published == ^true)`

test name: "Rule Statement 2: dynamic within where clause"

**Rule Statement 3:**

**Given** filter params: `[or_where: [dynamic: dynamic([p], p.views > ^100)]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `or_where: ^dynamic([p], p.views > ^100)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `or_where: ^dynamic([p], p.views > ^100)`

test name: "Rule Statement 3: dynamic within or_where clause"

**Rule Statement 4:**

**Given** filter params: `[where: [exists: subquery_expr]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: exists(subquery_expr)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: exists(subquery_expr)`

test name: "Rule Statement 4: exists subquery within where clause"

**Rule Statement 5:**

**Given** filter params: `[where: [not: [exists: subquery_expr]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: not exists(subquery_expr)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: not exists(subquery_expr)`

test name: "Rule Statement 5: negated exists subquery within where clause"

**Rule Statement 6:**

**Given** filter params: `[published: true, subquery: [id: 2]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `from(p in subquery(from(q in EctoShorts.TestPost, where: q.published == true and q.id == 2)))`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in subquery(from(q in EctoShorts.TestPost, where: q.published == true and q.id == 2)))`

test name: "Rule Statement 6: published and subquery"

**Rule Statement 8:**

**Given** filter params: `[from: Post, id: 1]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `from(p in EctoShorts.TestPost, where: p.id == 1)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.id == 1)`

test name: "Rule Statement 8: from Post with id filter"

**Rule Statement 9:**

**Given** filter params: `[from: "posts", id: 1]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `from(p in "posts", where: p.id == 1, select: p.id)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in "posts", where: p.id == 1, select: p.id)`

test name: "Rule Statement 9: from table string with id filter"

**Rule Statement 10:**

**Given** filter params: `[from: "posts", select: [:id]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `from(p in "posts", select: p.id)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in "posts", select: p.id)`

test name: "Rule Statement 10: from table string with select"

**Rule Statement 17:**

**Given** filter params: `[select_merge: [map: [custom_id: :id]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select_merge: %{custom_id: p.id}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select_merge: %{custom_id: p.id}`

test name: "Rule Statement 17: select_merge map with renamed key"

**Rule Statement 18:**

**Given** filter params: `[select_merge: [map: [:id, :title]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select_merge: %{id: p.id, title: p.title}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select_merge: %{id: p.id, title: p.title}`

test name: "Rule Statement 18: select_merge map with field list"

**Rule Statement 19:**

**Given** filter params: `[select: [map: [:id]], select_merge: [map: [post_title: :title]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: %{id: p.id}, select_merge: %{post_title: p.title}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: %{id: p.id}, select_merge: %{post_title: p.title}`

test name: "Rule Statement 19: select then select_merge"

**Rule Statement 21:**

**Given** filter params: `[distinct: false]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: false`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: false`

test name: "Rule Statement 21: distinct false"

**Rule Statement 22:**

**Given** filter params: `[distinct: :title]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: p.title`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: p.title`

test name: "Rule Statement 22: distinct on field"

**Rule Statement 23:**

**Given** filter params: `[distinct: [desc: :title]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: [desc: p.title]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: [desc: p.title]`

test name: "Rule Statement 23: distinct on field descending"

**Rule Statement 24:**

**Given** filter params: `[distinct: :title, order_by: :id]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: p.title, order_by: p.id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: p.title, order_by: p.id`

test name: "Rule Statement 24: distinct with order_by"

**Rule Statement 26:**

**Given** filter params: `[group_by: [:author_id, :published]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `group_by: [p.author_id, p.published]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `group_by: [p.author_id, p.published]`

test name: "Rule Statement 26: group by multiple fields"

**Rule Statement 28:**

**Given** filter params: `[having: [views: [>: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: p.views > 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: p.views > 10`

test name: "Rule Statement 28: having views greater than"

**Rule Statement 29:**

**Given** filter params: `[having: [views: [avg: [>: 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: avg(p.views) > 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: avg(p.views) > 10`

test name: "Rule Statement 29: having avg views greater than"

**Rule Statement 30:**

**Given** filter params: `[having: dynamic([p], p.views > ^10)]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: ^dynamic([p], p.views > ^10)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: ^dynamic([p], p.views > ^10)`

test name: "Rule Statement 30: having dynamic expression"

**Rule Statement 31:**

**Given** filter params: `[having: [and: [published: true, views: [>: 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: p.published == true and p.views > 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: p.published == true and p.views > 10`

test name: "Rule Statement 31: having and"

**Rule Statement 32:**

**Given** filter params: `[having: [or: [views: [>: 10], views: [<: 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: p.views > 10 or p.views < 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: p.views > 10 or p.views < 5`

test name: "Rule Statement 32: having or"

**Rule Statement 32A:**

**Given** filter params: `[having: [not: [views: [>: 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: not (p.views > 10)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: not (p.views > 10)`

test name: "Rule Statement 32A: having not views greater than"

**Rule Statement 33:**

**Given** filter params: `[or_having: [views: [<: 5]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `or_having: p.views < 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `or_having: p.views < 5`

test name: "Rule Statement 33: or_having"

**Rule Statement 33A:**

**Given** filter params: `[or_having: [views: [avg: [<: 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `or_having: avg(p.views) < 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `or_having: avg(p.views) < 5`

test name: "Rule Statement 33A: or_having avg views less than"

**Rule Statement 36:**

**Given** filter params: `[order_by: [asc: :title, desc: :id]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `order_by: [asc: p.title, desc: p.id]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `order_by: [asc: p.title, desc: p.id]`

test name: "Rule Statement 36: order by asc and desc"

**Rule Statement 37:**

**Given** filter params: `[prepend_order_by: :title]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `prepend_order_by([p], desc: p.title)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `prepend_order_by([p], desc: p.title)`

test name: "Rule Statement 37: prepend_order_by single field"

**Rule Statement 38:**

**Given** filter params: `[prepend_order_by: [asc: :published_at, desc: :title]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `prepend_order_by([p], asc: p.published_at, desc: p.title)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `prepend_order_by([p], asc: p.published_at, desc: p.title)`

test name: "Rule Statement 38: prepend_order_by multiple fields"

**Rule Statement 39:**

**Given** filter params: `[after: 10]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `offset: 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `offset: 10`

test name: "Rule Statement 39: after cursor"

**Rule Statement 40:**

**Given** filter params: `[before: 10]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `limit: 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `limit: 10`

test name: "Rule Statement 40: before cursor"

**Rule Statement 43:**

**Given** filter params: `[first: 10]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `limit: 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `limit: 10`

test name: "Rule Statement 43: first N records"

**Rule Statement 45:**

**Given** filter params: `[reverse_order: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `reverse_order(query)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `reverse_order(query)`

test name: "Rule Statement 45: reverse order"

**Rule Statement 46:**

**Given** filter params: `[exclude: :order_by]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `exclude(query, :order_by)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `exclude(query, :order_by)`

test name: "Rule Statement 46: exclude order_by"

**Rule Statement 47:**

**Given** filter params: `[exclude: [:order_by, :limit]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `query |> exclude(:order_by) |> exclude(:limit)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `query |> exclude(:order_by) |> exclude(:limit)`

test name: "Rule Statement 47: exclude order_by and limit"

**Rule Statement 48:**

**Given** filter params: `[put_query_prefix: "tenant_a"]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `put_query_prefix(query, "tenant_a")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `put_query_prefix(query, "tenant_a")`

test name: "Rule Statement 48: put_query_prefix single tenant"

**Rule Statement 49:**

**Given** filter params: `[put_query_prefix: "tenant_a", put_query_prefix: "tenant_b"]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `query |> put_query_prefix("tenant_a") |> put_query_prefix("tenant_b")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `query |> put_query_prefix("tenant_a") |> put_query_prefix("tenant_b")`

test name: "Rule Statement 49: put_query_prefix last wins"

---

## Terminal Filter Directives

- `:last`: Returns the last N records by reversing order, limiting, then re-ordering
- `:subquery`: Wraps the query and any additional filters in a subquery

### Examples

    [last: 2]
    [subquery: [title: "Second"]]
    [published: true, subquery: [title: "Match"]]
    [last: [title: 2]]
    [subquery: [published: true, views: [>: 10]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[last: 2]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `:last` terminal directive  
**And** it must check whether the query returns the last `2` records by reversing the default `:id` order, limiting, then re-ordering ascending  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in subquery(from(q in EctoShorts.TestPost, order_by: [desc: q.id], limit: 2)), order_by: [asc: p.id])`

test name: "Rule Statement 1: last 2 records"

**Rule Statement 2:**

**Given** filter params: `[subquery: [title: "Second"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `:subquery` terminal directive  
**And** it must check whether the query wraps the `:title == "Second"` filter in a subquery  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in subquery(from(q in EctoShorts.TestPost, where: q.title == "Second")))`

test name: "Rule Statement 2: subquery with title filter"

**Rule Statement 3:**

**Given** filter params: `[published: true, subquery: [title: "Match"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `:subquery` terminal directive after applying the top-level filter conditions  
**And** it must check whether the query wraps the `:published == true` and `:title == "Match"` filters in a subquery  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in subquery(from(q in EctoShorts.TestPost, where: q.published == true and q.title == "Match")))`

test name: "Rule Statement 3: published true and subquery with title filter"

**Rule Statement 4:**

**Given** filter params: `[last: [title: 2]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `:last` terminal directive using the provided sort field  
**And** it must check whether the query returns the last `2` records by reversing the `:title` order, limiting, then re-ordering ascending  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in subquery(from(q in EctoShorts.TestPost, order_by: [desc: q.title], limit: 2)), order_by: [asc: p.title])`

test name: "Rule Statement 4: last 2 records by title"

**Rule Statement 5:**

**Given** filter params: `[subquery: [published: true, views: [>: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `:subquery` terminal directive  
**And** it must check whether the query wraps the `:published == true` and `:views > 10` filters in a subquery  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in subquery(from(q in EctoShorts.TestPost, where: q.published == true and q.views > 10)))`

test name: "Rule Statement 5: subquery with published true and views greater than 10"

---

## Binding Selector Directives

- `:bind`: specify binding context
  - `:as`: named binding
  - `:at`: positional binding

### Examples

    [bind: [as: :post, published: true]]
    [bind: [[as: :post, published: true], [as: :comment, body: "hi"]]]
    [bind: [[as: :post, published: true], [as: :author, first_name: "John"]]]
    [bind: [at: 1, published: true]]
    [bind: [[at: 1, published: true]]]
    [bind: [at: :first, published: true]]
    [bind: [at: :last, title: "Published"]]
    [bind: [at: :last, first_name: "John"]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[bind: [as: :post, published: true]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the named binding selector  
**And** it must check whether the binding named `:post` has `:published == true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `as(:post).published == true`

test name: "Rule Statement 1: named binding as :post"

**Rule Statement 2:**

**Given** filter params: `[bind: [[as: :post, published: true], [as: :comment, body: "hi"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the named binding selector across the binding conditions  
**And** it must check whether the binding named `:post` has `:published == true` and the binding named `:comment` has `:body == "hi"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `as(:post).published == true and as(:comment).body == "hi"`

test name: "Rule Statement 2: multiple named bindings with comment"

**Rule Statement 3:**

**Given** filter params: `[bind: [[as: :post, published: true], [as: :author, first_name: "John"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the named binding selector across the binding conditions  
**And** it must check whether the binding named `:post` has `:published == true` and the binding named `:author` has `:first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `as(:post).published == true and as(:author).first_name == "John"`

test name: "Rule Statement 3: multiple named bindings with author"

**Rule Statement 4:**

**Given** filter params: `[bind: [at: 1, published: true]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the positional binding selector  
**And** it must check whether the binding at index `1` has `:published == true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published == true`

test name: "Rule Statement 4: positional binding at index 1"

**Rule Statement 5:**

**Given** filter params: `[bind: [[at: 1, published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the positional binding selector from the list-wrapped form  
**And** it must check whether the binding at index `1` has `:published == true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published == true`

test name: "Rule Statement 5: positional binding at index 1 list-wrapped"

**Rule Statement 6:**

**Given** filter params: `[bind: [at: :first, published: true]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the positional binding selector  
**And** it must check whether the first binding has `:published == true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published == true`

test name: "Rule Statement 6: first binding"

**Rule Statement 7:**

**Given** filter params: `[bind: [at: :last, title: "Published"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the positional binding selector  
**And** it must check whether the last binding has `:title == "Published"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "Published"`

test name: "Rule Statement 7: last binding without join"

**Rule Statement 8:**

**Given** filter params: `[bind: [at: :last, first_name: "John"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the positional binding selector  
**And** it must check whether the last binding has `:first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `a.first_name == "John"`

test name: "Rule Statement 8: last binding with author first name"

---

## Date/Time Directives

- `:datetime` or `:date`: Wrapper for date/time operations. Both use the same set of sub-keys.
  - `:add`: add time interval
  - `:ago`: time in the past
  - `:from_now`: time in the future

### Examples

    [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]
    [inserted_at: [>: [datetime: [ago: [count: 1, interval: "day"]]]]]
    [inserted_at: [>: [datetime: [from_now: [count: 1, interval: "day"]]]]]
    [not: [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]]
    [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]
    [inserted_at: [<=: [datetime: [from_now: [count: 30, interval: "day"]]]]]
    [inserted_at: [==: [date: [ago: [count: 1, interval: "day"]]]]]
    [inserted_at: [!=: [date: [from_now: [count: 1, interval: "day"]]]]]
    [not: [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]]
    [not: [inserted_at: [>: [date: [from_now: [count: 1, interval: "day"]]]]]]
    [inserted_at: [>=: [date: [add: [field: :inserted_at, count: 7, interval: "day"]]]]]
    [inserted_at: [<: [date: [ago: [count: 1, interval: "month"]]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `add` directive before the comparison  
**And** it must check whether the `:inserted_at` field is greater than or equal to `datetime_add(p.inserted_at, 1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at >= datetime_add(p.inserted_at, 1, "day")`

test name: "Rule Statement 1: inserted_at greater than or equal to datetime_add"

**Rule Statement 2:**

**Given** filter params: `[inserted_at: [>: [datetime: [ago: [count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `ago` directive before the comparison  
**And** it must check whether the `:inserted_at` field is greater than `ago(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at > ago(1, "day")`

test name: "Rule Statement 2: inserted_at greater than ago 1 day"

**Rule Statement 3:**

**Given** filter params: `[inserted_at: [>: [datetime: [from_now: [count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `from_now` directive before the comparison  
**And** it must check whether the `:inserted_at` field is greater than `from_now(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at > from_now(1, "day")`

test name: "Rule Statement 3: inserted_at greater than from_now 1 day"

**Rule Statement 4:**

**Given** filter params: `[not: [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the `datetime_add` comparison  
**And** it must check whether the `:inserted_at` field is not greater than or equal to `datetime_add(p.inserted_at, 1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.inserted_at >= datetime_add(p.inserted_at, 1, "day"))`

test name: "Rule Statement 4: inserted_at >= datetime_add negated"

**Rule Statement 5:**

**Given** filter params: `[inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `ago` directive before the comparison  
**And** it must check whether the `:inserted_at` field is less than `ago(7, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at < ago(7, "day")`

test name: "Rule Statement 5: inserted_at less than ago 7 days"

**Rule Statement 6:**

**Given** filter params: `[inserted_at: [<=: [datetime: [from_now: [count: 30, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `from_now` directive before the comparison  
**And** it must check whether the `:inserted_at` field is less than or equal to `from_now(30, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at <= from_now(30, "day")`

test name: "Rule Statement 6: inserted_at less than or equal to from_now 30 days"

**Rule Statement 7:**

**Given** filter params: `[inserted_at: [==: [date: [ago: [count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `date` wrapper with the `ago` directive before the comparison  
**And** it must check whether the `:inserted_at` field equals `ago(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at == ago(1, "day")`

test name: "Rule Statement 7: inserted_at equals ago 1 day using date wrapper"

**Rule Statement 8:**

**Given** filter params: `[inserted_at: [!=: [date: [from_now: [count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `date` wrapper with the `from_now` directive before the comparison  
**And** it must check whether the `:inserted_at` field is not equal to `from_now(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at != from_now(1, "day")`

test name: "Rule Statement 8: inserted_at not equals from_now 1 day using date wrapper"

**Rule Statement 9:**

**Given** filter params: `[not: [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the `ago` comparison  
**And** it must check whether the `:inserted_at` field is not less than `ago(7, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.inserted_at < ago(7, "day"))`

test name: "Rule Statement 9: inserted_at less than ago 7 days negated"

**Rule Statement 10:**

**Given** filter params: `[not: [inserted_at: [>: [date: [from_now: [count: 1, interval: "day"]]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the `from_now` comparison under the `date` wrapper  
**And** it must check whether the `:inserted_at` field is not greater than `from_now(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.inserted_at > from_now(1, "day"))`

test name: "Rule Statement 10: inserted_at greater than from_now 1 day negated using date wrapper"

**Rule Statement 11:**

**Given** filter params: `[inserted_at: [>=: [date: [add: [field: :inserted_at, count: 7, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `date` wrapper with the `add` directive before the comparison  
**And** it must check whether the `:inserted_at` field is greater than or equal to `datetime_add(p.inserted_at, 7, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at >= datetime_add(p.inserted_at, 7, "day")`

test name: "Rule Statement 11: inserted_at >= datetime_add 7 days using date wrapper"

**Rule Statement 12:**

**Given** filter params: `[inserted_at: [<: [date: [ago: [count: 1, interval: "month"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `date` wrapper with the `ago` directive before the comparison  
**And** it must check whether the `:inserted_at` field is less than `ago(1, "month")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at < ago(1, "month")`

test name: "Rule Statement 12: inserted_at less than ago 1 month using date wrapper"

---

## Arithmetic Operator Directives

- `:+`: addition
- `:-`: subtraction
- `:*`: multiplication
- `:/`: division

### Examples

    [views: [>: [+: [:views, 10]]]]
    [not: [views: [>: [+: [:views, 10]]]]]
    [views: [>=: [-: [:views, 5]]]]
    [views: [<: [*: [:views, 2]]]]
    [views: [==: [/: [:views, 2]]]]
    [views: [!=: [+: [:views, 10]]]]
    [not: [views: [>=: [-: [:views, 5]]]]]
    [not: [views: [<: [*: [:views, 2]]]]]
    [views: [<=: [+: [10, 5]]]]
    [views: [>: [-: [100, 10]]]]
    [views: [<=: [*: [10, 2]]]]
    [views: [==: [/: [10, 2]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[views: [>: [+: [:views, 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply addition before the greater-than comparison  
**And** it must check whether the `:views` field is greater than the `:views` field plus `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views > p.views + 10`

test name: "Rule Statement 1: views greater than views plus 10"

**Rule Statement 2:**

**Given** filter params: `[not: [views: [>: [+: [:views, 10]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than comparison after applying addition  
**And** it must check whether the `:views` field is not greater than the `:views` field plus `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views > p.views + 10)`

test name: "Rule Statement 2: views greater than views plus 10 negated"

**Rule Statement 3:**

**Given** filter params: `[views: [>=: [-: [:views, 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply subtraction before the greater-than-or-equal comparison  
**And** it must check whether the `:views` field is greater than or equal to the `:views` field minus `5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views >= p.views - 5`

test name: "Rule Statement 3: views greater than or equal to views minus 5"

**Rule Statement 4:**

**Given** filter params: `[views: [<: [*: [:views, 2]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply multiplication before the less-than comparison  
**And** it must check whether the `:views` field is less than the `:views` field times `2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views < p.views * 2`

test name: "Rule Statement 4: views less than views times 2"

**Rule Statement 5:**

**Given** filter params: `[views: [==: [/: [:views, 2]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply division before the equality comparison  
**And** it must check whether the `:views` field equals the `:views` field divided by `2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views == p.views / 2`

test name: "Rule Statement 5: views equals views divided by 2"

**Rule Statement 6:**

**Given** filter params: `[views: [!=: [+: [:views, 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply addition before the inequality comparison  
**And** it must check whether the `:views` field is not equal to the `:views` field plus `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views != p.views + 10`

test name: "Rule Statement 6: views not equals views plus 10"

**Rule Statement 7:**

**Given** filter params: `[not: [views: [>=: [-: [:views, 5]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than-or-equal comparison after applying subtraction  
**And** it must check whether the `:views` field is not greater than or equal to the `:views` field minus `5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views >= p.views - 5)`

test name: "Rule Statement 7: views greater than or equal to views minus 5 negated"

**Rule Statement 8:**

**Given** filter params: `[not: [views: [<: [*: [:views, 2]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than comparison after applying multiplication  
**And** it must check whether the `:views` field is not less than the `:views` field times `2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views < p.views * 2)`

test name: "Rule Statement 8: views less than views times 2 negated"

**Rule Statement 9:**

**Given** filter params: `[views: [<=: [+: [10, 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply addition before the less-than-or-equal comparison  
**And** it must check whether the `:views` field is less than or equal to `10 + 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views <= 10 + 5`

test name: "Rule Statement 9: views less than or equal to literal 10 plus 5"

**Rule Statement 10:**

**Given** filter params: `[views: [>: [-: [100, 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply subtraction before the greater-than comparison  
**And** it must check whether the `:views` field is greater than the result of `100 - 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views > ^(100 - 10)`

test name: "Rule Statement 10: views greater than literal 100 minus 10"

**Rule Statement 11:**

**Given** filter params: `[views: [<=: [*: [10, 2]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply multiplication before the less-than-or-equal comparison  
**And** it must check whether the `:views` field is less than or equal to `10 * 2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views <= 10 * 2`

test name: "Rule Statement 11: views less than or equal to literal 10 times 2"

**Rule Statement 12:**

**Given** filter params: `[views: [==: [/: [10, 2]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply division before the equality comparison  
**And** it must check whether the `:views` field equals `10 / 2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views == 10 / 2`

test name: "Rule Statement 12: views equals literal 10 divided by 2"

---

## Set Comparison Directives

- `:all`: compare against all values in subquery
- `:any`: compare against any value in subquery

When using `[from: Queryable, ...]`, the params after `:from` define the inner query. That inner query is wrapped in `subquery(...)` and passed to `:all` or `:any`.

### Examples

    [id: [>: [all: subquery_expr]]]
    [not: [id: [>: [all: [from: Comment, body: "Hello"]]]]]
    [id: [>: [any: [from: Comment, body: "Hello"]]]]
    [not: [id: [>: [any: [from: Comment, body: "Hello"]]]]]
    [id: [>=: [all: [from: Comment, body: "Hello"]]]]
    [id: [<: [all: [from: Comment, body: "Hello"]]]]
    [id: [<=: [all: [from: Comment, body: "Hello"]]]]
    [id: [==: [all: [from: Comment, body: "Hello"]]]]
    [id: [!=: [all: [from: Comment, body: "Hello"]]]]
    [id: [all: [from: Comment, body: "Hello"]]]
    [id: [all: [from: Comment, published: true]]]
    [not: [id: [all: [from: Comment, body: "Hello"]]]]
    [id: [any: [from: Comment, body: "Hello"]]]
    [not: [id: [any: [from: Comment, body: "Hello"]]]]
    [id: [>=: [any: [from: Comment, body: "Hello"]]]]
    [id: [<: [any: [from: Comment, body: "Hello"]]]]
    [id: [<=: [any: [from: Comment, body: "Hello"]]]]
    [id: [==: [any: [from: Comment, body: "Hello"]]]]
    [id: [!=: [any: [from: Comment, body: "Hello"]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[id: [>: [all: subquery_expr]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than set comparison against all values from the provided subquery expression  
**And** it must check whether the `:id` field is greater than every value returned by `subquery_expr`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id > all(subquery_expr)`

test name: "Rule Statement 1: id greater than all subquery values"

**Rule Statement 2:**

**Given** filter params: `[not: [id: [>: [all: [from: Comment, body: "Hello"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the negated greater-than set comparison against all values returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is not greater than every `post_id` returned by that subquery  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.id > all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id)))`

test name: "Rule Statement 2: not id greater than all subquery values"

**Rule Statement 3:**

**Given** filter params: `[id: [>: [any: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than set comparison against any value returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is greater than at least one returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id > any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 3: id greater than any subquery value"

**Rule Statement 4:**

**Given** filter params: `[not: [id: [>: [any: [from: Comment, body: "Hello"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the negated greater-than set comparison against any value returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is not greater than any returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.id > any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id)))`

test name: "Rule Statement 4: not id greater than any subquery value"

**Rule Statement 5:**

**Given** filter params: `[id: [>=: [all: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than-or-equal set comparison against all values returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is greater than or equal to every returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id >= all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 5: id greater than or equal to all subquery values"

**Rule Statement 6:**

**Given** filter params: `[id: [<: [all: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than set comparison against all values returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is less than every returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id < all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 6: id less than all subquery values"

**Rule Statement 7:**

**Given** filter params: `[id: [<=: [all: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than-or-equal set comparison against all values returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is less than or equal to every returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id <= all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 7: id less than or equal to all subquery values"

**Rule Statement 8:**

**Given** filter params: `[id: [==: [all: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality set comparison against all values returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field equals every returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 8: id equals all subquery values"

**Rule Statement 9:**

**Given** filter params: `[id: [!=: [all: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality set comparison against all values returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is not equal to every returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id != all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 9: id not equals all subquery values"

**Rule Statement 10:**

**Given** filter params: `[id: [all: [from: Comment, body: "Hello"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality set comparison against all values returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field equals every returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 10: id default equals all subquery values"

**Rule Statement 11:**

**Given** filter params: `[id: [all: [from: Comment, published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality set comparison against all values returned by a subquery filtered by `published == true`  
**And** it must check whether the `:id` field equals every returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == all(subquery(from c in Comment, where: c.published == true, select: c.post_id))`

test name: "Rule Statement 11: id equals all from inline filter params"

**Rule Statement 12:**

**Given** filter params: `[not: [id: [all: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the negated equality set comparison against all values returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is not equal to every returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.id == all(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id)))`

test name: "Rule Statement 12: not id default equals all subquery values"

**Rule Statement 13:**

**Given** filter params: `[id: [any: [from: Comment, body: "Hello"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality set comparison against any value returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field equals at least one returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 13: id default equals any subquery value"

**Rule Statement 14:**

**Given** filter params: `[not: [id: [any: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the negated equality set comparison against any value returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is not equal to any returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.id == any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id)))`

test name: "Rule Statement 14: not id default equals any subquery value"

**Rule Statement 15:**

**Given** filter params: `[id: [>=: [any: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than-or-equal set comparison against any value returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is greater than or equal to at least one returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id >= any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 15: id greater than or equal to any subquery values"

**Rule Statement 16:**

**Given** filter params: `[id: [<: [any: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than set comparison against any value returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is less than at least one returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id < any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 16: id less than any subquery values"

**Rule Statement 17:**

**Given** filter params: `[id: [<=: [any: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than-or-equal set comparison against any value returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is less than or equal to at least one returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id <= any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 17: id less than or equal to any subquery values"

**Rule Statement 18:**

**Given** filter params: `[id: [==: [any: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality set comparison against any value returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field equals at least one returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 18: id equals any subquery values"

**Rule Statement 19:**

**Given** filter params: `[id: [!=: [any: [from: Comment, body: "Hello"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality set comparison against any value returned by a subquery filtered by `body == "Hello"`  
**And** it must check whether the `:id` field is not equal to at least one returned `post_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id != any(subquery(from c in Comment, where: c.body == "Hello", select: c.post_id))`

test name: "Rule Statement 19: id not equals any subquery values"

---

## Aggregate Operator Directives

- `:avg`: average
- `:count`: count
- `:max`: maximum
- `:min`: minimum
- `:sum`: sum

### Examples

    [views: [avg: [>: 10]]]
    [not: [views: [avg: [>: 10]]]]
    [views: [count: [>: 0]]]
    [views: [max: [>=: 100]]]
    [views: [min: [<: 5]]]
    [views: [sum: [==: 1000]]]
    [views: [avg: [!=: 50]]]
    [views: [count: [==: nil]]]
    [not: [views: [count: [>: 0]]]]
    [not: [views: [max: [>=: 100]]]]
    [views: [avg: [<=: 10]]]
    [views: [sum: [>: 500]]]
    [views: [min: [==: 0]]]
    [views: [sum: [!=: 0]]]
    [not: [views: [min: [<: 5]]]]
    [not: [views: [sum: [>: 500]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[views: [avg: [>: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `avg` aggregate before the greater-than comparison  
**And** it must check whether the average of the `:views` field is greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `avg(p.views) > 10`

test name: "Rule Statement 1: avg views greater than"

**Rule Statement 2:**

**Given** filter params: `[not: [views: [avg: [>: 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated greater-than comparison  
**And** it must check whether the average of the `:views` field is not greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (avg(p.views) > 10)`

test name: "Rule Statement 2: avg views greater than negated"

**Rule Statement 3:**

**Given** filter params: `[views: [count: [>: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `count` aggregate before the greater-than comparison  
**And** it must check whether the count of the `:views` field is greater than `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `count(p.views) > 0`

test name: "Rule Statement 3: count views greater than zero"

**Rule Statement 4:**

**Given** filter params: `[views: [max: [>=: 100]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `max` aggregate before the greater-than-or-equal comparison  
**And** it must check whether the maximum of the `:views` field is greater than or equal to `100`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `max(p.views) >= 100`

test name: "Rule Statement 4: max views greater than or equal"

**Rule Statement 5:**

**Given** filter params: `[views: [min: [<: 5]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `min` aggregate before the less-than comparison  
**And** it must check whether the minimum of the `:views` field is less than `5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `min(p.views) < 5`

test name: "Rule Statement 5: min views less than"

**Rule Statement 6:**

**Given** filter params: `[views: [sum: [==: 1000]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `sum` aggregate before the equality comparison  
**And** it must check whether the sum of the `:views` field equals `1000`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `sum(p.views) == 1000`

test name: "Rule Statement 6: sum views equals"

**Rule Statement 7:**

**Given** filter params: `[views: [avg: [!=: 50]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `avg` aggregate before the inequality comparison  
**And** it must check whether the average of the `:views` field is not equal to `50`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `avg(p.views) != 50`

test name: "Rule Statement 7: avg views not equals"

**Rule Statement 8:**

**Given** filter params: `[views: [count: [==: nil]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison to the aggregate result as a nil check  
**And** it must check whether the count of the `:views` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(count(p.views))`

test name: "Rule Statement 8: count views equals nil"

**Rule Statement 9:**

**Given** filter params: `[not: [views: [count: [>: 0]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated greater-than comparison  
**And** it must check whether the count of the `:views` field is not greater than `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (count(p.views) > 0)`

test name: "Rule Statement 9: count views greater than zero negated"

**Rule Statement 10:**

**Given** filter params: `[not: [views: [max: [>=: 100]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated greater-than-or-equal comparison  
**And** it must check whether the maximum of the `:views` field is not greater than or equal to `100`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (max(p.views) >= 100)`

test name: "Rule Statement 10: max views greater than or equal negated"

**Rule Statement 11:**

**Given** filter params: `[views: [avg: [<=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `avg` aggregate before the less-than-or-equal comparison  
**And** it must check whether the average of the `:views` field is less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `avg(p.views) <= 10`

test name: "Rule Statement 11: avg views less than or equal"

**Rule Statement 12:**

**Given** filter params: `[views: [sum: [>: 500]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `sum` aggregate before the greater-than comparison  
**And** it must check whether the sum of the `:views` field is greater than `500`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `sum(p.views) > 500`

test name: "Rule Statement 12: sum views greater than"

**Rule Statement 13:**

**Given** filter params: `[views: [min: [==: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `min` aggregate before the equality comparison  
**And** it must check whether the minimum of the `:views` field equals `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `min(p.views) == 0`

test name: "Rule Statement 13: min views equals zero"

**Rule Statement 14:**

**Given** filter params: `[views: [sum: [!=: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `sum` aggregate before the inequality comparison  
**And** it must check whether the sum of the `:views` field is not equal to `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `sum(p.views) != 0`

test name: "Rule Statement 14: sum views not equals zero"

**Rule Statement 15:**

**Given** filter params: `[not: [views: [min: [<: 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated less-than comparison  
**And** it must check whether the minimum of the `:views` field is not less than `5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (min(p.views) < 5)`

test name: "Rule Statement 15: min views less than negated"

**Rule Statement 16:**

**Given** filter params: `[not: [views: [sum: [>: 500]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated greater-than comparison  
**And** it must check whether the sum of the `:views` field is not greater than `500`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (sum(p.views) > 500)`

test name: "Rule Statement 16: sum views greater than negated"

---

## String Matching Directives

- `:like`: pattern matching
- `:ilike`: case-insensitive pattern matching

### Examples

    [title: [like: "%hello%"]]
    [not: [title: [like: "%hello%"]]]
    [title: [ilike: "%HELLO%"]]
    [not: [title: [ilike: "%HELLO%"]]]
    [title: [like: ["%hello%", "%world%"]]]
    [title: [ilike: ["%HELLO%", "%WORLD%"]]]
    [not: [title: [like: ["%hello%", "%world%"]]]]
    [not: [title: [ilike: ["%HELLO%", "%WORLD%"]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[title: [like: "%hello%"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-sensitive pattern match  
**And** it must check whether the `:title` field matches the pattern `"%hello%"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `like(p.title, "%hello%")`

test name: "Rule Statement 1: title like pattern"

**Rule Statement 2:**

**Given** filter params: `[not: [title: [like: "%hello%"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated case-sensitive pattern match  
**And** it must check whether the `:title` field is not matched by the pattern `"%hello%"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not like(p.title, "%hello%")`

test name: "Rule Statement 2: title like pattern negated"

**Rule Statement 3:**

**Given** filter params: `[title: [ilike: "%HELLO%"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-insensitive pattern match  
**And** it must check whether the `:title` field matches the pattern `"%HELLO%"` case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `ilike(p.title, "%HELLO%")`

test name: "Rule Statement 3: title ilike pattern"

**Rule Statement 4:**

**Given** filter params: `[not: [title: [ilike: "%HELLO%"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated case-insensitive pattern match  
**And** it must check whether the `:title` field is not matched by the pattern `"%HELLO%"` case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not ilike(p.title, "%HELLO%")`

test name: "Rule Statement 4: title ilike pattern negated"

**Rule Statement 5:**

**Given** filter params: `[title: [like: ["%hello%", "%world%"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the `or` grouping for the case-sensitive pattern matches  
**And** it must check whether the `:title` field matches `"%hello%"` or `"%world%"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `like(p.title, "%hello%") or like(p.title, "%world%")`

test name: "Rule Statement 5: title like list of patterns"

**Rule Statement 6:**

**Given** filter params: `[title: [ilike: ["%HELLO%", "%WORLD%"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the `or` grouping for the case-insensitive pattern matches  
**And** it must check whether the `:title` field matches `"%HELLO%"` or `"%WORLD%"` case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%")`

test name: "Rule Statement 6: title ilike list of patterns"

**Rule Statement 7:**

**Given** filter params: `[not: [title: [like: ["%hello%", "%world%"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the negated `or` grouping for the case-sensitive pattern matches  
**And** it must check whether the `:title` field is not matched by either `"%hello%"` or `"%world%"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (like(p.title, "%hello%") or like(p.title, "%world%"))`

test name: "Rule Statement 7: title like list negated"

**Rule Statement 8:**

**Given** filter params: `[not: [title: [ilike: ["%HELLO%", "%WORLD%"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the negated `or` grouping for the case-insensitive pattern matches  
**And** it must check whether the `:title` field is not matched by either `"%HELLO%"` or `"%WORLD%"` case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%"))`

test name: "Rule Statement 8: title ilike list negated"

---

## Comparison Operator Directives

- `:==`: equality
- `:eq`: equality alias
- `:!=`: inequality
- `:ne`: inequality alias
- `:> `: greater than
- `:>=`: greater than or equal
- `:< `: less than
- `:<=`: less than or equal
- `:gt`: greater than alias
- `:gte`: greater than or equal alias
- `:lt`: less than alias
- `:lte`: less than or equal alias
- `:in`: membership

### Examples

    [title: [==: "Post 1"]]
    [title: [eq: "Post 1"]]
    [published_at: [==: nil]]
    [published_at: [eq: nil]]
    [published_at: [!=: nil]]
    [published_at: [ne: nil]]
    [views: [>: 10]]
    [views: [>=: 10]]
    [views: [<: 10]]
    [views: [<=: 10]]
    [views: [!=: 10]]
    [views: [ne: 10]]
    [views: [gt: 10]]
    [views: [gte: 10]]
    [views: [lt: 10]]
    [views: [lte: 10]]
    [published: [in: [true, false]]]
    [published: [==: [true, false]]]
    [published: [!=: [true, false]]]
    [published: [ne: [true, false]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[title: [==: "Post 1"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison  
**And** it must check whether the `:title` field equals `"Post 1"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "Post 1"`

test name: "Rule Statement 1: title equals with == operator"

**Rule Statement 2:**

**Given** filter params: `[title: [eq: "Post 1"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality alias comparison  
**And** it must check whether the `:title` field equals `"Post 1"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "Post 1"`

test name: "Rule Statement 2: title equals with eq operator"

**Rule Statement 3:**

**Given** filter params: `[published_at: [==: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a nil check  
**And** it must check whether the `:published_at` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.published_at)`

test name: "Rule Statement 3: published_at equals nil with =="

**Rule Statement 4:**

**Given** filter params: `[published_at: [eq: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality alias comparison as a nil check  
**And** it must check whether the `:published_at` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.published_at)`

test name: "Rule Statement 4: published_at equals nil with eq"

**Rule Statement 5:**

**Given** filter params: `[published_at: [!=: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison as a non-nil check  
**And** it must check whether the `:published_at` field is not `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not is_nil(p.published_at)`

test name: "Rule Statement 5: published_at not equals nil"

**Rule Statement 6:**

**Given** filter params: `[published_at: [ne: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality alias comparison as a non-nil check  
**And** it must check whether the `:published_at` field is not `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not is_nil(p.published_at)`

test name: "Rule Statement 6: published_at ne nil"

**Rule Statement 7:**

**Given** filter params: `[views: [>: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than comparison  
**And** it must check whether the `:views` field is greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views > 10`

test name: "Rule Statement 7: views greater than"

**Rule Statement 8:**

**Given** filter params: `[views: [>=: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than-or-equal comparison  
**And** it must check whether the `:views` field is greater than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views >= 10`

test name: "Rule Statement 8: views greater than or equal"

**Rule Statement 9:**

**Given** filter params: `[views: [<: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than comparison  
**And** it must check whether the `:views` field is less than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views < 10`

test name: "Rule Statement 9: views less than"

**Rule Statement 10:**

**Given** filter params: `[views: [<=: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than-or-equal comparison  
**And** it must check whether the `:views` field is less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views <= 10`

test name: "Rule Statement 10: views less than or equal"

**Rule Statement 11:**

**Given** filter params: `[views: [!=: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison  
**And** it must check whether the `:views` field is not equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views != 10`

test name: "Rule Statement 11: views not equals"

**Rule Statement 12:**

**Given** filter params: `[views: [ne: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality alias comparison  
**And** it must check whether the `:views` field is not equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views != 10`

test name: "Rule Statement 12: views ne operator"

**Rule Statement 13:**

**Given** filter params: `[views: [gt: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than alias comparison  
**And** it must check whether the `:views` field is greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views > 10`

test name: "Rule Statement 13: views gt operator"

**Rule Statement 14:**

**Given** filter params: `[views: [gte: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than-or-equal alias comparison  
**And** it must check whether the `:views` field is greater than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views >= 10`

test name: "Rule Statement 14: views gte operator"

**Rule Statement 15:**

**Given** filter params: `[views: [lt: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than alias comparison  
**And** it must check whether the `:views` field is less than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views < 10`

test name: "Rule Statement 15: views lt operator"

**Rule Statement 16:**

**Given** filter params: `[views: [lte: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than-or-equal alias comparison  
**And** it must check whether the `:views` field is less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views <= 10`

test name: "Rule Statement 16: views lte operator"

**Rule Statement 17:**

**Given** filter params: `[published: [in: [true, false]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a membership check  
**And** it must check whether the `:published` field is in `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published in [true, false]`

test name: "Rule Statement 17: published in list"

**Rule Statement 18:**

**Given** filter params: `[published: [==: [true, false]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a membership check  
**And** it must check whether the `:published` field is in `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published in [true, false]`

test name: "Rule Statement 18: published equals list"

**Rule Statement 19:**

**Given** filter params: `[published: [!=: [true, false]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison as a negated membership check that accounts for `nil`  
**And** it must check whether the `:published` field is `nil` or not in `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.published) or p.published not in [true, false]`

test name: "Rule Statement 19: published not equals list"

**Rule Statement 20:**

**Given** filter params: `[published: [ne: [true, false]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality alias comparison as a negated membership check that accounts for `nil`  
**And** it must check whether the `:published` field is `nil` or not in `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.published) or p.published not in [true, false]`

test name: "Rule Statement 20: published ne list"

---

## Logical Operator Directives

- `:and`: field level and top level
- `:or`: field level and top level

### Examples

    [and: [title: "hello", views: [>: 10, <: 20]]]
    [and: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]
    [or: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]
    [or: [[or: [title: "hello", views: [>: 10, <: 20]]], [published: true]]]
    [or: [[title: "hello", or: [views: [>: 10, <: 20]]], [published: true]]]
    [views: [or: [>: 100, <: 5]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[and: [title: "hello", views: [>: 10, <: 20]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the `and` grouping across the field conditions  
**And** it must check whether the `:title` field equals `"hello"` and the `:views` field is greater than `10` and less than `20`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "hello" and (p.views > 10 and p.views < 20)`

test name: "Rule Statement 1: and with multiple conditions on same field"

**Rule Statement 2:**

**Given** filter params: `[and: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the nested `and` grouping  
**And** it must check whether the `:title` field equals `"hello"`, the `:views` field is greater than `10` and less than `20`, and the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "hello" and (p.views > 10 and p.views < 20) and p.published == true`

test name: "Rule Statement 2: nested and with multiple conditions"

**Rule Statement 3:**

**Given** filter params: `[or: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the top-level `or` grouping with an `and`-grouped first branch  
**And** it must check whether the `:title` field equals `"hello"` and the `:views` field is greater than `10` and less than `20`, or whether the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `(p.title == "hello" and (p.views > 10 and p.views < 20)) or p.published == true`

test name: "Rule Statement 3: or with nested and conditions"

**Rule Statement 4:**

**Given** filter params: `[or: [[or: [title: "hello", views: [>: 10, <: 20]]], [published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the nested `or` grouping in the first branch of the top-level `or`  
**And** it must check whether the `:title` field equals `"hello"` or the `:views` field is greater than `10` and less than `20`, or whether the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "hello" or (p.views > 10 and p.views < 20) or p.published == true`

test name: "Rule Statement 4: nested or within or"

**Rule Statement 5:**

**Given** filter params: `[or: [[title: "hello", or: [views: [>: 10, <: 20]]], [published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the nested `or` grouping inside the `and`-grouped first branch of the top-level `or`  
**And** it must check whether the `:title` field equals `"hello"` and the `:views` field is greater than `10` or less than `20`, or whether the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `(p.title == "hello" and (p.views > 10 or p.views < 20)) or p.published == true`

test name: "Rule Statement 5: nested or within and within or"

**Rule Statement 6:**

**Given** filter params: `[views: [or: [>: 100, <: 5]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the same-field `or` grouping  
**And** it must check whether the `:views` field is greater than `100` or less than `5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views > 100 or p.views < 5`

No matching test currently in `test/examples/ecto_query_dsl.exs`.

## Array Fields

Array fields (e.g., `{:array, :string}` in Ecto schemas) support special comparison and containment operations.

- `:in`: Check if value is contained in array (for single values)
- `:all`: Apply operation to all array elements
- `:count`: Count array elements and compare
- Comparison operators work element-wise
- String matching (like, ilike) works on array elements
- Transformations (lower, upper) work on array elements

### Examples

    [tags: "elixir"]
    [tags: [==: "elixir"]]
    [tags: [!=: "elixir"]]
    [tags: [in: "elixir"]]
    [tags: ["elixir", "erlang"]]
    [tags: [==: ["elixir", "erlang"]]]
    [tags: [!=: ["elixir"]]]
    [tags: nil]
    [tags: [==: nil]]
    [tags: [!=: nil]]
    [tags: [in: ["elixir"]]]
    [tags: [all: [in: ["elixir", "erlang"]]]]
    [not: [tags: [==: ["elixir"]]]]
    [not: [tags: [in: "elixir"]]]
    [tags: [>: "elixir"]]
    [tags: [>=: "elixir"]]
    [tags: [<: "elixir"]]
    [tags: [<=: "elixir"]]
    [tags: [like: "elixir"]]
    [tags: [ilike: "elixir"]]
    [tags: [like: ["%elixir%", "%erlang%"]]]
    [not: [tags: [like: "elixir"]]]
    [tags: [==: [lower: "elixir"]]]
    [tags: [!=: [upper: "ELIXIR"]]]
    [tags: [count: [>: 0]]]
    [tags: [count: [==: 0]]]
    [tags: [all: [>: "a"]]]
    [tags: [ilike: ["elixir", "erlang"]]]
    [not: [tags: [ilike: ["%elixir%"]]]]
    [lower: [tags: [==: "elixir"]]]
    [upper: [tags: [==: "ELIXIR"]]]
    [tags: [count: [<: 5]]]
    [tags: [count: [>=: 2]]]
    [tags: [count: [<=: 10]]]
    [tags: [count: [!=: 3]]]

**Rule Statement 1:**

**Given** filter params: `[tags: "elixir"]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a membership check against the array field  
**And** it must check whether `"elixir"` is in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" in p.tags`

test name: "Rule Statement 1: tags contains single value"

**Rule Statement 2:**

**Given** filter params: `[tags: [==: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a membership check against the array field  
**And** it must check whether `"elixir"` is in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" in p.tags`

test name: "Rule Statement 2: tags equals operator with single value"

**Rule Statement 3:**

**Given** filter params: `[tags: [!=: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated membership check against the array field  
**And** it must check whether `"elixir"` is not in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" not in p.tags`

test name: "Rule Statement 3: tags not equals single value"

**Rule Statement 4:**

**Given** filter params: `[tags: [in: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a membership check against the array field  
**And** it must check whether `"elixir"` is in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" in p.tags`

test name: "Rule Statement 4: tags in operator with single value"

**Rule Statement 5:**

**Given** filter params: `[tags: ["elixir", "erlang"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison against the array field  
**And** it must check whether the `:tags` field is exactly `["elixir", "erlang"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.tags == ^["elixir", "erlang"]`

test name: "Rule Statement 5: tags equals array"

**Rule Statement 6:**

**Given** filter params: `[tags: [==: ["elixir", "erlang"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison against the array field  
**And** it must check whether the `:tags` field is exactly `["elixir", "erlang"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.tags == ^["elixir", "erlang"]`

test name: "Rule Statement 6: tags equals operator with array"

**Rule Statement 7:**

**Given** filter params: `[tags: [!=: ["elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison against the array field  
**And** it must check whether the `:tags` field is not exactly `["elixir"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.tags != ^["elixir"]`

test name: "Rule Statement 7: tags not equals array"

**Rule Statement 8:**

**Given** filter params: `[tags: nil]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a nil check against the array field  
**And** it must check whether the `:tags` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.tags)`

test name: "Rule Statement 8: tags is nil"

**Rule Statement 9:**

**Given** filter params: `[tags: [==: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a nil check against the array field  
**And** it must check whether the `:tags` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.tags)`

test name: "Rule Statement 9: tags equals nil"

**Rule Statement 10:**

**Given** filter params: `[tags: [!=: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison as a non-nil check against the array field  
**And** it must check whether the `:tags` field is not `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not is_nil(p.tags)`

test name: "Rule Statement 10: tags not equals nil"

**Rule Statement 11:**

**Given** filter params: `[tags: [in: ["elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply an overlap check  
**And** it must check whether the `:tags` field overlaps with the list `["elixir"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? && ?", p.tags, ^["elixir"])`

test name: "Rule Statement 11: tags array overlaps"

**Rule Statement 12:**

**Given** filter params: `[tags: [all: [in: ["elixir", "erlang"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a containment check  
**And** it must check whether every value in `:tags` is included in `["elixir", "erlang"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? <@ ?", p.tags, ^["elixir", "erlang"])`

test name: "Rule Statement 12: tags array is contained in list"

**Rule Statement 13:**

**Given** filter params: `[not: [tags: [==: ["elixir"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated equality comparison against the array field  
**And** it must check whether the `:tags` field is not exactly `["elixir"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.tags == ^["elixir"])`

test name: "Rule Statement 13: negated tags equals array"

**Rule Statement 14:**

**Given** filter params: `[not: [tags: [in: "elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated membership check against the array field  
**And** it must check whether `"elixir"` is not in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" not in p.tags`

test name: "Rule Statement 14: negated tags contains value"

**Rule Statement 15:**

**Given** filter params: `[tags: [>: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than comparison against any element of the array field  
**And** it must check whether `"elixir"` is greater than at least one value in `:tags`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? > ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 15: value greater than any tag"

**Rule Statement 16:**

**Given** filter params: `[tags: [>=: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than-or-equal comparison against any element of the array field  
**And** it must check whether `"elixir"` is greater than or equal to at least one value in `:tags`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? >= ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 16: value greater than or equal to any tag"

**Rule Statement 17:**

**Given** filter params: `[tags: [<: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than comparison against any element of the array field  
**And** it must check whether `"elixir"` is less than at least one value in `:tags`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? < ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 17: value less than any tag"

**Rule Statement 18:**

**Given** filter params: `[tags: [<=: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than-or-equal comparison against any element of the array field  
**And** it must check whether `"elixir"` is less than or equal to at least one value in `:tags`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? <= ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 18: value less than or equal to any tag"

**Rule Statement 19:**

**Given** filter params: `[tags: [like: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-sensitive pattern match against any element of the array field  
**And** it must check whether at least one value in `:tags` matches the provided pattern  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? LIKE ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 19: tags any like"

**Rule Statement 20:**

**Given** filter params: `[tags: [ilike: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-insensitive pattern match against any element of the array field  
**And** it must check whether at least one value in `:tags` matches the provided pattern case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? ILIKE ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 20: tags any ilike"

**Rule Statement 21:**

**Given** filter params: `[tags: [like: ["%elixir%", "%erlang%"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-sensitive pattern match against any element of the array field  
**And** it must check whether at least one value in `:tags` matches at least one of the provided patterns  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n", p.tags, ^["%elixir%", "%erlang%"])`

test name: "Rule Statement 21: tags any like any"

**Rule Statement 22:**

**Given** filter params: `[not: [tags: [like: "elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated case-sensitive pattern match against any element of the array field  
**And** it must check whether each value in `:tags` is not matched by the provided pattern  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not fragment("? LIKE ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 22: negated tags any like"

**Rule Statement 23:**

**Given** filter params: `[tags: [==: [lower: "elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the lower transformation before the membership check against the array field  
**And** it must check whether the lowercased value is in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("lower(?)", "elixir") in p.tags`

test name: "Rule Statement 23: tags contains lowercased value"

**Rule Statement 24:**

**Given** filter params: `[tags: [!=: [upper: "ELIXIR"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the upper transformation before the negated membership check against the array field  
**And** it must check whether the uppercased value is not in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("upper(?)", "ELIXIR") not in p.tags`

test name: "Rule Statement 24: tags not contains uppercased value"

**Rule Statement 25:**

**Given** filter params: `[tags: [count: [>: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the greater-than comparison  
**And** it must check whether the `:tags` field contains more than `0` elements  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) > 0`

test name: "Rule Statement 25: tags count greater than"

**Rule Statement 26:**

**Given** filter params: `[tags: [count: [==: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the equality comparison  
**And** it must check whether the `:tags` field has a count of `0`, treating both `nil` and empty arrays as `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("coalesce(array_length(?, 1), 0)", p.tags) == 0`

test name: "Rule Statement 26: tags count equals zero"

**Rule Statement 27:**

**Given** filter params: `[tags: [all: [>: "a"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than comparison against all elements of the array field  
**And** it must check whether `"a"` is greater than every value in `:tags`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? > ALL(?)", ^"a", p.tags)`

test name: "Rule Statement 27: value greater than all tags"

**Rule Statement 28:**

**Given** filter params: `[tags: [ilike: ["elixir", "erlang"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-insensitive pattern match against any element of the array field  
**And** it must check whether at least one value in `:tags` matches at least one of the provided inputs case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", p.tags, ^["%elixir%", "%erlang%"])`

test name: "Rule Statement 28: tags any ilike any"

**Rule Statement 29:**

**Given** filter params: `[not: [tags: [ilike: ["%elixir%"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated case-insensitive pattern match against any element of the array field  
**And** it must check whether each value in `:tags` is not matched by the provided pattern case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", p.tags, ^["%elixir%"])`

test name: "Rule Statement 29: negated tags any ilike"

**Rule Statement 30:**

**Given** filter params: `[lower: [tags: [==: "elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the lower transformation to the array field before the equality comparison  
**And** it must check whether any lowercased value in `:tags` equals `"elixir"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n", p.tags, ^"elixir")`

test name: "Rule Statement 30: lower tags equals value"

**Rule Statement 31:**

**Given** filter params: `[upper: [tags: [==: "ELIXIR"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the upper transformation to the array field before the equality comparison  
**And** it must check whether any uppercased value in `:tags` equals `"ELIXIR"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n", p.tags, ^"ELIXIR")`

test name: "Rule Statement 31: upper tags equals value"

**Rule Statement 32:**

**Given** filter params: `[tags: [count: [<: 5]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the less-than comparison  
**And** it must check whether the `:tags` field contains fewer than `5` elements  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) < 5`

test name: "Rule Statement 32: tags count less than"

**Rule Statement 33:**

**Given** filter params: `[tags: [count: [>=: 2]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the greater-than-or-equal comparison  
**And** it must check whether the `:tags` field contains at least `2` elements  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) >= 2`

test name: "Rule Statement 33: tags count greater than or equal"

**Rule Statement 34:**

**Given** filter params: `[tags: [count: [<=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the less-than-or-equal comparison  
**And** it must check whether the `:tags` field contains at most `10` elements  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) <= 10`

test name: "Rule Statement 34: tags count less than or equal"

**Rule Statement 35:**

**Given** filter params: `[tags: [count: [!=: 3]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the inequality comparison  
**And** it must check whether the `:tags` field has a count that is not equal to `3`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) != 3`

test name: "Rule Statement 35: tags count not equals"

---

## Scalar Fields

A scalar field is a field on a given schema or table. It can be used in filter conditions to compare values.

### Examples

    [id: 1]
    [published: true]
    [id: 1, published: true]
    [published_at: nil]
    [published: [true, false]]
    [title: "hello"]
    [and: [[id: 1]]]
    [and: [[id: 1], [published: true]]]
    [and: [id: 1, title: "hello"]]
    [and: [[id: 1, title: "hello"], [published: true]]]
    [or: [[views: 1, title: "hello"], [published: true]]]
    [or: [[id: 1, or: [title: "hello", body: "world"]], [published: true]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[id: 1]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison  
**And** it must check whether the `:id` field equals `1`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == 1`

test name: "Rule Statement 1: id equals value"

**Rule Statement 2:**

**Given** filter params: `[published: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison  
**And** it must check whether the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published == true`

test name: "Rule Statement 2: published equals boolean"

**Rule Statement 3:**

**Given** filter params: `[id: 1, published: true]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the top-level `and` grouping across the scalar field conditions  
**And** it must check whether the `:id` field equals `1` and the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == 1 and p.published == true`

test name: "Rule Statement 3: multiple scalar fields with and"

**Rule Statement 4:**

**Given** filter params: `[published_at: nil]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a nil check  
**And** it must check whether the `:published_at` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.published_at)`

test name: "Rule Statement 4: published_at is nil"

**Rule Statement 5:**

**Given** filter params: `[published: [true, false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a membership check  
**And** it must check whether the `:published` field is in the list `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published in [true, false]`

test name: "Rule Statement 5: published in list"

**Rule Statement 6:**

**Given** filter params: `[title: "hello"]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison  
**And** it must check whether the `:title` field equals `"hello"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "hello"`

test name: "Rule Statement 6: title equals string"

**Rule Statement 7:**

**Given** filter params: `[and: [[id: 1]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the explicit `and` wrapper around the single nested condition  
**And** it must check whether the `:id` field equals `1`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == 1`

test name: "Rule Statement 7: explicit and with single condition"

**Rule Statement 8:**

**Given** filter params: `[and: [[id: 1], [published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the `and` grouping across the nested conditions  
**And** it must check whether the `:id` field equals `1` and the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == 1 and p.published == true`

test name: "Rule Statement 8: explicit and with multiple conditions"

**Rule Statement 9:**

**Given** filter params: `[and: [id: 1, title: "hello"]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the explicit `and` grouping across the keyword conditions  
**And** it must check whether the `:id` field equals `1` and the `:title` field equals `"hello"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.id == 1 and p.title == "hello"`

test name: "Rule Statement 9: explicit and with map conditions"

**Rule Statement 10:**

**Given** filter params: `[and: [[id: 1, title: "hello"], [published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the nested `and` grouping  
**And** it must check whether the `:id` field equals `1`, the `:title` field equals `"hello"`, and the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `(p.id == 1 and p.title == "hello") and p.published == true`

test name: "Rule Statement 10: nested and conditions"

**Rule Statement 11:**

**Given** filter params: `[or: [[views: 1, title: "hello"], [published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the top-level `or` grouping with an `and`-grouped first branch  
**And** it must check whether the grouped `:views` and `:title` conditions match or the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `(p.views == 1 and p.title == "hello") or p.published == true`

test name: "Rule Statement 11: or with map conditions"

**Rule Statement 12:**

**Given** filter params: `[or: [[id: 1, or: [title: "hello", body: "world"]], [published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the nested `or` grouping inside the first branch of the top-level `or`  
**And** it must check whether the `:id` field equals the provided id and either the `:title` field equals `"hello"` or the `:body` field equals `"world"`, or whether the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `(p.id == ^post1.id and (p.title == "hello" or p.body == "world")) or p.published == true`

test name: "Rule Statement 12: nested or within and"

---

## Negation Directives

- `:not`: Wraps an entire operation in a logical NOT. Whatever condition the nested operation produces, `:not` flips it (`true` becomes `false`, and `false` becomes `true`).

### Examples

    [not: [published: [in: [true]]]]
    [not: [published: [==: [true]]]]
    [not: [published: [!=: [true]]]]
    [not: [views: [>: 10]]]
    [not: [views: [>=: 10]]]
    [not: [views: [<: 10]]]
    [not: [views: [<=: 10]]]
    [not: [views: [==: 10]]]
    [not: [views: [!=: 10]]]
    [not: [views: [gt: 10]]]
    [not: [views: [gte: 10]]]
    [not: [views: [lt: 10]]]
    [not: [views: [lte: 10]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[not: [published: [in: [true]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the membership check  
**And** it must check whether the `:published` field is not in the list `[true]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published not in [true]`

test name: "Rule Statement 1: negated published in list"

**Rule Statement 2:**

**Given** filter params: `[not: [published: [==: [true]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the equality comparison after applying the membership check  
**And** it must check whether the `:published` field is not in the list `[true]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published not in [true]`

test name: "Rule Statement 2: negated published equals operator with list"

**Rule Statement 3:**

**Given** filter params: `[not: [published: [!=: [true]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the inequality comparison after applying the membership check  
**And** it must check whether the `:published` field is in the list `[true]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published in [true]`

test name: "Rule Statement 3: negated published not equals operator with list"

**Rule Statement 4:**

**Given** filter params: `[not: [views: [>: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than comparison  
**And** it must check whether the `:views` field is not greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views > 10)`

test name: "Rule Statement 4: negated views greater than"

**Rule Statement 5:**

**Given** filter params: `[not: [views: [>=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than-or-equal comparison  
**And** it must check whether the `:views` field is not greater than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views >= 10)`

test name: "Rule Statement 5: negated views greater than or equal"

**Rule Statement 6:**

**Given** filter params: `[not: [views: [<: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than comparison  
**And** it must check whether the `:views` field is not less than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views < 10)`

test name: "Rule Statement 6: negated views less than"

**Rule Statement 7:**

**Given** filter params: `[not: [views: [<=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than-or-equal comparison  
**And** it must check whether the `:views` field is not less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views <= 10)`

test name: "Rule Statement 7: negated views less than or equal"

**Rule Statement 8:**

**Given** filter params: `[not: [views: [==: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the equality comparison  
**And** it must check whether the `:views` field is not equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views == 10)`

test name: "Rule Statement 8: negated views equals"

**Rule Statement 9:**

**Given** filter params: `[not: [views: [!=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the inequality comparison  
**And** it must check whether the `:views` field is equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views != 10)`

test name: "Rule Statement 9: negated views not equals"

**Rule Statement 10:**

**Given** filter params: `[not: [views: [gt: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than comparison  
**And** it must check whether the `:views` field is not greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views > 10)`

test name: "Rule Statement 10: negated views gt alias"

**Rule Statement 11:**

**Given** filter params: `[not: [views: [gte: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than-or-equal comparison  
**And** it must check whether the `:views` field is not greater than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views >= 10)`

test name: "Rule Statement 11: negated views gte alias"

**Rule Statement 12:**

**Given** filter params: `[not: [views: [lt: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than comparison  
**And** it must check whether the `:views` field is not less than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views < 10)`

test name: "Rule Statement 12: negated views lt alias"

**Rule Statement 13:**

**Given** filter params: `[not: [views: [lte: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than-or-equal comparison  
**And** it must check whether the `:views` field is not less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views <= 10)`

test name: "Rule Statement 13: negated views lte alias"
