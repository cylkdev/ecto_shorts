## Join Directives

Assumed starting query unless otherwise stated: `from(p in EctoShorts.TestPost)`

- association keys such as `:author`: association join shorthand
- `:join`: explicit join container
  - `:association`: association join type
  - `:schema`: schema join type
  - `:table`: table join type
  - `:query`: query join type
  - `:subquery`: subquery join type
  - `:fragment`: fragment join type
  - `:on`: join condition
  - `:type`: join type such as `:left`
  - `:hints`: join hints

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
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, where: as(:author).first_name == "John")`

test name: "Rule Statement 1: association join with named binding and filter"

**Rule Statement 2:**

**Given** filter params: `[author: [first_name: "John"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the `:author` association without a named binding and filters by `a.first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), where: a.first_name == "John")`

test name: "Rule Statement 2: association join without named binding"

**Rule Statement 3:**

**Given** filter params: `[author: [as: :author, on: true, first_name: "John"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the `:author` association as the named binding `:author` with `on: true` and filters by `as(:author).first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, on: true, where: as(:author).first_name == "John")`

test name: "Rule Statement 3: association join with explicit on condition"

**Rule Statement 4:**

**Given** filter params: `[author: [as: :author, type: :left, first_name: "John"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query applies a `left_join` for the `:author` association as the named binding `:author` and filters by `as(:author).first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, left_join: a in assoc(p, :author), as: :author, where: as(:author).first_name == "John")`

test name: "Rule Statement 4: association join with left join qualifier"

**Rule Statement 5:**

**Given** filter params: `[join: [author: [as: :author]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query adds the canonical association join for `:author` as the named binding `:author`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author)`

test name: "Rule Statement 5: canonical association join"

**Rule Statement 6:**

**Given** filter params: `[join: [schema: [source: User, as: :user_join, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the schema source as the named binding `:user_join` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: u in EctoShorts.TestUser, as: :user_join, on: true)`

test name: "Rule Statement 6: canonical schema join"

**Rule Statement 7:**

**Given** filter params: `[join: [table: [source: "users", as: :users_table, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the table source as the named binding `:users_table` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: u in "users", as: :users_table, on: true)`

test name: "Rule Statement 7: canonical table join"

**Rule Statement 8:**

**Given** filter params: `[join: [query: [source: user_query, as: :named_users, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the provided query source as the named binding `:named_users` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: u in ^user_query, as: :named_users, on: true)`

test name: "Rule Statement 8: canonical query join"

**Rule Statement 9:**

**Given** filter params: `[join: [subquery: [source: user_query, as: :named_users_subquery, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the provided query source as a subquery named binding `:named_users_subquery` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: u in subquery(user_query), as: :named_users_subquery, on: true)`

test name: "Rule Statement 9: canonical subquery join"

**Rule Statement 10:**

**Given** filter params: `[join: [subquery: [source: [from: [query: User, first_name: "John"]], as: :named_users_subquery, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query builds the subquery source from the explicit `:from` payload and joins it as the named binding `:named_users_subquery` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `named_users_query = from(u in EctoShorts.TestUser, where: u.first_name == "John"); from(p in EctoShorts.TestPost, join: u in subquery(named_users_query), as: :named_users_subquery, on: true)`

test name: "Rule Statement 10: canonical subquery join from filter params with explicit from"

**Rule Statement 11:**

**Given** filter params: `[join: [subquery: [source: [from: [published: true]], as: :published_posts_subquery, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query builds the subquery source from the current schema filter params and joins it as the named binding `:published_posts_subquery` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `pub_query = from(p in EctoShorts.TestPost, where: p.published == true); from(p in EctoShorts.TestPost, join: s in subquery(pub_query), as: :published_posts_subquery, on: true)`

test name: "Rule Statement 11: canonical subquery join from current schema filter params"

**Rule Statement 12:**

**Given** filter params: `[join: [fragment: [source: [name: :active_posts, values: [min_views: 0]], as: :active_posts, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the fragment source as the named binding `:active_posts` with `on: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `active_posts_query = from(ap in fragment("SELECT id FROM posts WHERE views >= ?", ^0), select: %{id: field(ap, :id)}); from(p in EctoShorts.TestPost, join: ap in ^active_posts_query, as: :active_posts, on: true)`

test name: "Rule Statement 12: canonical fragment join"

**Rule Statement 13:**

**Given** filter params: `[join: [fragment: [source: [name: :active_posts, values: [min_views: 0]], hints: :test_index, as: :active_posts, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query joins the fragment source as the named binding `:active_posts`, preserves `on: true`, and applies the join hints  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `active_posts_query = from(ap in fragment("SELECT id FROM posts WHERE views >= ?", ^0), select: %{id: field(ap, :id)}); from(p in EctoShorts.TestPost, join: ap in ^active_posts_query, as: :active_posts, on: true, hints: ["USE INDEX(test_index)"])`

test name: "Rule Statement 13: canonical fragment join with hints"

**Rule Statement 14:**

**Given** filter params: `[join: [author: [as: :author], table: [source: "users", as: :users_table, on: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided join directives  
**And** it must check whether the query applies the `:author` association join first and then the table join for `"users"` in the same query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, join: u in "users", as: :users_table, on: true)`

test name: "Rule Statement 14: canonical multiple joins"
