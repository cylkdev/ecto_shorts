
## CTE Directives

Assumed starting query unless otherwise stated: `from(p in EctoShorts.TestPost)`

- `:recursive_ctes`: enable recursive CTEs
- `:with_cte`: define a CTE

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
**And** it must check whether the query enables recursive CTEs  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost) |> recursive_ctes(true)`

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
**And** it must check whether the query defines the `published_posts` CTE from the provided query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost) |> with_cte("published_posts", as: ^cte_query)`

test name: "Rule Statement 3: with_cte basic"

**Rule Statement 4:**

**Given** filter params: `[with_cte: [published_posts: [as: cte_query, materialized: false, operation: :all]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query defines the `published_posts` CTE as not materialized with `operation: :all`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost) |> with_cte("published_posts", as: ^cte_query, materialized: false, operation: :all)`

test name: "Rule Statement 4: with_cte not materialized"

**Rule Statement 5:**

**Given** filter params: `[with_cte: [published_posts: [as: [from: [query: Post, published: true]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query builds the `published_posts` CTE from the provided filter params  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `cte_query = from(p in EctoShorts.TestPost, where: p.published == true); from(p in EctoShorts.TestPost) |> with_cte("published_posts", as: ^cte_query)`

test name: "Rule Statement 5: with_cte from filter params published true"

**Rule Statement 6:**

**Given** filter params: `[with_cte: [target_post: [as: [from: [query: Post, id: 1]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query builds the `target_post` CTE from the provided filter params  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `cte_query = from(p in EctoShorts.TestPost, where: p.id == 1); from(p in EctoShorts.TestPost) |> with_cte("target_post", as: ^cte_query)`

test name: "Rule Statement 6: with_cte from filter params id"

**Rule Statement 7:**

**Given** filter params: `[recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided CTE directives  
**And** it must check whether the query enables recursive CTEs and defines the `published_posts` CTE from the provided query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost) |> recursive_ctes(true) |> with_cte("published_posts", as: ^cte_query)`

test name: "Rule Statement 7: recursive_ctes with cte"
