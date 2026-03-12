## Query Modifier Directives

Rules that depend on existing query state include an explicit `Assumed starting query` note. Resulting expressions are limited to the modifier applied to that stated base query.

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

**Assumed starting query:** `from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1)`

**Given** filter params: `[with_ties: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query enables `with_ties(true)` on a limited descending `:views` query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1) |> with_ties(true)`

test name: "Rule Statement 1: with_ties true"

**Rule Statement 2:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1)`

**Given** filter params: `[with_ties: false]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query disables `with_ties` on a limited descending `:views` query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1) |> with_ties(false)`

test name: "Rule Statement 2: with_ties false"

**Rule Statement 3:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, as: :post, order_by: [desc: p.views], limit: 1)`

**Given** filter params: `[with_ties: [bind: [as: :post, value: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query enables `with_ties(true)` while preserving the named `:post` binding  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, as: :post, order_by: [desc: p.views], limit: 1) |> with_ties(true)`

test name: "Rule Statement 3: with_ties named binding"

**Rule Statement 4:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1)`

**Given** filter params: `[with_ties: [bind: [at: 1, value: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query enables `with_ties(true)` while preserving the positional binding selector  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, order_by: [desc: p.views], limit: 1) |> with_ties(true)`

test name: "Rule Statement 4: with_ties positional binding"

**Rule Statement 5:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, where: p.id == 1)`

**Given** filter params: `[update: [set: [title: "After"], inc: [views: 1]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query adds the `set` and `inc` update operations to the assumed update query for `id == 1`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.id == 1, update: [set: [title: "After"], inc: [views: 1]])`

test name: "Rule Statement 5: update set and inc"

**Rule Statement 6:**

**Given** filter params: `[windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query defines the `post_window` window with the provided partition and ordering fields  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, windows: [post_window: [partition_by: p.author_id, order_by: [desc: p.inserted_at]]])`

test name: "Rule Statement 6: windows partition_by and order_by"

**Rule Statement 7:**

**Given** filter params: `[preload: :author]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query preloads the `:author` association by name  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, preload: :author)`

test name: "Rule Statement 7: preload atom"

**Rule Statement 8:**

**Given** filter params: `[preload: [:author]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query preloads the `:author` association from a list payload  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, preload: [:author])`

test name: "Rule Statement 8: preload list with atom"

**Rule Statement 9:**

**Given** filter params: `[preload: [author: [:posts]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query preloads the `:author` association with nested `:posts`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, preload: [author: [:posts]])`

test name: "Rule Statement 9: preload nested associations"

**Rule Statement 10:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :example)`

**Given** filter params: `[preload: [bind: [as: :example, value: :author]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query preloads the `:author` association from the named `:example` binding  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :example, preload: [author: a])`

test name: "Rule Statement 10: preload from named binding"

**Rule Statement 11:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, join: a in assoc(p, :author))`

**Given** filter params: `[preload: [bind: [at: 2, value: :author]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query preloads the `:author` association from the positional binding  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), preload: [author: a])`

test name: "Rule Statement 11: preload from positional binding"

**Rule Statement 12:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, join: a in assoc(p, :author))`

**Given** filter params: `[preload: [bind: [at: 2, value: :author], posts: [:comments]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query preloads the `:author` association from the positional binding with nested `posts: [:comments]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), preload: [author: {a, [posts: [:comments]]}])`

test name: "Rule Statement 12: preload from binding and nested"

**Rule Statement 13:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author)`

**Given** filter params: `[preload: [bind: [as: :author, value: :author], posts: [:comments]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query preloads the `:author` association from the named `:author` binding with nested `posts: [:comments]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, preload: [author: {a, [posts: [:comments]]}])`

test name: "Rule Statement 13: preload from named binding and nested"

**Rule Statement 14:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author)`

**Given** filter params: `[preload: [bind: [[as: :author, value: :author], [at: 2, value: :author]], posts: [:comments]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query modifier operators  
**And** it must check whether the query preloads the `:author` association from the named and positional bindings with shared nested `posts: [:comments]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, preload: [author: {a, [posts: [:comments]]}])`

test name: "Rule Statement 14: preload from multiple bindings and nested"
