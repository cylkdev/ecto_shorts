## Query Configuration Directives

When a resulting expression uses pipeline helpers such as `reverse_order/1`, `exclude/2`, or `put_query_prefix/2`, the assumed starting query is shown directly in that expression.

- `:where`: explicit WHERE clause
- `:or_where`: OR WHERE clause
- `:from`: specify source schema/table
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
- `:after`: filter by ids greater than the provided raw id
- `:before`: filter by ids less than the provided raw id
- `:reverse_order`: reverse ordering
- `:exclude`: exclude query parts
- `:put_query_prefix`: set schema prefix
- `:start_date`: filter by start date
- `:end_date`: filter by end date
- `:ids`: filter by IDs
- `:dynamic`: raw dynamic expression
- nested `:exists`: exists subquery condition inside explicit clauses such as `:where` or `:or_where`

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

**Rule Statement 1:**

**Given** filter params: `[from: Post, id: 1, published: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `from(p in EctoShorts.TestPost, where: p.id == 1 and p.published == true)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.id == 1 and p.published == true)`

test name: "Rule Statement 1: from schema with filters"

**Rule Statement 2:**

**Given** filter params: `[select: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: p`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: p`

test name: "Rule Statement 2: select true selects all fields"

**Rule Statement 3:**

**Given** filter params: `[select: :id]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: p.id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: p.id`

test name: "Rule Statement 3: select single field"

**Rule Statement 4:**

**Given** filter params: `[select: [:id, :title]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: [p.id, p.title]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: [p.id, p.title]`

test name: "Rule Statement 4: select list of fields"

**Rule Statement 5:**

**Given** filter params: `[select: [map: [:id, :title]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: %{id: p.id, title: p.title}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: %{id: p.id, title: p.title}`

test name: "Rule Statement 5: select map of fields"

**Rule Statement 6:**

**Given** filter params: `[select: [map: [custom_id: :id]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: %{custom_id: p.id}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: %{custom_id: p.id}`

test name: "Rule Statement 6: select map with renamed key"

**Rule Statement 7:**

**Given** filter params: `[select: [struct: [:id]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: struct(p, [:id])`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: struct(p, [:id])`

test name: "Rule Statement 7: select struct"

**Rule Statement 8:**

**Given** filter params: `[distinct: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: true`

test name: "Rule Statement 8: distinct true"

**Rule Statement 9:**

**Given** filter params: `[group_by: :author_id]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `group_by: p.author_id`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `group_by: p.author_id`

test name: "Rule Statement 9: group by single field"

**Rule Statement 10:**

**Given** filter params: `[having: [published: true]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: p.published == true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: p.published == true`

test name: "Rule Statement 10: having clause with equality"

**Rule Statement 11:**

**Given** filter params: `[order_by: :title]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `order_by: [asc: p.title]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `order_by: [asc: p.title]`

test name: "Rule Statement 11: order by field ascending"

**Rule Statement 12:**

**Given** filter params: `[order_by: [desc: :title]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `order_by: [desc: p.title]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `order_by: [desc: p.title]`

test name: "Rule Statement 12: order by field descending"

**Rule Statement 13:**

**Given** filter params: `[limit: 10]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `limit: 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `limit: 10`

test name: "Rule Statement 13: limit results"

**Rule Statement 14:**

**Given** filter params: `[offset: 5]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `offset: 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `offset: 5`

test name: "Rule Statement 14: offset results"

**Rule Statement 15:**

**Given** filter params: `[limit: 10, offset: 5]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `limit: 10, offset: 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `limit: 10, offset: 5`

test name: "Rule Statement 15: limit and offset"

**Rule Statement 16:**

**Given** filter params: `[start_date: ~N[2026-01-01 00:00:00]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: p.inserted_at >= ^start_date`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: p.inserted_at >= ^start_date`

test name: "Rule Statement 16: start_date filter"

**Rule Statement 17:**

**Given** filter params: `[end_date: ~N[2026-12-31 23:59:59]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: p.inserted_at <= ^end_date`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: p.inserted_at <= ^end_date`

test name: "Rule Statement 17: end_date filter"

**Rule Statement 18:**

**Given** filter params: `[ids: [1, 2, 3]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: p.id in [1, 2, 3]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: p.id in [1, 2, 3]`

test name: "Rule Statement 18: ids filter"

**Rule Statement 19:**

**Given** filter params: `[dynamic: dynamic([p], p.views > ^10)]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `dynamic([p], p.views > ^10)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `dynamic([p], p.views > ^10)`

test name: "Rule Statement 19: raw dynamic expression"

**Rule Statement 20:**

**Given** filter params: `[where: [dynamic: dynamic([p], p.published === ^true)]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: ^dynamic([p], p.published === ^true)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: ^dynamic([p], p.published === ^true)`

test name: "Rule Statement 20: dynamic within where clause"

**Rule Statement 21:**

**Given** filter params: `[or_where: [dynamic: dynamic([p], p.views > ^100)]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `or_where: ^dynamic([p], p.views > ^100)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `or_where: ^dynamic([p], p.views > ^100)`

test name: "Rule Statement 21: dynamic within or_where clause"

**Rule Statement 22:**

**Given** filter params: `[where: [exists: subquery_expr]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: exists(subquery_expr)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: exists(subquery_expr)`

test name: "Rule Statement 22: exists subquery within where clause"

**Rule Statement 23:**

**Given** filter params: `[where: [not: [exists: subquery_expr]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: not exists(subquery_expr)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: not exists(subquery_expr)`

test name: "Rule Statement 23: negated exists subquery within where clause"

**Rule Statement 24:**

**Given** filter params: `[from: Post, id: 1]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `from(p in EctoShorts.TestPost, where: p.id == 1)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, where: p.id == 1)`

test name: "Rule Statement 24: from Post with id filter"

**Rule Statement 25:**

**Given** filter params: `[from: "posts", id: 1]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `from(p in "posts", where: p.id == 1)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in "posts", where: p.id == 1)`

test name: "Rule Statement 25: from table string with id filter"

**Rule Statement 26:**

**Given** filter params: `[from: "posts", select: [:id]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `from(p in "posts", select: p.id)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in "posts", select: p.id)`

test name: "Rule Statement 26: from table string with select"

**Rule Statement 27:**

**Given** filter params: `[select_merge: [map: [custom_id: :id]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select_merge: %{custom_id: p.id}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select_merge: %{custom_id: p.id}`

test name: "Rule Statement 27: select_merge map with renamed key"

**Rule Statement 28:**

**Given** filter params: `[select_merge: [map: [:id, :title]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select_merge: %{id: p.id, title: p.title}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select_merge: %{id: p.id, title: p.title}`

test name: "Rule Statement 28: select_merge map with field list"

**Rule Statement 29:**

**Given** filter params: `[select: [map: [:id]], select_merge: [map: [post_title: :title]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `select: %{id: p.id}, select_merge: %{post_title: p.title}`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `select: %{id: p.id}, select_merge: %{post_title: p.title}`

test name: "Rule Statement 29: select then select_merge"

**Rule Statement 30:**

**Given** filter params: `[distinct: false]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: false`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: false`

test name: "Rule Statement 30: distinct false"

**Rule Statement 31:**

**Given** filter params: `[distinct: :title]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: p.title`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: p.title`

test name: "Rule Statement 31: distinct on field"

**Rule Statement 32:**

**Given** filter params: `[distinct: [desc: :title]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: [desc: p.title]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: [desc: p.title]`

test name: "Rule Statement 32: distinct on field descending"

**Rule Statement 33:**

**Given** filter params: `[distinct: :title, order_by: :id]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `distinct: p.title, order_by: [asc: p.id]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `distinct: p.title, order_by: [asc: p.id]`

test name: "Rule Statement 33: distinct with order_by"

**Rule Statement 34:**

**Given** filter params: `[group_by: [:author_id, :published]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `group_by: [p.author_id, p.published]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `group_by: [p.author_id, p.published]`

test name: "Rule Statement 34: group by multiple fields"

**Rule Statement 35:**

**Given** filter params: `[having: [views: [>: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: p.views > 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: p.views > 10`

test name: "Rule Statement 35: having views greater than"

**Rule Statement 36:**

**Given** filter params: `[having: [views: [avg: [>: 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: avg(p.views) > 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: avg(p.views) > 10`

test name: "Rule Statement 36: having avg views greater than"

**Rule Statement 37:**

**Given** filter params: `[having: dynamic([p], p.views > ^10)]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: ^dynamic([p], p.views > ^10)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: ^dynamic([p], p.views > ^10)`

test name: "Rule Statement 37: having dynamic expression"

**Rule Statement 38:**

**Given** filter params: `[having: [and: [published: true, views: [>: 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: p.published == true and p.views > 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: p.published == true and p.views > 10`

test name: "Rule Statement 38: having and"

**Rule Statement 39:**

**Given** filter params: `[having: [or: [views: [>: 10], views: [<: 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: p.views > 10 or p.views < 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: p.views > 10 or p.views < 5`

test name: "Rule Statement 39: having or"

**Rule Statement 40:**

**Given** filter params: `[having: [not: [views: [>: 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `having: not (p.views > 10)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `having: not (p.views > 10)`

test name: "Rule Statement 40: having not views greater than"

**Rule Statement 41:**

**Given** filter params: `[or_having: [views: [<: 5]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `or_having: p.views < 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `or_having: p.views < 5`

test name: "Rule Statement 41: or_having"

**Rule Statement 42:**

**Given** filter params: `[or_having: [views: [avg: [<: 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `or_having: avg(p.views) < 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `or_having: avg(p.views) < 5`

test name: "Rule Statement 42: or_having avg views less than"

**Rule Statement 43:**

**Given** filter params: `[order_by: [asc: :title, desc: :id]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `order_by: [asc: p.title, desc: p.id]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `order_by: [asc: p.title, desc: p.id]`

test name: "Rule Statement 43: order by asc and desc"

**Rule Statement 44:**

**Given** filter params: `[prepend_order_by: :title]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `prepend_order_by([p], asc: p.title)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `prepend_order_by([p], asc: p.title)`

test name: "Rule Statement 44: prepend_order_by single field"

**Rule Statement 45:**

**Given** filter params: `[prepend_order_by: [asc: :published_at, desc: :title]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `prepend_order_by([p], asc: p.published_at, desc: p.title)`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `prepend_order_by([p], asc: p.published_at, desc: p.title)`

test name: "Rule Statement 45: prepend_order_by multiple fields"

**Rule Statement 46:**

**Given** filter params: `[after: 10]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: p.id > 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: p.id > 10`

test name: "Rule Statement 46: after raw id"

**Rule Statement 47:**

**Given** filter params: `[before: 10]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `where: p.id < 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `where: p.id < 10`

test name: "Rule Statement 47: before raw id"

**Rule Statement 48:**

**Given** filter params: `[first: 10]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query uses `limit: 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `limit: 10`

test name: "Rule Statement 48: first N records"

**Rule Statement 49:**

**Given** filter params: `[reverse_order: true]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query reverses an existing ascending `:id` order  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, order_by: [asc: p.id]) |> reverse_order()`

test name: "Rule Statement 49: reverse order"

**Rule Statement 50:**

**Given** filter params: `[exclude: :order_by]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query excludes the existing `order_by` clause from the starting query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, order_by: [asc: p.id]) |> exclude(:order_by)`

test name: "Rule Statement 50: exclude order_by"

**Rule Statement 51:**

**Given** filter params: `[exclude: [:order_by, :limit]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query excludes the existing `order_by` and `limit` clauses from the starting query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, order_by: [asc: p.id], limit: 5) |> exclude(:order_by) |> exclude(:limit)`

test name: "Rule Statement 51: exclude order_by and limit"

**Rule Statement 52:**

**Given** filter params: `[put_query_prefix: "tenant_a"]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query applies the `"tenant_a"` prefix to the starting query  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost) |> put_query_prefix("tenant_a")`

test name: "Rule Statement 52: put_query_prefix single tenant"

**Rule Statement 53:**

**Given** filter params: `[put_query_prefix: "tenant_a", put_query_prefix: "tenant_b"]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided query configuration directives  
**And** it must check whether the query applies both prefixes in order, with the last prefix winning  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost) |> put_query_prefix("tenant_a") |> put_query_prefix("tenant_b")`

test name: "Rule Statement 53: put_query_prefix last wins"
