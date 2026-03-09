
## Terminal Filter Directives

This section is the authoritative definition of top-level `:subquery`.

- `:last`: Returns the last N records by reversing order, limiting, then re-ordering
- `:subquery`: Wraps the query and any additional filters in a subquery

### Examples

    [last: 2]
    [subquery: [title: "Second"]]
    [published: true, subquery: [id: 2]]
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
