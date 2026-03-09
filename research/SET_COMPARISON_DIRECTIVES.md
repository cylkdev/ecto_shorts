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
