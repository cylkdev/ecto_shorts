
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

test name: "Rule Statement 9: explicit and with keyword conditions"

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

test name: "Rule Statement 11: or with keyword conditions"

**Rule Statement 12:**

**Given** filter params: `[or: [[id: 1, or: [title: "hello", body: "world"]], [published: true]]]`  
**When** the filter params are converted into a query condition  
**Then** it must preserve the nested `or` grouping inside the first branch of the top-level `or`  
**And** it must check whether the `:id` field equals `1` and either the `:title` field equals `"hello"` or the `:body` field equals `"world"`, or whether the `:published` field equals `true`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `(p.id == 1 and (p.title == "hello" or p.body == "world")) or p.published == true`

test name: "Rule Statement 12: nested or within and"
