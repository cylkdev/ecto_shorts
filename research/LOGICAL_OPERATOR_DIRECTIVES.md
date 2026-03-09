
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
**And** the resulting expression is: `(p.title == "hello" or (p.views > 10 and p.views < 20)) or p.published == true`

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

test name: "Rule Statement 6: same-field or conditions"
