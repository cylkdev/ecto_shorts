
## Binding Selector Directives

- Positional binding selectors are 1-based. `at: 1` and `at: :first` both target the first binding. `at: :last` targets the final binding in the current binding list.
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

**Assumed starting query:** `from(p in EctoShorts.TestPost)`

**Given** filter params: `[bind: [at: :last, title: "Published"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the positional binding selector  
**And** it must check whether the last binding has `:title == "Published"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "Published"`

test name: "Rule Statement 7: last binding without join"

**Rule Statement 8:**

**Assumed starting query:** `from(p in EctoShorts.TestPost, join: a in assoc(p, :author))`

**Given** filter params: `[bind: [at: :last, first_name: "John"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the positional binding selector  
**And** it must check whether the last binding has `:first_name == "John"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `a.first_name == "John"`

test name: "Rule Statement 8: last binding with author first name"

---