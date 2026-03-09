
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
