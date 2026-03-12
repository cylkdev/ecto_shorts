## Lock Directives

- `:lock`: query locking

### Examples

    [lock: fn query -> from(p in query, lock: "FOR UPDATE") end]
    [lock: [name: :for_share]]
    [lock: [name: :for_update_with_clause, values: [clause: "SKIP LOCKED"]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[lock: fn query -> from(p in query, lock: "FOR UPDATE") end]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided lock operators  
**And** it must check whether the query uses `lock: "FOR UPDATE"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, lock: "FOR UPDATE")`

test name: "Rule Statement 1: lock for update"

**Rule Statement 2:**

**Given** filter params: `[lock: [name: :for_share]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided lock operators  
**And** it must check whether the query uses `lock: "FOR SHARE"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, lock: "FOR SHARE")`

test name: "Rule Statement 2: lock for share"

**Rule Statement 3:**

**Given** filter params: `[lock: [name: :for_update_with_clause, values: [clause: "SKIP LOCKED"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided lock operators  
**And** it must check whether the query uses `lock: "FOR UPDATE SKIP LOCKED"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, lock: "FOR UPDATE SKIP LOCKED")`

test name: "Rule Statement 3: lock with values"
