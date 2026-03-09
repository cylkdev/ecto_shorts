## Set Directives

Assumed starting query for set operations: `base_query = from(p in EctoShorts.TestPost)`

- `:except`: set except
- `:except_all`: set except all
- `:intersect`: set intersect
- `:intersect_all`: set intersect all
- `:union`: set union
- `:union_all`: set union all

### Examples

    [except: [published: false]]
    [except: from(p in EctoShorts.TestPost, where: p.published == ^false)]
    [except_all: [published: false]]
    [intersect: [published: false]]
    [intersect_all: [published: false]]
    [union: [published: false]]
    [union_all: [published: false]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[except: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `except/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `base_query = from(p in EctoShorts.TestPost); except_query = from(q in EctoShorts.TestPost, where: q.published == false); except(base_query, ^except_query)`

test name: "Rule Statement 1: except with filter params"

**Rule Statement 2:**

**Given** filter params: `[except: from(p in EctoShorts.TestPost, where: p.published == ^false)]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must use the provided raw query exactly and apply it with `except/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `base_query = from(p in EctoShorts.TestPost); except_query = from(p in EctoShorts.TestPost, where: p.published == ^false); except(base_query, ^except_query)`

test name: "Rule Statement 2: except with raw query"

**Rule Statement 3:**

**Given** filter params: `[except_all: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `except_all/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `base_query = from(p in EctoShorts.TestPost); except_query = from(q in EctoShorts.TestPost, where: q.published == false); except_all(base_query, ^except_query)`

test name: "Rule Statement 3: except_all with filter params"

**Rule Statement 4:**

**Given** filter params: `[intersect: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `intersect/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `base_query = from(p in EctoShorts.TestPost); intersect_query = from(q in EctoShorts.TestPost, where: q.published == false); intersect(base_query, ^intersect_query)`

test name: "Rule Statement 4: intersect with filter params"

**Rule Statement 5:**

**Given** filter params: `[intersect_all: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `intersect_all/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `base_query = from(p in EctoShorts.TestPost); intersect_query = from(q in EctoShorts.TestPost, where: q.published == false); intersect_all(base_query, ^intersect_query)`

test name: "Rule Statement 5: intersect_all with filter params"

**Rule Statement 6:**

**Given** filter params: `[union: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `union/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `base_query = from(p in EctoShorts.TestPost); union_query = from(q in EctoShorts.TestPost, where: q.published == false); union(base_query, ^union_query)`

test name: "Rule Statement 6: union with filter params"

**Rule Statement 7:**

**Given** filter params: `[union_all: [published: false]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided set directives  
**And** it must build the set query `from(q in EctoShorts.TestPost, where: q.published == false)` from the provided filter params and apply it with `union_all/2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `base_query = from(p in EctoShorts.TestPost); union_query = from(q in EctoShorts.TestPost, where: q.published == false); union_all(base_query, ^union_query)`

test name: "Rule Statement 7: union_all with filter params"
