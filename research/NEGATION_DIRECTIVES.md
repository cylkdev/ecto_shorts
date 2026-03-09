
## Negation Directives

- `:not`: Wraps an entire operation in a logical NOT. Whatever condition the nested operation produces, `:not` flips it (`true` becomes `false`, and `false` becomes `true`).

### Examples

    [not: [published: [in: [true]]]]
    [not: [published: [==: [true]]]]
    [not: [published: [!=: [true, false]]]]
    [not: [views: [>: 10]]]
    [not: [views: [>=: 10]]]
    [not: [views: [<: 10]]]
    [not: [views: [<=: 10]]]
    [not: [views: [==: 10]]]
    [not: [views: [!=: 10]]]
    [not: [views: [gt: 10]]]
    [not: [views: [gte: 10]]]
    [not: [views: [lt: 10]]]
    [not: [views: [lte: 10]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[not: [published: [in: [true]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the membership check  
**And** it must check whether the `:published` field is not in the list `[true]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.published in [true])`

test name: "Rule Statement 1: negated published in list"

**Rule Statement 2:**

**Given** filter params: `[not: [published: [==: [true]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the equality comparison after applying the membership check  
**And** it must check whether the `:published` field is not in the list `[true]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.published in [true])`

test name: "Rule Statement 2: negated published equals operator with list"

**Rule Statement 3:**

**Given** filter params: `[not: [published: [!=: [true, false]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the inequality comparison after applying the nil-aware negated membership check  
**And** it must check whether the `:published` field is not `nil` and belongs to the list `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (is_nil(p.published) or p.published not in [true, false])`

test name: "Rule Statement 3: negated published not equals operator with list"

**Rule Statement 4:**

**Given** filter params: `[not: [views: [>: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than comparison  
**And** it must check whether the `:views` field is not greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views > 10)`

test name: "Rule Statement 4: negated views greater than"

**Rule Statement 5:**

**Given** filter params: `[not: [views: [>=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than-or-equal comparison  
**And** it must check whether the `:views` field is not greater than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views >= 10)`

test name: "Rule Statement 5: negated views greater than or equal"

**Rule Statement 6:**

**Given** filter params: `[not: [views: [<: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than comparison  
**And** it must check whether the `:views` field is not less than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views < 10)`

test name: "Rule Statement 6: negated views less than"

**Rule Statement 7:**

**Given** filter params: `[not: [views: [<=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than-or-equal comparison  
**And** it must check whether the `:views` field is not less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views <= 10)`

test name: "Rule Statement 7: negated views less than or equal"

**Rule Statement 8:**

**Given** filter params: `[not: [views: [==: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the equality comparison  
**And** it must check whether the `:views` field is not equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views == 10)`

test name: "Rule Statement 8: negated views equals"

**Rule Statement 9:**

**Given** filter params: `[not: [views: [!=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the inequality comparison  
**And** it must check whether the `:views` field is equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views != 10)`

test name: "Rule Statement 9: negated views not equals"

**Rule Statement 10:**

**Given** filter params: `[not: [views: [gt: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than comparison  
**And** it must check whether the `:views` field is not greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views > 10)`

test name: "Rule Statement 10: negated views gt alias"

**Rule Statement 11:**

**Given** filter params: `[not: [views: [gte: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than-or-equal comparison  
**And** it must check whether the `:views` field is not greater than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views >= 10)`

test name: "Rule Statement 11: negated views gte alias"

**Rule Statement 12:**

**Given** filter params: `[not: [views: [lt: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than comparison  
**And** it must check whether the `:views` field is not less than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views < 10)`

test name: "Rule Statement 12: negated views lt alias"

**Rule Statement 13:**

**Given** filter params: `[not: [views: [lte: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than-or-equal comparison  
**And** it must check whether the `:views` field is not less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views <= 10)`

test name: "Rule Statement 13: negated views lte alias"
