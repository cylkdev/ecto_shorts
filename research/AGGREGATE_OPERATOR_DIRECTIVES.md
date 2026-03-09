
## Aggregate Operator Directives

- `:avg`: average
- `:count`: count
- `:max`: maximum
- `:min`: minimum
- `:sum`: sum

### Examples

    [views: [avg: [>: 10]]]
    [not: [views: [avg: [>: 10]]]]
    [views: [count: [>: 0]]]
    [views: [max: [>=: 100]]]
    [views: [min: [<: 5]]]
    [views: [sum: [==: 1000]]]
    [views: [avg: [!=: 50]]]
    [views: [count: [==: 0]]]
    [not: [views: [count: [>: 0]]]]
    [not: [views: [max: [>=: 100]]]]
    [views: [avg: [<=: 10]]]
    [views: [sum: [>: 500]]]
    [views: [min: [==: 0]]]
    [views: [sum: [!=: 0]]]
    [not: [views: [min: [<: 5]]]]
    [not: [views: [sum: [>: 500]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[views: [avg: [>: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `avg` aggregate before the greater-than comparison  
**And** it must check whether the average of the `:views` field is greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `avg(p.views) > 10`

test name: "Rule Statement 1: avg views greater than"

**Rule Statement 2:**

**Given** filter params: `[not: [views: [avg: [>: 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated greater-than comparison  
**And** it must check whether the average of the `:views` field is not greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (avg(p.views) > 10)`

test name: "Rule Statement 2: avg views greater than negated"

**Rule Statement 3:**

**Given** filter params: `[views: [count: [>: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `count` aggregate before the greater-than comparison  
**And** it must check whether the count of the `:views` field is greater than `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `count(p.views) > 0`

test name: "Rule Statement 3: count views greater than zero"

**Rule Statement 4:**

**Given** filter params: `[views: [max: [>=: 100]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `max` aggregate before the greater-than-or-equal comparison  
**And** it must check whether the maximum of the `:views` field is greater than or equal to `100`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `max(p.views) >= 100`

test name: "Rule Statement 4: max views greater than or equal"

**Rule Statement 5:**

**Given** filter params: `[views: [min: [<: 5]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `min` aggregate before the less-than comparison  
**And** it must check whether the minimum of the `:views` field is less than `5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `min(p.views) < 5`

test name: "Rule Statement 5: min views less than"

**Rule Statement 6:**

**Given** filter params: `[views: [sum: [==: 1000]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `sum` aggregate before the equality comparison  
**And** it must check whether the sum of the `:views` field equals `1000`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `sum(p.views) == 1000`

test name: "Rule Statement 6: sum views equals"

**Rule Statement 7:**

**Given** filter params: `[views: [avg: [!=: 50]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `avg` aggregate before the inequality comparison  
**And** it must check whether the average of the `:views` field is not equal to `50`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `avg(p.views) != 50`

test name: "Rule Statement 7: avg views not equals"

**Rule Statement 8:**

**Given** filter params: `[views: [count: [==: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison to the aggregate result  
**And** it must check whether the count of the `:views` field equals `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `count(p.views) == 0`

test name: "Rule Statement 8: count views equals zero"

**Rule Statement 9:**

**Given** filter params: `[not: [views: [count: [>: 0]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated greater-than comparison  
**And** it must check whether the count of the `:views` field is not greater than `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (count(p.views) > 0)`

test name: "Rule Statement 9: count views greater than zero negated"

**Rule Statement 10:**

**Given** filter params: `[not: [views: [max: [>=: 100]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated greater-than-or-equal comparison  
**And** it must check whether the maximum of the `:views` field is not greater than or equal to `100`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (max(p.views) >= 100)`

test name: "Rule Statement 10: max views greater than or equal negated"

**Rule Statement 11:**

**Given** filter params: `[views: [avg: [<=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `avg` aggregate before the less-than-or-equal comparison  
**And** it must check whether the average of the `:views` field is less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `avg(p.views) <= 10`

test name: "Rule Statement 11: avg views less than or equal"

**Rule Statement 12:**

**Given** filter params: `[views: [sum: [>: 500]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `sum` aggregate before the greater-than comparison  
**And** it must check whether the sum of the `:views` field is greater than `500`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `sum(p.views) > 500`

test name: "Rule Statement 12: sum views greater than"

**Rule Statement 13:**

**Given** filter params: `[views: [min: [==: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `min` aggregate before the equality comparison  
**And** it must check whether the minimum of the `:views` field equals `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `min(p.views) == 0`

test name: "Rule Statement 13: min views equals zero"

**Rule Statement 14:**

**Given** filter params: `[views: [sum: [!=: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `sum` aggregate before the inequality comparison  
**And** it must check whether the sum of the `:views` field is not equal to `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `sum(p.views) != 0`

test name: "Rule Statement 14: sum views not equals zero"

**Rule Statement 15:**

**Given** filter params: `[not: [views: [min: [<: 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated less-than comparison  
**And** it must check whether the minimum of the `:views` field is not less than `5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (min(p.views) < 5)`

test name: "Rule Statement 15: min views less than negated"

**Rule Statement 16:**

**Given** filter params: `[not: [views: [sum: [>: 500]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the aggregated greater-than comparison  
**And** it must check whether the sum of the `:views` field is not greater than `500`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (sum(p.views) > 500)`

test name: "Rule Statement 16: sum views greater than negated"
