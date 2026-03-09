
## Arithmetic Operator Directives

- `:+`: addition
- `:-`: subtraction
- `:*`: multiplication
- `:/`: division

### Examples

    [views: [>: [+: [:views, 10]]]]
    [not: [views: [>: [+: [:views, 10]]]]]
    [views: [>=: [-: [:views, 5]]]]
    [views: [<: [*: [:views, 2]]]]
    [views: [==: [/: [:views, 2]]]]
    [views: [!=: [+: [:views, 10]]]]
    [not: [views: [>=: [-: [:views, 5]]]]]
    [not: [views: [<: [*: [:views, 2]]]]]
    [views: [<=: [+: [10, 5]]]]
    [views: [>: [-: [100, 10]]]]
    [views: [<=: [*: [10, 2]]]]
    [views: [==: [/: [10, 2]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[views: [>: [+: [:views, 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply addition before the greater-than comparison  
**And** it must check whether the `:views` field is greater than the `:views` field plus `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views > p.views + 10`

test name: "Rule Statement 1: views greater than views plus 10"

**Rule Statement 2:**

**Given** filter params: `[not: [views: [>: [+: [:views, 10]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than comparison after applying addition  
**And** it must check whether the `:views` field is not greater than the `:views` field plus `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views > p.views + 10)`

test name: "Rule Statement 2: views greater than views plus 10 negated"

**Rule Statement 3:**

**Given** filter params: `[views: [>=: [-: [:views, 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply subtraction before the greater-than-or-equal comparison  
**And** it must check whether the `:views` field is greater than or equal to the `:views` field minus `5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views >= p.views - 5`

test name: "Rule Statement 3: views greater than or equal to views minus 5"

**Rule Statement 4:**

**Given** filter params: `[views: [<: [*: [:views, 2]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply multiplication before the less-than comparison  
**And** it must check whether the `:views` field is less than the `:views` field times `2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views < p.views * 2`

test name: "Rule Statement 4: views less than views times 2"

**Rule Statement 5:**

**Given** filter params: `[views: [==: [/: [:views, 2]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply division before the equality comparison  
**And** it must check whether the `:views` field equals the `:views` field divided by `2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views == p.views / 2`

test name: "Rule Statement 5: views equals views divided by 2"

**Rule Statement 6:**

**Given** filter params: `[views: [!=: [+: [:views, 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply addition before the inequality comparison  
**And** it must check whether the `:views` field is not equal to the `:views` field plus `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views != p.views + 10`

test name: "Rule Statement 6: views not equals views plus 10"

**Rule Statement 7:**

**Given** filter params: `[not: [views: [>=: [-: [:views, 5]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the greater-than-or-equal comparison after applying subtraction  
**And** it must check whether the `:views` field is not greater than or equal to the `:views` field minus `5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views >= p.views - 5)`

test name: "Rule Statement 7: views greater than or equal to views minus 5 negated"

**Rule Statement 8:**

**Given** filter params: `[not: [views: [<: [*: [:views, 2]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the less-than comparison after applying multiplication  
**And** it must check whether the `:views` field is not less than the `:views` field times `2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.views < p.views * 2)`

test name: "Rule Statement 8: views less than views times 2 negated"

**Rule Statement 9:**

**Given** filter params: `[views: [<=: [+: [10, 5]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply addition before the less-than-or-equal comparison  
**And** it must check whether the `:views` field is less than or equal to `10 + 5`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views <= 10 + 5`

test name: "Rule Statement 9: views less than or equal to literal 10 plus 5"

**Rule Statement 10:**

**Given** filter params: `[views: [>: [-: [100, 10]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply subtraction before the greater-than comparison  
**And** it must check whether the `:views` field is greater than the result of `100 - 10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views > ^(100 - 10)`

test name: "Rule Statement 10: views greater than literal 100 minus 10"

**Rule Statement 11:**

**Given** filter params: `[views: [<=: [*: [10, 2]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply multiplication before the less-than-or-equal comparison  
**And** it must check whether the `:views` field is less than or equal to `10 * 2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views <= 10 * 2`

test name: "Rule Statement 11: views less than or equal to literal 10 times 2"

**Rule Statement 12:**

**Given** filter params: `[views: [==: [/: [10, 2]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply division before the equality comparison  
**And** it must check whether the `:views` field equals `10 / 2`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views == 10 / 2`

test name: "Rule Statement 12: views equals literal 10 divided by 2"
