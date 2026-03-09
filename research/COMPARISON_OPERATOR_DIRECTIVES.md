
## Comparison Operator Directives

- `:==`: equality
- `:eq`: equality alias
- `:!=`: inequality
- `:ne`: inequality alias
- `:>`: greater than
- `:>=`: greater than or equal
- `:<`: less than
- `:<=`: less than or equal
- `:gt`: greater than alias
- `:gte`: greater than or equal alias
- `:lt`: less than alias
- `:lte`: less than or equal alias
- `:in`: membership

### Examples

    [title: [==: "Post 1"]]
    [title: [eq: "Post 1"]]
    [published_at: [==: nil]]
    [published_at: [eq: nil]]
    [published_at: [!=: nil]]
    [published_at: [ne: nil]]
    [views: [>: 10]]
    [views: [>=: 10]]
    [views: [<: 10]]
    [views: [<=: 10]]
    [views: [!=: 10]]
    [views: [ne: 10]]
    [views: [gt: 10]]
    [views: [gte: 10]]
    [views: [lt: 10]]
    [views: [lte: 10]]
    [published: [in: [true, false]]]
    [published: [==: [true, false]]]
    [published: [!=: [true, false]]]
    [published: [ne: [true, false]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[title: [==: "Post 1"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison  
**And** it must check whether the `:title` field equals `"Post 1"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "Post 1"`

test name: "Rule Statement 1: title equals with == operator"

**Rule Statement 2:**

**Given** filter params: `[title: [eq: "Post 1"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality alias comparison  
**And** it must check whether the `:title` field equals `"Post 1"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.title == "Post 1"`

test name: "Rule Statement 2: title equals with eq operator"

**Rule Statement 3:**

**Given** filter params: `[published_at: [==: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a nil check  
**And** it must check whether the `:published_at` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.published_at)`

test name: "Rule Statement 3: published_at equals nil with =="

**Rule Statement 4:**

**Given** filter params: `[published_at: [eq: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality alias comparison as a nil check  
**And** it must check whether the `:published_at` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.published_at)`

test name: "Rule Statement 4: published_at equals nil with eq"

**Rule Statement 5:**

**Given** filter params: `[published_at: [!=: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison as a non-nil check  
**And** it must check whether the `:published_at` field is not `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not is_nil(p.published_at)`

test name: "Rule Statement 5: published_at not equals nil"

**Rule Statement 6:**

**Given** filter params: `[published_at: [ne: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality alias comparison as a non-nil check  
**And** it must check whether the `:published_at` field is not `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not is_nil(p.published_at)`

test name: "Rule Statement 6: published_at ne nil"

**Rule Statement 7:**

**Given** filter params: `[views: [>: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than comparison  
**And** it must check whether the `:views` field is greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views > 10`

test name: "Rule Statement 7: views greater than"

**Rule Statement 8:**

**Given** filter params: `[views: [>=: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than-or-equal comparison  
**And** it must check whether the `:views` field is greater than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views >= 10`

test name: "Rule Statement 8: views greater than or equal"

**Rule Statement 9:**

**Given** filter params: `[views: [<: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than comparison  
**And** it must check whether the `:views` field is less than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views < 10`

test name: "Rule Statement 9: views less than"

**Rule Statement 10:**

**Given** filter params: `[views: [<=: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than-or-equal comparison  
**And** it must check whether the `:views` field is less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views <= 10`

test name: "Rule Statement 10: views less than or equal"

**Rule Statement 11:**

**Given** filter params: `[views: [!=: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison  
**And** it must check whether the `:views` field is not equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views != 10`

test name: "Rule Statement 11: views not equals"

**Rule Statement 12:**

**Given** filter params: `[views: [ne: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality alias comparison  
**And** it must check whether the `:views` field is not equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views != 10`

test name: "Rule Statement 12: views ne operator"

**Rule Statement 13:**

**Given** filter params: `[views: [gt: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than alias comparison  
**And** it must check whether the `:views` field is greater than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views > 10`

test name: "Rule Statement 13: views gt operator"

**Rule Statement 14:**

**Given** filter params: `[views: [gte: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than-or-equal alias comparison  
**And** it must check whether the `:views` field is greater than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views >= 10`

test name: "Rule Statement 14: views gte operator"

**Rule Statement 15:**

**Given** filter params: `[views: [lt: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than alias comparison  
**And** it must check whether the `:views` field is less than `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views < 10`

test name: "Rule Statement 15: views lt operator"

**Rule Statement 16:**

**Given** filter params: `[views: [lte: 10]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than-or-equal alias comparison  
**And** it must check whether the `:views` field is less than or equal to `10`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.views <= 10`

test name: "Rule Statement 16: views lte operator"

**Rule Statement 17:**

**Given** filter params: `[published: [in: [true, false]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a membership check  
**And** it must check whether the `:published` field is in `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published in [true, false]`

test name: "Rule Statement 17: published in list"

**Rule Statement 18:**

**Given** filter params: `[published: [==: [true, false]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a membership check  
**And** it must check whether the `:published` field is in `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.published in [true, false]`

test name: "Rule Statement 18: published equals list"

**Rule Statement 19:**

**Given** filter params: `[published: [!=: [true, false]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison as a negated membership check that accounts for `nil`  
**And** it must check whether the `:published` field is `nil` or not in `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.published) or p.published not in [true, false]`

test name: "Rule Statement 19: published not equals list"

**Rule Statement 20:**

**Given** filter params: `[published: [ne: [true, false]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality alias comparison as a negated membership check that accounts for `nil`  
**And** it must check whether the `:published` field is `nil` or not in `[true, false]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.published) or p.published not in [true, false]`

test name: "Rule Statement 20: published ne list"
