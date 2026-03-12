
## Date/Time Directives

- `:datetime`: Wrapper for date/time operations that preserves timestamp precision
- `:date`: Wrapper for date/time operations that compares date values without time-of-day
- Shared sub-keys for both wrappers:
  - `:add`: add time interval
  - `:ago`: time in the past
  - `:from_now`: time in the future

### Examples

    [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]
    [inserted_at: [>: [datetime: [ago: [count: 1, interval: "day"]]]]]
    [inserted_at: [>: [datetime: [from_now: [count: 1, interval: "day"]]]]]
    [not: [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]]
    [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]
    [inserted_at: [<=: [datetime: [from_now: [count: 30, interval: "day"]]]]]
    [inserted_at: [==: [date: [ago: [count: 1, interval: "day"]]]]]
    [inserted_at: [!=: [date: [from_now: [count: 1, interval: "day"]]]]]
    [not: [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]]
    [not: [inserted_at: [>: [date: [from_now: [count: 1, interval: "day"]]]]]]
    [inserted_at: [>=: [date: [add: [field: :inserted_at, count: 7, interval: "day"]]]]]
    [inserted_at: [<: [date: [ago: [count: 1, interval: "month"]]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `add` operator before the comparison  
**And** it must check whether the `:inserted_at` field is greater than or equal to `datetime_add(p.inserted_at, 1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at >= datetime_add(p.inserted_at, 1, "day")`

test name: "Rule Statement 1: inserted_at greater than or equal to datetime_add"

**Rule Statement 2:**

**Given** filter params: `[inserted_at: [>: [datetime: [ago: [count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `ago` operator before the comparison  
**And** it must check whether the `:inserted_at` field is greater than `ago(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at > ago(1, "day")`

test name: "Rule Statement 2: inserted_at greater than ago 1 day"

**Rule Statement 3:**

**Given** filter params: `[inserted_at: [>: [datetime: [from_now: [count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `from_now` operator before the comparison  
**And** it must check whether the `:inserted_at` field is greater than `from_now(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at > from_now(1, "day")`

test name: "Rule Statement 3: inserted_at greater than from_now 1 day"

**Rule Statement 4:**

**Given** filter params: `[not: [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the `datetime_add` comparison  
**And** it must check whether the `:inserted_at` field is not greater than or equal to `datetime_add(p.inserted_at, 1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.inserted_at >= datetime_add(p.inserted_at, 1, "day"))`

test name: "Rule Statement 4: inserted_at >= datetime_add negated"

**Rule Statement 5:**

**Given** filter params: `[inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `ago` operator before the comparison  
**And** it must check whether the `:inserted_at` field is less than `ago(7, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at < ago(7, "day")`

test name: "Rule Statement 5: inserted_at less than ago 7 days"

**Rule Statement 6:**

**Given** filter params: `[inserted_at: [<=: [datetime: [from_now: [count: 30, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `datetime` wrapper with the `from_now` operator before the comparison  
**And** it must check whether the `:inserted_at` field is less than or equal to `from_now(30, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.inserted_at <= from_now(30, "day")`

test name: "Rule Statement 6: inserted_at less than or equal to from_now 30 days"

**Rule Statement 7:**

**Given** filter params: `[inserted_at: [==: [date: [ago: [count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `date` wrapper with the `ago` operator before the comparison  
**And** it must check whether the date portion of `:inserted_at` equals the date portion of `ago(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("date(?)", p.inserted_at) == fragment("date(?)", ago(1, "day"))`

test name: "Rule Statement 7: inserted_at equals ago 1 day using date wrapper"

**Rule Statement 8:**

**Given** filter params: `[inserted_at: [!=: [date: [from_now: [count: 1, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `date` wrapper with the `from_now` operator before the comparison  
**And** it must check whether the date portion of `:inserted_at` is not equal to the date portion of `from_now(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("date(?)", p.inserted_at) != fragment("date(?)", from_now(1, "day"))`

test name: "Rule Statement 8: inserted_at not equals from_now 1 day using date wrapper"

**Rule Statement 9:**

**Given** filter params: `[not: [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the `ago` comparison  
**And** it must check whether the `:inserted_at` field is not less than `ago(7, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.inserted_at < ago(7, "day"))`

test name: "Rule Statement 9: inserted_at less than ago 7 days negated"

**Rule Statement 10:**

**Given** filter params: `[not: [inserted_at: [>: [date: [from_now: [count: 1, interval: "day"]]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must negate the `from_now` comparison under the `date` wrapper  
**And** it must check whether the date portion of `:inserted_at` is not greater than the date portion of `from_now(1, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (fragment("date(?)", p.inserted_at) > fragment("date(?)", from_now(1, "day")))`

test name: "Rule Statement 10: inserted_at greater than from_now 1 day negated using date wrapper"

**Rule Statement 11:**

**Given** filter params: `[inserted_at: [>=: [date: [add: [field: :inserted_at, count: 7, interval: "day"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `date` wrapper with the `add` operator before the comparison  
**And** it must check whether the date portion of `:inserted_at` is greater than or equal to the date portion of `datetime_add(p.inserted_at, 7, "day")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("date(?)", p.inserted_at) >= fragment("date(?)", datetime_add(p.inserted_at, 7, "day"))`

test name: "Rule Statement 11: inserted_at >= datetime_add 7 days using date wrapper"

**Rule Statement 12:**

**Given** filter params: `[inserted_at: [<: [date: [ago: [count: 1, interval: "month"]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the `date` wrapper with the `ago` operator before the comparison  
**And** it must check whether the date portion of `:inserted_at` is less than the date portion of `ago(1, "month")`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("date(?)", p.inserted_at) < fragment("date(?)", ago(1, "month"))`

test name: "Rule Statement 12: inserted_at less than ago 1 month using date wrapper"
