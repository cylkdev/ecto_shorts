
## Array Fields

Array fields (e.g., `{:array, :string}` in Ecto schemas) support special comparison and containment operations.

- `:in`: Check if value is contained in array (for single values)
- `:all`: Apply operation to all array elements
- `:count`: Count array elements and compare
- Comparison operators use field-side semantics
- String matching (like, ilike) works on array elements
- Transformations (lower, upper) work on array elements

For comparison operators, the array field remains the logical left-hand side. For example, `[tags: [>: "elixir"]]` means at least one value in `:tags` is greater than `"elixir"`.

### Examples

    [tags: "elixir"]
    [tags: [==: "elixir"]]
    [tags: [!=: "elixir"]]
    [tags: [in: "elixir"]]
    [tags: ["elixir", "erlang"]]
    [tags: [==: ["elixir", "erlang"]]]
    [tags: [!=: ["elixir"]]]
    [tags: nil]
    [tags: [==: nil]]
    [tags: [!=: nil]]
    [tags: [in: ["elixir"]]]
    [tags: [all: [in: ["elixir", "erlang"]]]]
    [not: [tags: [==: ["elixir"]]]]
    [not: [tags: [in: "elixir"]]]
    [tags: [>: "elixir"]]
    [tags: [>=: "elixir"]]
    [tags: [<: "elixir"]]
    [tags: [<=: "elixir"]]
    [tags: [like: "elixir"]]
    [tags: [ilike: "elixir"]]
    [tags: [like: ["%elixir%", "%erlang%"]]]
    [not: [tags: [like: "elixir"]]]
    [tags: [==: [lower: "elixir"]]]
    [tags: [!=: [upper: "ELIXIR"]]]
    [tags: [count: [>: 0]]]
    [tags: [count: [==: 0]]]
    [tags: [all: [>: "a"]]]
    [tags: [ilike: ["elixir", "erlang"]]]
    [not: [tags: [ilike: ["%elixir%"]]]]
    [lower: [tags: [==: "elixir"]]]
    [upper: [tags: [==: "ELIXIR"]]]
    [tags: [count: [<: 5]]]
    [tags: [count: [>=: 2]]]
    [tags: [count: [<=: 10]]]
    [tags: [count: [!=: 3]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[tags: "elixir"]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a membership check against the array field  
**And** it must check whether `"elixir"` is in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" in p.tags`

test name: "Rule Statement 1: tags contains single value"

**Rule Statement 2:**

**Given** filter params: `[tags: [==: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a membership check against the array field  
**And** it must check whether `"elixir"` is in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" in p.tags`

test name: "Rule Statement 2: tags equals operator with single value"

**Rule Statement 3:**

**Given** filter params: `[tags: [!=: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated membership check against the array field  
**And** it must check whether `"elixir"` is not in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" not in p.tags`

test name: "Rule Statement 3: tags not equals single value"

**Rule Statement 4:**

**Given** filter params: `[tags: [in: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a membership check against the array field  
**And** it must check whether `"elixir"` is in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" in p.tags`

test name: "Rule Statement 4: tags in operator with single value"

**Rule Statement 5:**

**Given** filter params: `[tags: ["elixir", "erlang"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison against the array field  
**And** it must check whether the `:tags` field is exactly `["elixir", "erlang"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.tags == ^["elixir", "erlang"]`

test name: "Rule Statement 5: tags equals array"

**Rule Statement 6:**

**Given** filter params: `[tags: [==: ["elixir", "erlang"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison against the array field  
**And** it must check whether the `:tags` field is exactly `["elixir", "erlang"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.tags == ^["elixir", "erlang"]`

test name: "Rule Statement 6: tags equals operator with array"

**Rule Statement 7:**

**Given** filter params: `[tags: [!=: ["elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison against the array field  
**And** it must check whether the `:tags` field is not exactly `["elixir"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `p.tags != ^["elixir"]`

test name: "Rule Statement 7: tags not equals array"

**Rule Statement 8:**

**Given** filter params: `[tags: nil]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a nil check against the array field  
**And** it must check whether the `:tags` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.tags)`

test name: "Rule Statement 8: tags is nil"

**Rule Statement 9:**

**Given** filter params: `[tags: [==: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the equality comparison as a nil check against the array field  
**And** it must check whether the `:tags` field is `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `is_nil(p.tags)`

test name: "Rule Statement 9: tags equals nil"

**Rule Statement 10:**

**Given** filter params: `[tags: [!=: nil]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the inequality comparison as a non-nil check against the array field  
**And** it must check whether the `:tags` field is not `nil`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not is_nil(p.tags)`

test name: "Rule Statement 10: tags not equals nil"

**Rule Statement 11:**

**Given** filter params: `[tags: [in: ["elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply an overlap check  
**And** it must check whether the `:tags` field overlaps with the list `["elixir"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? && ?", p.tags, ^["elixir"])`

test name: "Rule Statement 11: tags array overlaps"

**Rule Statement 12:**

**Given** filter params: `[tags: [all: [in: ["elixir", "erlang"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a containment check  
**And** it must check whether every value in `:tags` is included in `["elixir", "erlang"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? <@ ?", p.tags, ^["elixir", "erlang"])`

test name: "Rule Statement 12: tags array is contained in list"

**Rule Statement 13:**

**Given** filter params: `[not: [tags: [==: ["elixir"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated equality comparison against the array field  
**And** it must check whether the `:tags` field is not exactly `["elixir"]`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not (p.tags == ^["elixir"])`

test name: "Rule Statement 13: negated tags equals array"

**Rule Statement 14:**

**Given** filter params: `[not: [tags: [in: "elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated membership check against the array field  
**And** it must check whether `"elixir"` is not in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `"elixir" not in p.tags`

test name: "Rule Statement 14: negated tags contains value"

**Rule Statement 15:**

**Given** filter params: `[tags: [>: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than comparison against any element of the array field  
**And** it must check whether at least one value in `:tags` is greater than `"elixir"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? < ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 15: any tag greater than value"

**Rule Statement 16:**

**Given** filter params: `[tags: [>=: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than-or-equal comparison against any element of the array field  
**And** it must check whether at least one value in `:tags` is greater than or equal to `"elixir"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? <= ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 16: any tag greater than or equal to value"

**Rule Statement 17:**

**Given** filter params: `[tags: [<: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than comparison against any element of the array field  
**And** it must check whether at least one value in `:tags` is less than `"elixir"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? > ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 17: any tag less than value"

**Rule Statement 18:**

**Given** filter params: `[tags: [<=: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the less-than-or-equal comparison against any element of the array field  
**And** it must check whether at least one value in `:tags` is less than or equal to `"elixir"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? >= ANY(?)", ^"elixir", p.tags)`

test name: "Rule Statement 18: any tag less than or equal to value"

**Rule Statement 19:**

**Given** filter params: `[tags: [like: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-sensitive pattern match against any element of the array field  
**And** it must check whether at least one value in `:tags` matches the provided pattern  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ?\n)\n", p.tags, ^"elixir")`

test name: "Rule Statement 19: tags any like"

**Rule Statement 20:**

**Given** filter params: `[tags: [ilike: "elixir"]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-insensitive pattern match against any element of the array field  
**And** it must check whether at least one value in `:tags` matches the provided pattern case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ?\n)\n", p.tags, ^"elixir")`

test name: "Rule Statement 20: tags any ilike"

**Rule Statement 21:**

**Given** filter params: `[tags: [like: ["%elixir%", "%erlang%"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-sensitive pattern match against any element of the array field  
**And** it must check whether at least one value in `:tags` matches at least one of the provided patterns  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ANY (?)\n)\n", p.tags, ^["%elixir%", "%erlang%"])`

test name: "Rule Statement 21: tags any like any"

**Rule Statement 22:**

**Given** filter params: `[not: [tags: [like: "elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated case-sensitive pattern match against any element of the array field  
**And** it must check whether no value in `:tags` matches the provided pattern  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t LIKE ?\n)\n", p.tags, ^"elixir")`

test name: "Rule Statement 22: negated tags any like"

**Rule Statement 23:**

**Given** filter params: `[tags: [==: [lower: "elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the lower transformation before the membership check against the array field  
**And** it must check whether the lowercased value is in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("lower(?)", "elixir") in p.tags`

test name: "Rule Statement 23: tags contains lowercased value"

**Rule Statement 24:**

**Given** filter params: `[tags: [!=: [upper: "ELIXIR"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the upper transformation before the negated membership check against the array field  
**And** it must check whether the uppercased value is not in the `:tags` field  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("upper(?)", "ELIXIR") not in p.tags`

test name: "Rule Statement 24: tags not contains uppercased value"

**Rule Statement 25:**

**Given** filter params: `[tags: [count: [>: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the greater-than comparison  
**And** it must check whether the `:tags` field contains more than `0` elements  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) > 0`

test name: "Rule Statement 25: tags count greater than"

**Rule Statement 26:**

**Given** filter params: `[tags: [count: [==: 0]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the equality comparison  
**And** it must check whether the `:tags` field has a count of `0`, treating both `nil` and empty arrays as `0`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("coalesce(array_length(?, 1), 0)", p.tags) == 0`

test name: "Rule Statement 26: tags count equals zero"

**Rule Statement 27:**

**Given** filter params: `[tags: [all: [>: "a"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the greater-than comparison against all elements of the array field  
**And** it must check whether every value in `:tags` is greater than `"a"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("? < ALL(?)", ^"a", p.tags)`

test name: "Rule Statement 27: all tags greater than value"

**Rule Statement 28:**

**Given** filter params: `[tags: [ilike: ["elixir", "erlang"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a case-insensitive pattern match against any element of the array field  
**And** it must check whether at least one value in `:tags` matches at least one of the provided patterns case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", p.tags, ^["elixir", "erlang"])`

test name: "Rule Statement 28: tags any ilike any"

**Rule Statement 29:**

**Given** filter params: `[not: [tags: [ilike: ["%elixir%"]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply a negated case-insensitive pattern match against any element of the array field  
**And** it must check whether no value in `:tags` matches the provided pattern case-insensitively  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `not fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE t ILIKE ANY (?)\n)\n", p.tags, ^["%elixir%"])`

test name: "Rule Statement 29: negated tags any ilike"

**Rule Statement 30:**

**Given** filter params: `[lower: [tags: [==: "elixir"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the lower transformation to the array field before the equality comparison  
**And** it must check whether any lowercased value in `:tags` equals `"elixir"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE lower(t) = ?\n)\n", p.tags, ^"elixir")`

test name: "Rule Statement 30: lower tags equals value"

**Rule Statement 31:**

**Given** filter params: `[upper: [tags: [==: "ELIXIR"]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the upper transformation to the array field before the equality comparison  
**And** it must check whether any uppercased value in `:tags` equals `"ELIXIR"`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("EXISTS (\n  SELECT 1\n  FROM unnest(?) AS t\n  WHERE upper(t) = ?\n)\n", p.tags, ^"ELIXIR")`

test name: "Rule Statement 31: upper tags equals value"

**Rule Statement 32:**

**Given** filter params: `[tags: [count: [<: 5]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the less-than comparison  
**And** it must check whether the `:tags` field contains fewer than `5` elements  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) < 5`

test name: "Rule Statement 32: tags count less than"

**Rule Statement 33:**

**Given** filter params: `[tags: [count: [>=: 2]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the greater-than-or-equal comparison  
**And** it must check whether the `:tags` field contains at least `2` elements  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) >= 2`

test name: "Rule Statement 33: tags count greater than or equal"

**Rule Statement 34:**

**Given** filter params: `[tags: [count: [<=: 10]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the less-than-or-equal comparison  
**And** it must check whether the `:tags` field contains at most `10` elements  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) <= 10`

test name: "Rule Statement 34: tags count less than or equal"

**Rule Statement 35:**

**Given** filter params: `[tags: [count: [!=: 3]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the count helper before the inequality comparison  
**And** it must check whether the `:tags` field has a count that is not equal to `3`  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) != 3`

test name: "Rule Statement 35: tags count not equals"
