## CommonFilters

This document defines the end-to-end behaviour of the query-building API.

It uses example mapping: it lists examples, maps each example to a behaviour, and defines the rules, constraints, directives, and operators used by the API.

Every example in this document is verified by a test in `test/examples/ecto_query_dsl.exs`.

Run the examples with `mix run test/examples/run.exs`.

## Keyword Lists vs Maps

This API is designed with keyword lists as the primary input format. Keyword lists work well with the composeability of the `Ecto.Query` API because they preserve order and they let you repeat keys, which makes it easier to build up complex queries.

Because of that, this API treats maps and keyword lists as the same shape.

In other words, anywhere you can write a map (%{...}), you can also write a keyword list ([...]), and it will behave the same way.

Because of this, each pair below is equivalent:

    `%{id: 1}` means the same thing as `[id: 1]`.

    `[%{id: 1}, %{id: 2}]` means the same thing as `[[id: 1], [id: 2]]`.

    `[%{id: 1}, [id: 2]]` means the same thing as `[[id: 1], [id: 2]]`.

This works because lists can mix maps and keyword lists, and the API still treats them as the same structure.

This also works for nested values, including directive filters:

    `%{id: %{>: 1}}` means the same thing as `[id: [>: 1]]`.

Here’s what that looks like in code:

    EctoShorts.CommonFilters.convert_params_to_filter(Post, %{id: 1})
    EctoShorts.CommonFilters.convert_params_to_filter(Post, [id: 1])

    EctoShorts.CommonFilters.convert_params_to_filter(Post, %{id: %{>: 1}})
    EctoShorts.CommonFilters.convert_params_to_filter(Post, [id: [>: 1]])

---

## Query Builder Language

The query builder API’s “language” is based on directives.

A directive is a named instruction. When the parser sees a directive key (for example `:and`, `:or`, `:not`, `:lower`, `:avg`, `:date`, `:datetime`), it knows the key is not a normal field name. Instead, that key tells the parser to apply a specific behavior that changes how a value or field is interpreted.

Directives are grouped into "slots", and slots have a fixed priority order. When parsing filter params, the parser checks slots in order, from the outer-most slot to the inner-most slot.

### Understanding Slots

When you write a filter expression, the query builder reads it from the outer-most layer to the inner-most layer. At each layer, it looks for a directive that matches that layer’s rules.

You can think of it like peeling an onion: each layer is a slot, and each slot only allows certain kinds of directives.

Here’s the order the parser follows:

    Top-Level Logical Operator Directive
    (:or / :and)
        |
        v

    Negation Directive
    (:not)
        |
        v

    Field Transform Directive
    (:lower / :upper)
        |
        v

    Field (Scalar/Array)
    (:title / :views / :published)
        |
        v

    Aggregate Operator Directive
    (:avg / :sum / :count / :max / :min)
        |
        v

    Field-Level Logical Operator Directive
    (:and / :or)
        |
        v

    Comparison Operator Directive
    (:> / :== / :like / :in)
        |
        v

    Value Transformation Directive
    (:lower / :upper / :all / :any / :datetime / :date / :+ / :- / :* / :/)
        |
        v

    Value
    (1 / "hello" / [1, 2, 3])
        |
        v

#### What the slots mean

**1. Top-Level Logical Operator Directive**

Examples: `:and`, `:or`

Combines multiple filter groups into one condition.

**2. Negation Directive**

Examples: `:not`

Wraps a filter and inverts it.

**3. Field Transform Directive**

Examples: `:lower`, `:upper`

Changes the field before it is compared. Appears before the field name. String transformations only.

**4. Field (Scalar/Array)**

The schema field you are filtering on (for example `:title`, `:views`, `:published`). 

**5. Aggregate Operator Directive**

Examples: `:avg`, `:sum`, `:count`, `:max`, `:min`

Applies an aggregate function to the field. Appears after the field name. Used with `group_by` and `having` clauses.

**6. Field-Level Logical Operator Directive**

Examples: `:and`, `:or`

Combines multiple operations that all target the same field. When multiple operations appear in a list without an explicit logical operator directive, AND is implicit: `[views: [>: 10, <: 20]]` means `views > 10 and views < 20`. You can also use explicit `:and` or `:or` at the field level to change the combinator.

**7. Comparison Operator Directive**

| Directive | Alias  | Meaning                        |
| --------- | ------ | ------------------------------ |
| `:=`      | `:eq`  | equal to                       |
| `:!`      | `:neq` | not equal to                   |
| `:>`      | `:gt`  | greater than                   |
| `:>=`     | `:gte` | greater than or equal to       |
| `:<`      | `:lt`  | less than                      |
| `:<=`     | `:lte` | less than or equal to          |
| `:in`     |        | contains                       |
| `:like`   |        | pattern match                  |
| `:ilike`  |        | case-insensitive pattern match |

**8. Value Transformation Directive**

Examples: `:lower`, `:upper`, `:all`, `:any`, `:datetime`, `:date`, `:+`, `:-`, `:*`, `:/`

Changes the value before it is compared. String transformations (`:lower`, `:upper`), set comparisons (`:all`, `:any`), date/time operations (`:datetime`, `:date`), and arithmetic operator directives (`:+`, `:-`, `:*`, `:/`).

**9. Value**

The actual value used in the comparison (for example `1`, `"hello"`, `[1, 2, 3]`, `true`, `false`, `nil`, or a subquery filter).

### Slot Guidelines

- Slots are optional. Not every filter uses every slot. You only include the slots you need.

- Slots must appear in order. Do not skip ahead and come back. For example, Do not put `:not` after the field name:

    # BAD EXAMPLE, DO NOT COPY THIS!
    [id: [not: [==: 1]]]

    # GOOD EXAMPLE, COPY THIS!
    [not: [id: [==: 1]]]

- The `:not` directive wraps everything after it. When `:not` is used it negates the entire filter that follows.

- String transformation directives (`:lower`, `:upper`) behavior depends on context:
  - **Slot 3** (before field): Transforms the field itself -> `[lower: [title: [==: "hello"]]]` produces `lower(title) == "hello"`
  - **Slot 8** (after operator): Transforms the value -> `[title: [==: [lower: "hello"]]]` produces `title == lower("hello")`

- Logical operator directives (`:and`, `:or`) behavior depends on context:
  - **Top-Level (Slot 1)**: Wraps multiple field groups in a list: `[and: [[title: "hello"], [published: true]]]`
  - **Top-Level with enumerable syntax**: Acts as top-level when appearing as an enumerable: `[and: [title: "hello", views: [>: 10]]]`
  - **Field-Level (Slot 6)**: Implicit when multiple operations target the same field: `[views: [>: 10, <: 20]]`

### Examples

**Example 1: Simple comparison**

    [views: [>: 10]]

Find records where the views field is greater than 10.

How the parser interprets it:

  - Slot 4: views is the field name
  - Slot 7: > is the comparison operator directive (greater than)
  - Slot 9: 10 is the value to compare against

SQL condition:

    views > 10

**Example 2: Negating a comparison**

    [not: [views: [>: 10]]]

Find records where the `views` field is NOT greater than `10`.

How the parser interprets it:

  - Slot 2: `:not` negates the entire filter
  - Slot 4: `views` is the field name
  - Slot 7: `>` is the comparison operator directive (greater than)
  - Slot 9: `10` is the value to compare against

SQL condition:

    not (views > 10)

**Example 3: Transforming the field before comparison**

    [lower: [title: [==: "hello"]]]

Find records where the `title` field, converted to lowercase, equals `"hello"`.

How the parser interprets it:

  - Slot 3: `:lower` transforms the field to lowercase
  - Slot 4: `:title` is the field name
  - Slot 7: `:==` is the comparison operator directive (equality)
  - Slot 9: `"hello"` is the value to compare against

SQL condition:

    lower(title) == "hello"

**Example 4: Transforming the value before comparison**

    [title: [==: [lower: "hello"]]]

Find records where the `title` field equals the lowercase version of `"hello"`.

How the parser interprets it:

  - Slot 4: `:title` is the field name
  - Slot 7: `:==` is the comparison operator directive (equality)
  - Slot 8: `:lower` transforms the value to lowercase
  - Slot 9: `"hello"` is the value to compare against

SQL condition:

    title == lower("hello")

**Example 5: Negating a field transformation**

    [not: [lower: [title: [==: "hello"]]]]

Find records where the `:title` field, converted to lowercase, does NOT equal `"hello"`.

How the parser interprets it:

  - Slot 2: `:not` negates the entire filter
  - Slot 3: `:lower` transforms the field to lowercase
  - Slot 4: `:title` is the field name
  - Slot 7: `:==` is the comparison operator directive (equality)
  - Slot 9: `"hello"` is the value to compare against

SQL condition:

    not (lower(title) == "hello")

**Example 6: Combining filters with OR and negation**

    [or: [[not: [views: [>: 10]]], [published: true]]]

Find records where EITHER the `:views` field is NOT greater than `10`, OR the `published` field is `true`.

How the parser interprets it:

  - Slot 1: `:or` combines multiple filter groups
  - Slot 2: `:not` negates the first filter
  - Slot 4: `views` is the field name (first group), `published` is the field name (second group)
  - Slot 7: `>` is the comparison operator directive (first group), equality is implicit (second group)
  - Slot 9: `10` is the value (first group), `true` is the value (second group)

SQL condition:

    not (views > 10) or (published == true)

**Example 7: Multiple conditions on the same field**

    [and: [views: [>: 10, <: 20]]]

Find records where the `:views` field is greater than `10` AND less than `20`.

How the parser interprets it:

  - Slot 1: `:and` is a top-level logical operator directive
  - Slot 4: `views` is the field name
  - Slot 7: `>` and `<` are the comparison operator directives
  - Slot 9: `10` and `20` are the values to compare against
  - Note: Multiple operations on the same field have implicit AND between them

SQL condition:

    views > 10 and views < 20

**Example 8: Using an aggregate function**

    [views: [avg: [>: 10]]]

Find records where the average of the `views` field is greater than `10`.

How the parser interprets it:

  - Slot 4: `views` is the field name
  - Slot 5: `:avg` aggregates the field
  - Slot 7: `>` is the comparison operator directive (greater than)
  - Slot 9: `10` is the value to compare against

SQL condition:

    avg(views) > 10

**Example 9: Field-level implicit AND**

    [views: [>: 10, <: 20]]

Find records where the `views` field is greater than `10` AND less than `20`.

How the parser interprets it:

  - Slot 4: `views` is the field name
  - Slot 6: Implicit `:and` combines multiple operations on the same field
  - Slot 7: `>` and `<` are the comparison operator directives
  - Slot 9: `10` and `20` are the values to compare against

SQL condition:

    views > 10 and views < 20

### Invalid Examples

These patterns violate the slot ordering rules and will not work:

**Invalid Example 1: Negation after field**

    [views: [not: [>: 10]]]

This attempts to negate a comparison on the views field, but places :not after the field name.

Why it fails:

- Slot 4: `:views` is the field name
  - Slot 2: `:not` attempts to negate, but Slot 2 must come before Slot 4
  - Violation: `:not` (Slot 2) must come before field (Slot 4)

Correct version:

    [not: [views: [>: 10]]]

**Invalid Example 2: Directive before field**

    [>: [views: 10]]

This attempts to use the `>` directive before specifying which field to compare.

Why it fails:

  - Slot 7: `:>` is the comparison operator directive
  - Slot 4: `:views` attempts to specify the field name, but Slot 4 must come before Slot 7
  - Violation: Directive (Slot 7) must come after field (Slot 4)

Correct version:

    [views: [>: 10]]

**Invalid Example 3: Field after directive**

    [==: [views: 10]]

This attempts to use the `:==` directive before specifying which field to compare.

Why it fails:

  - Slot 7: `:==` is the comparison operator directive
  - Slot 4: `:views` attempts to specify the field name, but Slot 4 must come before Slot 7
  - Violation: Field (Slot 4) must come before directive (Slot 7)

Correct version:

    [views: [==: 10]]

---

## Array Fields

Array fields (e.g., `{:array, :string}` in Ecto schemas) support special comparison and containment operations.

- `:in`: Check if value is contained in array (for single values)
- `:all`: Apply operation to all array elements
- `:count`: Count array elements and compare
- Comparison operators work element-wise
- String matching (like, ilike) works on array elements
- Transformations (lower, upper) work on array elements

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
    [tags: [like: ["elixir", "erlang"]]]
    [not: [tags: [like: "elixir"]]]
    [tags: [==: [lower: "elixir"]]]
    [tags: [!=: [upper: "ELIXIR"]]]
    [tags: [count: [>: 0]]]
    [tags: [count: [==: 0]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[tags: "elixir"]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` contains the value "elixir"
**And** the resulting expression is: `"elixir" in p.tags`

_Test reference: test/examples/ecto_query_dsl.exs:16_

**Rule Statement 2:**

**Given** filter params: `[tags: [==: "elixir"]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` contains the value "elixir" using equality
**And** the resulting expression is: `"elixir" in p.tags`

_Test reference: test/examples/ecto_query_dsl.exs:29_

**Rule Statement 3:**

**Given** filter params: `[tags: [!=: "elixir"]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` does not contain the value "elixir"
**And** the resulting expression is: `not ("elixir" in p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:42_

**Rule Statement 4:**

**Given** filter params: `[tags: [in: "elixir"]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` contains the value "elixir" using the `:in` operator
**And** the resulting expression is: `"elixir" in p.tags`

_Test reference: test/examples/ecto_query_dsl.exs:55_

**Rule Statement 5:**

**Given** filter params: `[tags: ["elixir", "erlang"]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` equals the list `["elixir", "erlang"]`
**And** the resulting expression is: `p.tags == ["elixir", "erlang"]`

_Test reference: test/examples/ecto_query_dsl.exs:68_

**Rule Statement 6:**

**Given** filter params: `[tags: [==: ["elixir", "erlang"]]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` equals the list `["elixir", "erlang"]`
**And** the resulting expression is: `p.tags == ["elixir", "erlang"]`

_Test reference: test/examples/ecto_query_dsl.exs:81_

**Rule Statement 7:**

**Given** filter params: `[tags: [!=: ["elixir"]]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` does not equal the list `["elixir"]`
**And** the resulting expression is: `p.tags != ["elixir"]`

_Test reference: test/examples/ecto_query_dsl.exs:94_

**Rule Statement 8:**

**Given** filter params: `[tags: nil]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` is nil
**And** the resulting expression is: `is_nil(p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:107_

**Rule Statement 9:**

**Given** filter params: `[tags: [==: nil]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` is nil using equality
**And** the resulting expression is: `is_nil(p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:120_

**Rule Statement 10:**

**Given** filter params: `[tags: [!=: nil]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` is not nil
**And** the resulting expression is: `not is_nil(p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:133_

**Rule Statement 11:**

**Given** filter params: `[tags: [in: ["elixir"]]]`
**When** the filter params are converted into a query expression
**Then** it must check if any element in the array `:tags` is in the list `["elixir"]`
**And** the resulting expression is: `fragment("? && ?", p.tags, ["elixir"])`

_Test reference: test/examples/ecto_query_dsl.exs:146_

**Rule Statement 12:**

**Given** filter params: `[tags: [all: [in: ["elixir", "erlang"]]]]`
**When** the filter params are converted into a query expression
**Then** it must check if all elements in the array `:tags` are in the list `["elixir", "erlang"]`
**And** the resulting expression is: `fragment("? <@ ?", p.tags, ^["elixir", "erlang"])`

_Test reference: test/examples/ecto_query_dsl.exs:159_

**Rule Statement 13:**

**Given** filter params: `[not: [tags: [==: ["elixir"]]]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` does not equal the list `["elixir"]`
**And** it must negate the comparison
**And** the resulting expression is: `not (p.tags == ^["elixir"])`

_Test reference: test/examples/ecto_query_dsl.exs:172_

**Rule Statement 14:**

**Given** filter params: `[not: [tags: [in: "elixir"]]]`
**When** the filter params are converted into a query expression
**Then** it must check if the array `:tags` does not contain the value "elixir"
**And** it must negate the comparison
**And** the resulting expression is: `not ("elixir" in p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:185_

**Rule Statement 15:**

**Given** filter params: `[tags: [>: "elixir"]]`
**When** the filter params are converted into a query expression
**Then** it must check if any element in the array `:tags` is greater than "elixir"
**And** the resulting expression is: `fragment("? > ANY(?)", ^"elixir", p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:198_

**Rule Statement 16:**

**Given** filter params: `[tags: [>=: "elixir"]]`
**When** the filter params are converted into a query expression
**Then** it must check if any element in the array `:tags` is greater than or equal to "elixir"
**And** the resulting expression is: `fragment("? >= ANY(?)", ^"elixir", p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:211_

**Rule Statement 17:**

**Given** filter params: `[tags: [<: "elixir"]]`
**When** the filter params are converted into a query expression
**Then** it must check if any element in the array `:tags` is less than "elixir"
**And** the resulting expression is: `fragment("? < ANY(?)", ^"elixir", p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:224_

**Rule Statement 18:**

**Given** filter params: `[tags: [<=: "elixir"]]`
**When** the filter params are converted into a query expression
**Then** it must check if any element in the array `:tags` is less than or equal to "elixir"
**And** the resulting expression is: `fragment("? <= ANY(?)", ^"elixir", p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:237_

**Rule Statement 19:**

**Given** filter params: `[tags: [like: "elixir"]]`
**When** the filter params are converted into a query expression
**Then** it must check if any element in the array `:tags` matches the pattern "elixir"
**And** the resulting expression is: `fragment("? LIKE ANY(?)", ^"elixir", p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:250_

**Rule Statement 20:**

**Given** filter params: `[tags: [ilike: "elixir"]]`
**When** the filter params are converted into a query expression
**Then** it must check if any element in the array `:tags` matches the pattern "elixir" case-insensitively
**And** the resulting expression is: `fragment("? ILIKE ANY(?)", ^"elixir", p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:263_

**Rule Statement 21:**

**Given** filter params: `[tags: [like: ["elixir", "erlang"]]]`
**When** the filter params are converted into a query expression
**Then** it must check if any element in the array `:tags` matches any of the patterns `["elixir", "erlang"]`
**And** the resulting expression is: `fragment("? && ?", p.tags, ^["elixir", "erlang"])`

_Test reference: test/examples/ecto_query_dsl.exs:276_

**Rule Statement 22:**

**Given** filter params: `[not: [tags: [like: "elixir"]]]`
**When** the filter params are converted into a query expression
**Then** it must check if no element in the array `:tags` matches the pattern "elixir"
**And** it must negate the comparison
**And** the resulting expression is: `not fragment("? LIKE ANY(?)", ^"elixir", p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:289_

**Rule Statement 23:**

**Given** filter params: `[tags: [==: [lower: "elixir"]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `lower/1` to the comparison value "elixir"
**And** it must check if the array `:tags` contains the lowercased value
**And** the resulting expression is: `fragment("lower(?)", "elixir") in p.tags`

_Test reference: test/examples/ecto_query_dsl.exs:302_

**Rule Statement 24:**

**Given** filter params: `[tags: [!=: [upper: "ELIXIR"]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `upper/1` to the comparison value "ELIXIR"
**And** it must check if the array `:tags` does not contain the uppercased value
**And** the resulting expression is: `not (fragment("upper(?)", "ELIXIR") in p.tags)`

_Test reference: test/examples/ecto_query_dsl.exs:315_

**Rule Statement 25:**

**Given** filter params: `[tags: [count: [>: 0]]]`
**When** the filter params are converted into a query expression
**Then** it must count the elements in the array `:tags`
**And** it must compare the count to `0` using `>`
**And** the resulting expression is: `fragment("array_length(?, 1)", p.tags) > 0`

_Test reference: test/examples/ecto_query_dsl.exs:328_

**Rule Statement 26:**

**Given** filter params: `[tags: [count: [==: 0]]]`
**When** the filter params are converted into a query expression
**Then** it must count the elements in the array `:tags`
**And** it must compare the count to `0` using `==`
**And** the resulting expression is: `fragment("coalesce(array_length(?, 1), 0)", p.tags) == 0`

_Test reference: test/examples/ecto_query_dsl.exs:341_

Note: Array fields support all comparison operators, string matching (like/ilike), transformations (lower/upper), and the special `:all` directive for universal quantification. The `:in` operator checks containment, while `:count` operates on array length.

---

## Scalar Fields

A scalar field is a field on a given schema or table. It can be used in filter conditions to compare values.

### Examples

    [id: 1]
    [published: true]
    [id: 1, published: true]
    [published_at: nil]
    [published: [true, false]]
    [title: "hello"]
    [and: [[id: 1]]]
    [and: [[id: 1], [published: true]]]
    [and: [id: 1, title: "hello"]]
    [and: [[id: 1, title: "hello"], [published: true]]]
    [or: [[id: 1, title: "hello"], [published: true]]]
    [or: [[id: 1, or: [title: "hello", body: "world"]], [published: true]]]

### Rule Statements

**Rule Statement 1:**
 
**Given** filter params: `[id: 1]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:id` field to the value `1` using equality
**And** the resulting expression is: `p.id == 1`

_Test reference: test/examples/ecto_query_dsl.exs:356_

**Rule Statement 2:**
 
**Given** filter params: `[published: true]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:published` field to the value `true` using equality
**And** the resulting expression is: `p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:369_

**Rule Statement 3:**
 
**Given** filter params: `[id: 1, published: true]`
**When** the filter params are converted into a query condition
**Then** it must combine multiple field comparisons with an implicit AND
**And** it must compare the `:id` field to `1` and the `:published` field to `true`
**And** the resulting expression is: `p.id == 1 and p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:382_

**Rule Statement 4:**
 
**Given** filter params: `[published_at: nil]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:published_at` field to `nil` using equality
**And** the resulting expression is: `is_nil(p.published_at)`

_Test reference: test/examples/ecto_query_dsl.exs:395_

**Rule Statement 5:**
 
**Given** filter params: `[published: [true, false]]`
**When** the filter params are converted into a query condition
**Then** it must check if the `:published` field is in the list `[true, false]`
**And** the resulting expression is: `p.published in [true, false]`

_Test reference: test/examples/ecto_query_dsl.exs:408_

**Rule Statement 6:**
 
**Given** filter params: `[title: "hello"]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:title` field to the value `"hello"` using equality
**And** the resulting expression is: `p.title == "hello"`

_Test reference: test/examples/ecto_query_dsl.exs:422_

**Rule Statement 7:**
 
**Given** filter params: `[and: [[id: 1]]]`
**When** the filter params are converted into a query condition
**Then** it must apply an AND logical operator directive to the nested filter
**And** it must compare the `:id` field to the value `1` using equality
**And** the resulting expression is: `p.id == 1`

_Test reference: test/examples/ecto_query_dsl.exs:435_
 
**Rule Statement 8:**
 
**Given** filter params: `[and: [[id: 1], [published: true]]]`
**When** the filter params are converted into a query condition
**Then** it must combine the nested filters with the `AND` directive
**And** it must produce one group for `[id: 1]` and one group for `[published: true]`
**And** it must compare `:id` to `1` and `:published` to `true`
**And** the resulting expression is: `p.id == 1 and p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:448_

**Rule Statement 9:**
 
**Given** filter params: `[and: [id: 1, title: "hello"]]`
**When** the filter params are converted into a query condition
**Then** it must apply an AND logical operator directive to the nested filter
**And** it must compare the `:id` field to the value `1` using equality
**And** it must compare the `:title` field to the value `"hello"` using equality
**And** the resulting expression is: `p.id == 1 and p.title == "hello"`

_Test reference: test/examples/ecto_query_dsl.exs:461_

**Rule Statement 10:**

**Given** filter params: `[and: [[id: 1, title: "hello"], [published: true]]]`
**When** the filter params are converted into a query condition
**Then** it must combine the nested filters with the `AND` directive
**And** it must produce one group for `[id: 1, title: "hello"]` and one group for `[published: true]`
**And** it must compare `:id` to `1`, `:title` to `"hello"`, and `:published` to `true`
**And** the resulting expression is: `(p.id == 1 and p.title == "hello") and p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:474_

**Rule Statement 11:**
 
**Given** filter params: `[or: [[id: 1, title: "hello"], [published: true]]]`
**When** the filter params are converted into a query condition
**Then** it must combine the nested filters with the `OR` directive
**And** it must produce one group for `[id: 1, title: "hello"]` and one group for `[published: true]`
**And** it must compare `:id` to `1`, `:title` to `"hello"`, and `:published` to `true`
**And** the resulting expression is: `(p.id == 1 and p.title == "hello") or p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:487_
 
**Rule Statement 12:**
 
**Given** filter params: `[or: [[id: 1, or: [title: "hello", body: "world"]], [published: true]]]`
**When** the filter params are converted into a query condition
**Then** it must combine the top-level filters with the `OR` directive
**And** it must produce one group for `[id: 1, or: [title: "hello", body: "world"]]` and one group for `[published: true]`
**And** it must apply a nested `OR` directive to `:title` and `:body` within the first group
**And** it must compare `:id` to `1`, `:title` to `"hello"`, `:body` to `"world"`, and `:published` to `true`
**And** the resulting expression is: `(p.id == 1 and (p.title == "hello" or p.body == "world")) or p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:502_

---

## Negation Directives

- `:not`: Wraps an entire operation in a logical NOT. Whatever condition the nested operation produces, `:not` flips it (`true` becomes `false`, and `false` becomes `true`).

### Examples

    [not: [published: [in: [true, false]]]]
    [not: [published: [==: [true, false]]]]
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

**Given** filter params: `[not: [published: [in: [true, false]]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the IN comparison
**And** it must check if the `:published` field is not in the list `[true, false]`, accounting for NULL
**And** the resulting expression is: `is_nil(p.published) or p.published not in [true, false]`

_Test reference: test/examples/ecto_query_dsl.exs:520_

**Rule Statement 2:**

**Given** filter params: `[not: [published: [==: [true, false]]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the equality comparison on a list value, coercing it to a NOT IN check
**And** it must check if the `:published` field is not in the list `[true, false]`, accounting for NULL
**And** the resulting expression is: `is_nil(p.published) or p.published not in [true, false]`

_Test reference: test/examples/ecto_query_dsl.exs:535_

**Rule Statement 3:**

**Given** filter params: `[not: [published: [!=: [true, false]]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the inequality comparison on a list value, coercing it to an IN check
**And** it must check if the `:published` field is in the list `[true, false]`
**And** the resulting expression is: `p.published in [true, false]`

_Test reference: test/examples/ecto_query_dsl.exs:549_

**Rule Statement 4:**

**Given** filter params: `[not: [views: [>: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the greater-than comparison
**And** it must check if the `:views` field is not greater than `10`
**And** the resulting expression is: `not (p.views > 10)`

_Test reference: test/examples/ecto_query_dsl.exs:564_

**Rule Statement 5:**

**Given** filter params: `[not: [views: [>=: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the greater-than-or-equal comparison
**And** it must check if the `:views` field is not greater than or equal to `10`
**And** the resulting expression is: `not (p.views >= 10)`

_Test reference: test/examples/ecto_query_dsl.exs:577_

**Rule Statement 6:**

**Given** filter params: `[not: [views: [<: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the less-than comparison
**And** it must check if the `:views` field is not less than `10`
**And** the resulting expression is: `not (p.views < 10)`

_Test reference: test/examples/ecto_query_dsl.exs:590_

**Rule Statement 7:**

**Given** filter params: `[not: [views: [<=: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the less-than-or-equal comparison
**And** it must check if the `:views` field is not less than or equal to `10`
**And** the resulting expression is: `not (p.views <= 10)`

_Test reference: test/examples/ecto_query_dsl.exs:603_

**Rule Statement 8:**

**Given** filter params: `[not: [views: [==: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the equality comparison
**And** it must check if the `:views` field is not equal to `10`
**And** the resulting expression is: `not (p.views == 10)`

_Test reference: test/examples/ecto_query_dsl.exs:616_

**Rule Statement 9:**

**Given** filter params: `[not: [views: [!=: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the inequality comparison
**And** it must check if the `:views` field is not not-equal to `10`
**And** the resulting expression is: `not (p.views != 10)`

_Test reference: test/examples/ecto_query_dsl.exs:629_

**Rule Statement 10:**

**Given** filter params: `[not: [views: [gt: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the greater-than comparison
**And** it must check if the `:views` field is not greater than `10`
**And** the resulting expression is: `not (p.views > 10)`

_Test reference: test/examples/ecto_query_dsl.exs:642_

**Rule Statement 11:**

**Given** filter params: `[not: [views: [gte: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the greater-than-or-equal comparison
**And** it must check if the `:views` field is not greater than or equal to `10`
**And** the resulting expression is: `not (p.views >= 10)`

_Test reference: test/examples/ecto_query_dsl.exs:655_

**Rule Statement 12:**

**Given** filter params: `[not: [views: [lt: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the less-than comparison
**And** it must check if the `:views` field is not less than `10`
**And** the resulting expression is: `not (p.views < 10)`

_Test reference: test/examples/ecto_query_dsl.exs:668_

**Rule Statement 13:**

**Given** filter params: `[not: [views: [lte: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the less-than-or-equal comparison
**And** it must check if the `:views` field is not less than or equal to `10`
**And** the resulting expression is: `not (p.views <= 10)`

_Test reference: test/examples/ecto_query_dsl.exs:681_

---

## Logical Operator Directives

- `:and`: field level and top level
- `:or`: field level and top level

### Examples

    [and: [title: "hello", views: [>: 10, <: 20]]]
    [and: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]
    [or: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]
    [or: [[or: [title: "hello", views: [>: 10, <: 20]]], [published: true]]]
    [or: [[title: "hello", or: [views: [>: 10, <: 20]]], [published: true]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[and: [title: "hello", views: [>: 10, <: 20]]]`
**When** the filter params are converted into a query condition
**Then** it must apply top-level AND to combine the field conditions
**And** it must apply implicit AND between multiple operations on the `views` field
**And** the resulting expression is: `p.title == "hello" and (p.views > 10 and p.views < 20)`

_Test reference: test/examples/ecto_query_dsl.exs:696_

**Rule Statement 2:**

**Given** filter params: `[and: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]`
**When** the filter params are converted into a query condition
**Then** it must apply top-level AND with list syntax to combine filter groups
**And** it must apply implicit AND between multiple operations on the `views` field
**And** the resulting expression is: `(p.title == "hello" and (p.views > 10 and p.views < 20)) and p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:710_

**Rule Statement 3:**

**Given** filter params: `[or: [[title: "hello", views: [>: 10, <: 20]], [published: true]]]`
**When** the filter params are converted into a query condition
**Then** it must apply top-level OR to combine filter groups
**And** it must apply implicit AND between multiple operations on the `views` field
**And** the resulting expression is: `(p.title == "hello" and (p.views > 10 and p.views < 20)) or p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:724_

**Rule Statement 4:**

**Given** filter params: `[or: [[or: [title: "hello", views: [>: 10, <: 20]]], [published: true]]]`
**When** the filter params are converted into a query condition
**Then** it must apply top-level OR to combine filter groups
**And** it must apply nested OR to the first group, changing the inner combinator
**And** it must apply implicit AND between multiple operations on the `views` field
**And** the resulting expression is: `(p.title == "hello" or (p.views > 10 and p.views < 20)) or p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:739_

**Rule Statement 5:**

**Given** filter params: `[or: [[title: "hello", or: [views: [>: 10, <: 20]]], [published: true]]]`
**When** the filter params are converted into a query condition
**Then** it must apply top-level OR to combine filter groups
**And** it must apply nested OR within the first group
**And** it must apply implicit AND between multiple operations on the `views` field
**And** the resulting expression is: `(p.title == "hello" or (p.views > 10 and p.views < 20)) or p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:755_

---

## Comparison Operator Directives

- `:==`: equality
- `:eq`: equality alias
- `:!=`: inequality
- `:> `: greater than
- `:>=`: greater than or equal
- `:< `: less than
- `:<=`: less than or equal
- `:gt`: greater than alias
- `:gte`: greater than or equal alias
- `:lt`: less than alias
- `:lte`: less than or equal alias
- `:in`: membership

### Examples

    [id: [==: 1]]
    [id: [eq: 1]]
    [published_at: [==: nil]]
    [published_at: [eq: nil]]
    [published_at: [!=: nil]]
    [views: [>: 10]]
    [views: [>=: 10]]
    [views: [<: 10]]
    [views: [<=: 10]]
    [views: [!=: 10]]
    [views: [gt: 10]]
    [views: [gte: 10]]
    [views: [lt: 10]]
    [views: [lte: 10]]
    [published: [in: [true, false]]]
    [published: [==: [true, false]]]
    [published: [!=: [true, false]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[id: [==: 1]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:id` field to the value `1` using equality
**And** the resulting expression is: `p.id == 1`

_Test reference: test/examples/ecto_query_dsl.exs:773_

**Rule Statement 2:**

**Given** filter params: `[id: [eq: 1]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:id` field to the value `1` using the equality alias
**And** the resulting expression is: `p.id == 1`

_Test reference: test/examples/ecto_query_dsl.exs:786_

**Rule Statement 3:**

**Given** filter params: `[published_at: [==: nil]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:published_at` field to `nil` using equality
**And** the resulting expression is: `is_nil(p.published_at)`

_Test reference: test/examples/ecto_query_dsl.exs:799_

**Rule Statement 4:**

**Given** filter params: `[published_at: [eq: nil]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:published_at` field to `nil` using the equality alias
**And** the resulting expression is: `is_nil(p.published_at)`

_Test reference: test/examples/ecto_query_dsl.exs:812_

**Rule Statement 5:**

**Given** filter params: `[published_at: [!=: nil]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:published_at` field to `nil` using inequality
**And** the resulting expression is: `not is_nil(p.published_at)`

_Test reference: test/examples/ecto_query_dsl.exs:825_

**Rule Statement 6:**

**Given** filter params: `[views: [>: 10]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:views` field to `10` using greater than
**And** the resulting expression is: `p.views > 10`

_Test reference: test/examples/ecto_query_dsl.exs:838_

**Rule Statement 7:**

**Given** filter params: `[views: [>=: 10]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:views` field to `10` using greater than or equal
**And** the resulting expression is: `p.views >= 10`

_Test reference: test/examples/ecto_query_dsl.exs:851_

**Rule Statement 8:**

**Given** filter params: `[views: [<: 10]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:views` field to `10` using less than
**And** the resulting expression is: `p.views < 10`

_Test reference: test/examples/ecto_query_dsl.exs:864_

**Rule Statement 9:**

**Given** filter params: `[views: [<=: 10]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:views` field to `10` using less than or equal
**And** the resulting expression is: `p.views <= 10`

_Test reference: test/examples/ecto_query_dsl.exs:877_

**Rule Statement 10:**

**Given** filter params: `[views: [!=: 10]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:views` field to `10` using inequality
**And** the resulting expression is: `p.views != 10`

_Test reference: test/examples/ecto_query_dsl.exs:890_

**Rule Statement 11:**

**Given** filter params: `[views: [gt: 10]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:views` field to `10` using the greater than alias
**And** the resulting expression is: `p.views > 10`

_Test reference: test/examples/ecto_query_dsl.exs:903_

**Rule Statement 12:**

**Given** filter params: `[views: [gte: 10]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:views` field to `10` using the greater than or equal alias
**And** the resulting expression is: `p.views >= 10`

_Test reference: test/examples/ecto_query_dsl.exs:916_

**Rule Statement 13:**

**Given** filter params: `[views: [lt: 10]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:views` field to `10` using the less than alias
**And** the resulting expression is: `p.views < 10`

_Test reference: test/examples/ecto_query_dsl.exs:929_

**Rule Statement 14:**

**Given** filter params: `[views: [lte: 10]]`
**When** the filter params are converted into a query condition
**Then** it must compare the `:views` field to `10` using the less than or equal alias
**And** the resulting expression is: `p.views <= 10`

_Test reference: test/examples/ecto_query_dsl.exs:942_

**Rule Statement 15:**

**Given** filter params: `[published: [in: [true, false]]]`
**When** the filter params are converted into a query condition
**Then** it must check if the `:published` field is in the list `[true, false]`
**And** the resulting expression is: `p.published in [true, false]`

_Test reference: test/examples/ecto_query_dsl.exs:955_

**Rule Statement 16:**

**Given** filter params: `[published: [==: [true, false]]]`
**When** the filter params are converted into a query condition
**Then** it must coerce equality on a list value to an IN check
**And** the resulting expression is: `p.published in [true, false]`

_Test reference: test/examples/ecto_query_dsl.exs:970_

**Rule Statement 17:**

**Given** filter params: `[published: [!=: [true, false]]]`
**When** the filter params are converted into a query condition
**Then** it must coerce inequality on a list value to a NOT IN check, accounting for NULL
**And** the resulting expression is: `is_nil(p.published) or p.published not in [true, false]`

_Test reference: test/examples/ecto_query_dsl.exs:985_

---

## String Matching Directives

- `:like`: pattern matching
- `:ilike`: case-insensitive pattern matching

### Examples

    [title: [like: "hello"]]
    [title: [ilike: "hello"]]
    [title: [like: ["hello", "world"]]]
    [title: [ilike: ["hello", "world"]]]
    [not: [title: [like: "hello"]]]
    [not: [title: [ilike: "hello"]]]
    [not: [title: [like: ["hello", "world"]]]]
    [not: [title: [ilike: ["hello", "world"]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[title: [like: "%hello%"]]`
**When** the filter params are converted into a query condition
**Then** it must apply pattern matching to the `:title` field with the value `"%hello%"`
**And** the resulting expression is: `like(p.title, "%hello%")`

_Test reference: test/examples/ecto_query_dsl.exs:1002_

**Rule Statement 2:**

**Given** filter params: `[not: [title: [like: "%hello%"]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the pattern matching comparison
**And** the resulting expression is: `not like(p.title, "%hello%")`

_Test reference: test/examples/ecto_query_dsl.exs:1015_

**Rule Statement 3:**

**Given** filter params: `[title: [ilike: "%HELLO%"]]`
**When** the filter params are converted into a query condition
**Then** it must apply case-insensitive pattern matching to the `:title` field with the value `"%HELLO%"`
**And** the resulting expression is: `ilike(p.title, "%HELLO%")`

_Test reference: test/examples/ecto_query_dsl.exs:1028_

**Rule Statement 4:**

**Given** filter params: `[not: [title: [ilike: "%HELLO%"]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the case-insensitive pattern matching comparison
**And** the resulting expression is: `not ilike(p.title, "%HELLO%")`

_Test reference: test/examples/ecto_query_dsl.exs:1041_

**Rule Statement 5:**

**Given** filter params: `[title: [like: ["%hello%", "%world%"]]]`
**When** the filter params are converted into a query condition
**Then** it must apply pattern matching to the `:title` field with multiple values `["%hello%", "%world%"]`
**And** the resulting expression is: `like(p.title, "%hello%") or like(p.title, "%world%")`

_Test reference: test/examples/ecto_query_dsl.exs:1054_

**Rule Statement 6:**

**Given** filter params: `[title: [ilike: ["%HELLO%", "%WORLD%"]]]`
**When** the filter params are converted into a query condition
**Then** it must apply case-insensitive pattern matching to the `:title` field with multiple values `["%HELLO%", "%WORLD%"]`
**And** the resulting expression is: `ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%")`

_Test reference: test/examples/ecto_query_dsl.exs:1068_

**Rule Statement 7:**

**Given** filter params: `[not: [title: [like: ["%hello%", "%world%"]]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the pattern matching comparison with multiple values
**And** the resulting expression is: `not (like(p.title, "%hello%") or like(p.title, "%world%"))`

_Test reference: test/examples/ecto_query_dsl.exs:1082_

**Rule Statement 8:**

**Given** filter params: `[not: [title: [ilike: ["%HELLO%", "%WORLD%"]]]]`
**When** the filter params are converted into a query condition
**Then** it must negate the case-insensitive pattern matching comparison with multiple values
**And** the resulting expression is: `not (ilike(p.title, "%HELLO%") or ilike(p.title, "%WORLD%"))`

_Test reference: test/examples/ecto_query_dsl.exs:1096_

---

## String Transformation Directives

- `:lower`: convert to lowercase
- `:upper`: convert to uppercase

### Examples

    [title: [==: [lower: "hello"]]]
    [title: [==: [upper: "HELLO"]]]
    [title: [!=: [lower: "hello"]]]
    [title: [!=: [upper: "HELLO"]]]
    [not: [title: [==: [lower: "hello"]]]]
    [not: [title: [==: [upper: "HELLO"]]]]
    [title: [==: [lower: "HELLO"]]]
    [title: [==: [upper: "hello"]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[title: [==: [lower: "hello"]]]`
**When** the filter params are converted into a query condition
**Then** it must apply `lower/1` to the comparison value "hello"
**And** it must compare the `:title` field to the result using equality
**And** the resulting expression is: `p.title == fragment("lower(?)", "hello")`

_Test reference: test/examples/ecto_query_dsl.exs:1112_

**Rule Statement 2:**

**Given** filter params: `[title: [==: [upper: "HELLO"]]]`
**When** the filter params are converted into a query condition
**Then** it must apply `upper/1` to the comparison value "HELLO"
**And** it must compare the `:title` field to the result using equality
**And** the resulting expression is: `p.title == fragment("upper(?)", "HELLO")`

_Test reference: test/examples/ecto_query_dsl.exs:1125_

**Rule Statement 3:**

**Given** filter params: `[title: [!=: [lower: "hello"]]]`
**When** the filter params are converted into a query condition
**Then** it must apply `lower/1` to the comparison value "hello"
**And** it must compare the `:title` field to the result using inequality
**And** the resulting expression is: `p.title != fragment("lower(?)", "hello")`

_Test reference: test/examples/ecto_query_dsl.exs:1138_

**Rule Statement 4:**

**Given** filter params: `[title: [!=: [upper: "HELLO"]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `upper/1` to the comparison value "HELLO"
**And** it must compare the `:title` field to the result using inequality
**And** the resulting expression is: `p.title != fragment("upper(?)", "HELLO")`

_Test reference: test/examples/ecto_query_dsl.exs:1151_

**Rule Statement 5:**

**Given** filter params: `[not: [title: [==: [lower: "hello"]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `lower/1` to the comparison value "hello"
**And** it must negate the equality comparison
**And** the resulting expression is: `not (p.title == fragment("lower(?)", "hello"))`

_Test reference: test/examples/ecto_query_dsl.exs:1164_

**Rule Statement 6:**

**Given** filter params: `[not: [title: [==: [upper: "HELLO"]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `upper/1` to the comparison value "HELLO"
**And** it must negate the equality comparison
**And** the resulting expression is: `not (p.title == fragment("upper(?)", "HELLO"))`

_Test reference: test/examples/ecto_query_dsl.exs:1177_

**Rule Statement 7:**

**Given** filter params: `[title: [==: [lower: "HELLO"]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `lower/1` to the comparison value "HELLO"
**And** it must compare the `:title` field to the result using equality
**And** the resulting expression is: `p.title == fragment("lower(?)", "HELLO")`

_Test reference: test/examples/ecto_query_dsl.exs:1190_

**Rule Statement 8:**

**Given** filter params: `[title: [==: [upper: "hello"]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `upper/1` to the comparison value "hello"
**And** it must compare the `:title` field to the result using equality
**And** the resulting expression is: `p.title == fragment("upper(?)", "hello")`

_Test reference: test/examples/ecto_query_dsl.exs:1203_

Note: String transformations at slot 8 (after the comparison operator) transform the value before comparison. Both `:lower` and `:upper` work with all comparison operators and with `:not`.

---

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
    [views: [count: [==: nil]]]
    [not: [views: [count: [>: 0]]]]
    [not: [views: [max: [>=: 100]]]]
    [views: [avg: [<=: 10]]]
    [views: [sum: [>: 500]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[views: [avg: [>: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must apply `avg/1` to the `:views` field
**And** it must compare the result to `10` using `>`
**And** the resulting expression is: `avg(p.views) > 10`

_Test reference: test/examples/ecto_query_dsl.exs:1218_

**Rule Statement 2:**

**Given** filter params: `[not: [views: [avg: [>: 10]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply `avg/1` to the `:views` field
**And** it must compare the result to `10` using `>`
**And** it must negate the entire comparison
**And** the resulting expression is: `not (avg(p.views) > 10)`

_Test reference: test/examples/ecto_query_dsl.exs:1231_

**Rule Statement 3:**

**Given** filter params: `[views: [count: [>: 0]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `count/1` to the `:views` field
**And** it must compare the result to `0` using `>`
**And** the resulting expression is: `count(p.views) > 0`

_Test reference: test/examples/ecto_query_dsl.exs:1244_

**Rule Statement 4:**

**Given** filter params: `[views: [max: [>=: 100]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `max/1` to the `:views` field
**And** it must compare the result to `100` using `>=`
**And** the resulting expression is: `max(p.views) >= 100`

_Test reference: test/examples/ecto_query_dsl.exs:1256_

**Rule Statement 5:**

**Given** filter params: `[views: [min: [<: 5]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `min/1` to the `:views` field
**And** it must compare the result to `5` using `<`
**And** the resulting expression is: `min(p.views) < 5`

_Test reference: test/examples/ecto_query_dsl.exs:1269_

**Rule Statement 6:**

**Given** filter params: `[views: [sum: [==: 1000]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `sum/1` to the `:views` field
**And** it must compare the result to `1000` using `==`
**And** the resulting expression is: `sum(p.views) == 1000`

_Test reference: test/examples/ecto_query_dsl.exs:1282_

**Rule Statement 7:**

**Given** filter params: `[views: [avg: [!=: 50]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `avg/1` to the `:views` field
**And** it must compare the result to `50` using `!=`
**And** the resulting expression is: `avg(p.views) != 50`

_Test reference: test/examples/ecto_query_dsl.exs:1295_

**Rule Statement 8:**

**Given** filter params: `[views: [count: [==: nil]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `count/1` to the `:views` field
**And** it must compare the result to `nil` using `==`
**And** the resulting expression is: `is_nil(count(p.views))`

_Test reference: test/examples/ecto_query_dsl.exs:1308_

**Rule Statement 9:**

**Given** filter params: `[not: [views: [count: [>: 0]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `count/1` to the `:views` field
**And** it must compare the result to `0` using `>`
**And** it must negate the entire comparison
**And** the resulting expression is: `not (count(p.views) > 0)`

_Test reference: test/examples/ecto_query_dsl.exs:1320_

**Rule Statement 10:**

**Given** filter params: `[not: [views: [max: [>=: 100]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `max/1` to the `:views` field
**And** it must compare the result to `100` using `>=`
**And** it must negate the entire comparison
**And** the resulting expression is: `not (max(p.views) >= 100)`

_Test reference: test/examples/ecto_query_dsl.exs:1335_

**Rule Statement 11:**

**Given** filter params: `[views: [avg: [<=: 10]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `avg/1` to the `:views` field
**And** it must compare the result to `10` using `<=`
**And** the resulting expression is: `avg(p.views) <= 10`

_Test reference: test/examples/ecto_query_dsl.exs:1350_

**Rule Statement 12:**

**Given** filter params: `[views: [sum: [>: 500]]]`
**When** the filter params are converted into a query expression
**Then** it must apply `sum/1` to the `:views` field
**And** it must compare the result to `500` using `>`
**And** the resulting expression is: `sum(p.views) > 500`

_Test reference: test/examples/ecto_query_dsl.exs:1363_

Note: All aggregate operators (avg, count, max, min, sum) work with all comparison operators (>, >=, <, <=, ==, !=, and their aliases). The pattern shown above applies universally across aggregates and comparisons.

---

## Set Comparison Directives

- `:all`: compare against all values in subquery
- `:any`: compare against any value in subquery

### Examples

    [id: [>: [all: subquery_expr]]]
    [not: [id: [>: [all: subquery_expr]]]]
    [id: [>: [any: subquery_expr]]]
    [not: [id: [>: [any: subquery_expr]]]]
    [id: [>=: [all: subquery_expr]]]
    [id: [<: [all: subquery_expr]]]
    [id: [<=: [all: subquery_expr]]]
    [id: [==: [all: subquery_expr]]]
    [id: [!=: [all: subquery_expr]]]
    [id: [all: subquery_expr]]
    [id: [all: [from: Post, id: 1]]]
    [not: [id: [all: subquery_expr]]]
    [id: [any: subquery_expr]]
    [not: [id: [any: subquery_expr]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[id: [>: [all: subquery_expr]]]`
**When** the filter params are converted into a query condition
**And** it must apply the `>` comparison to the `:id` field against all values returned by the subquery
**And** the resulting expression is: `p.id > all(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:1506_

**Rule Statement 2:**

**Given** filter params: `[not: [id: [>: [all: subquery_expr]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `>` comparison to the `:id` field against all values returned by the subquery
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.id > all(subquery_expr))`

_Test reference: test/examples/ecto_query_dsl.exs:1521_

**Rule Statement 3:**

**Given** filter params: `[id: [>: [any: subquery_expr]]]`
**When** the filter params are converted into a query condition
**And** it must apply the `>` comparison to the `:id` field against any value returned by the subquery
**And** the resulting expression is: `p.id > any(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:1535_

**Rule Statement 4:**

**Given** filter params: `[not: [id: [>: [any: subquery_expr]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `>` comparison to the `:id` field against any value returned by the subquery
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.id > any(subquery_expr))`

_Test reference: test/examples/ecto_query_dsl.exs:1549_

**Rule Statement 5:**

**Given** filter params: `[id: [>=: [all: subquery_expr]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `>=` comparison to the `:id` field against all values returned by the subquery
**And** the resulting expression is: `p.id >= all(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:1563_

**Rule Statement 6:**

**Given** filter params: `[id: [<: [all: subquery_expr]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `<` comparison to the `:id` field against all values returned by the subquery
**And** the resulting expression is: `p.id < all(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:1579_

**Rule Statement 7:**

**Given** filter params: `[id: [<=: [all: subquery_expr]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `<=` comparison to the `:id` field against all values returned by the subquery
**And** the resulting expression is: `p.id <= all(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:1593_

**Rule Statement 8:**

**Given** filter params: `[id: [==: [all: subquery_expr]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `==` comparison to the `:id` field against all values returned by the subquery
**And** the resulting expression is: `p.id == all(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:1609_

**Rule Statement 9:**

**Given** filter params: `[id: [!=: [all: subquery_expr]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `!=` comparison to the `:id` field against all values returned by the subquery
**And** the resulting expression is: `p.id != all(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:1623_

**Rule Statement 10:**

**Given** filter params: `[id: [all: subquery_expr]]`
**When** the filter params are converted into a query condition
**Then** it must apply the default comparison to the `:id` field against all values returned by the subquery
**And** the resulting expression is: `p.id == all(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:1638_

**Rule Statement 11:**

**Given** filter params: `[id: [all: [from: Post, id: 1]]]`
**When** the filter params are converted into a query condition
**Then** it must build a subquery from the filter params and apply the comparison to the `:id` field against all values
**And** the resulting expression is: `p.id == all(subquery(from p in Post, where: p.id == 1))`

_Test reference: test/examples/ecto_query_dsl.exs:1652_

**Rule Statement 12:**

**Given** filter params: `[not: [id: [all: subquery_expr]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the default comparison to the `:id` field against all values returned by the subquery
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.id == all(subquery_expr))`

_Test reference: test/examples/ecto_query_dsl.exs:1666_

**Rule Statement 13:**

**Given** filter params: `[id: [any: subquery_expr]]`
**When** the filter params are converted into a query condition
**And** it must apply the default comparison to the `:id` field against any value returned by the subquery
**And** the resulting expression is: `p.id == any(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:1680_

**Rule Statement 14:**

**Given** filter params: `[not: [id: [any: subquery_expr]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the default comparison to the `:id` field against any value returned by the subquery
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.id == any(subquery_expr))`

_Test reference: test/examples/ecto_query_dsl.exs:1694_

---

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

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[views: [>: [+: [:views, 10]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `+` directive to the `:views` field and `10`
**And** it must compare the `:views` field to the result using `>`
**And** the resulting expression is: `p.views > p.views + 10`

_Test reference: test/examples/ecto_query_dsl.exs:1378_

**Rule Statement 2:**

**Given** filter params: `[not: [views: [>: [+: [:views, 10]]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `+` directive to the `:views` field and `10`
**And** it must compare the `:views` field to the result using `>`
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.views > p.views + 10)`

_Test reference: test/examples/ecto_query_dsl.exs:1390_

**Rule Statement 3:**

**Given** filter params: `[views: [>=: [-: [:views, 5]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `-` directive to the `:views` field and `5`
**And** it must compare the `:views` field to the result using `>=`
**And** the resulting expression is: `p.views >= p.views - 5`

_Test reference: test/examples/ecto_query_dsl.exs:1402_

**Rule Statement 4:**

**Given** filter params: `[views: [<: [*: [:views, 2]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `*` directive to the `:views` field and `2`
**And** it must compare the `:views` field to the result using `<`
**And** the resulting expression is: `p.views < p.views * 2`

_Test reference: test/examples/ecto_query_dsl.exs:1414_

**Rule Statement 5:**

**Given** filter params: `[views: [==: [/: [:views, 2]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `/` directive to the `:views` field and `2`
**And** it must compare the `:views` field to the result using `==`
**And** the resulting expression is: `p.views == p.views / 2`

_Test reference: test/examples/ecto_query_dsl.exs:1427_

**Rule Statement 6:**

**Given** filter params: `[views: [!=: [+: [:views, 10]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `+` directive to the `:views` field and `10`
**And** it must compare the `:views` field to the result using `!=`
**And** the resulting expression is: `p.views != p.views + 10`

_Test reference: test/examples/ecto_query_dsl.exs:1440_

**Rule Statement 7:**

**Given** filter params: `[not: [views: [>=: [-: [:views, 5]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `-` directive to the `:views` field and `5`
**And** it must compare the `:views` field to the result using `>=`
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.views >= p.views - 5)`

_Test reference: test/examples/ecto_query_dsl.exs:1452_

**Rule Statement 8:**

**Given** filter params: `[not: [views: [<: [*: [:views, 2]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `*` directive to the `:views` field and `2`
**And** it must compare the `:views` field to the result using `<`
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.views < p.views * 2)`

_Test reference: test/examples/ecto_query_dsl.exs:1464_

**Rule Statement 9:**

**Given** filter params: `[views: [<=: [+: [10, 5]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `+` directive to the literal values `10` and `5`
**And** it must compare the `:views` field to the result using `<=`
**And** the resulting expression is: `p.views <= 10 + 5`

_Test reference: test/examples/ecto_query_dsl.exs:1477_

**Rule Statement 10:**

**Given** filter params: `[views: [>: [-: [100, 10]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `-` directive to the literal values `100` and `10`
**And** it must compare the `:views` field to the result using `>`
**And** the resulting expression is: `p.views > 100 - 10`

_Test reference: test/examples/ecto_query_dsl.exs:1490_

Note: All arithmetic operators (+, -, *, /) work with all comparison operators. Arithmetic can be applied to field references (e.g., `:views`) or literal values.

---

## Date/Time Directives

- `:datetime` or `:date`: Wrapper for date/time operations. Both use the same set of sub-keys.
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
**Then** it must apply the `datetime_add/3` function to the `:inserted_at` field with count `1` and interval `"day"`
**And** it must compare the `:inserted_at` field to the result using `>=`
**And** the resulting expression is: `p.inserted_at >= datetime_add(p.inserted_at, 1, "day")`

_Test reference: test/examples/ecto_query_dsl.exs:1505_

**Rule Statement 2:**

**Given** filter params: `[inserted_at: [>: [datetime: [ago: [count: 1, interval: "day"]]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `ago/2` function with count `1` and interval `"day"`
**And** it must compare the `:inserted_at` field to the result using `>`
**And** the resulting expression is: `p.inserted_at > ago(1, "day")`

_Test reference: test/examples/ecto_query_dsl.exs:1517_

**Rule Statement 3:**

**Given** filter params: `[inserted_at: [>: [datetime: [from_now: [count: 1, interval: "day"]]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `from_now/2` function with count `1` and interval `"day"`
**And** it must compare the `:inserted_at` field to the result using `>`
**And** the resulting expression is: `p.inserted_at > from_now(1, "day")`

_Test reference: test/examples/ecto_query_dsl.exs:1529_

**Rule Statement 4:**

**Given** filter params: `[not: [inserted_at: [>=: [datetime: [add: [field: :inserted_at, count: 1, interval: "day"]]]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `datetime_add/3` function to the `:inserted_at` field with count `1` and interval `"day"`
**And** it must compare the `:inserted_at` field to the result using `>=`
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.inserted_at >= datetime_add(p.inserted_at, 1, "day"))`

_Test reference: test/examples/ecto_query_dsl.exs:1541_

**Rule Statement 5:**

**Given** filter params: `[inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `ago/2` function with count `7` and interval `"day"`
**And** it must compare the `:inserted_at` field to the result using `<`
**And** the resulting expression is: `p.inserted_at < ago(7, "day")`

_Test reference: test/examples/ecto_query_dsl.exs:1553_

**Rule Statement 6:**

**Given** filter params: `[inserted_at: [<=: [datetime: [from_now: [count: 30, interval: "day"]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `from_now/2` function with count `30` and interval `"day"`
**And** it must compare the `:inserted_at` field to the result using `<=`
**And** the resulting expression is: `p.inserted_at <= from_now(30, "day")`

_Test reference: test/examples/ecto_query_dsl.exs:1565_

**Rule Statement 7:**

**Given** filter params: `[inserted_at: [==: [date: [ago: [count: 1, interval: "day"]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `ago/2` function with count `1` and interval `"day"` using the `:date` wrapper
**And** it must compare the `:inserted_at` field to the result using `==`
**And** the resulting expression is: `p.inserted_at == ago(1, "day")`

_Test reference: test/examples/ecto_query_dsl.exs:1577_

**Rule Statement 8:**

**Given** filter params: `[inserted_at: [!=: [date: [from_now: [count: 1, interval: "day"]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `from_now/2` function with count `1` and interval `"day"` using the `:date` wrapper
**And** it must compare the `:inserted_at` field to the result using `!=`
**And** the resulting expression is: `p.inserted_at != from_now(1, "day")`

_Test reference: test/examples/ecto_query_dsl.exs:1589_

**Rule Statement 9:**

**Given** filter params: `[not: [inserted_at: [<: [datetime: [ago: [count: 7, interval: "day"]]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `ago/2` function with count `7` and interval `"day"`
**And** it must compare the `:inserted_at` field to the result using `<`
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.inserted_at < ago(7, "day"))`

_Test reference: test/examples/ecto_query_dsl.exs:1601_

**Rule Statement 10:**

**Given** filter params: `[not: [inserted_at: [>: [date: [from_now: [count: 1, interval: "day"]]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `from_now/2` function with count `1` and interval `"day"` using the `:date` wrapper
**And** it must compare the `:inserted_at` field to the result using `>`
**And** it must negate the entire comparison
**And** the resulting expression is: `not (p.inserted_at > from_now(1, "day"))`

_Test reference: test/examples/ecto_query_dsl.exs:1613_

**Rule Statement 11:**

**Given** filter params: `[inserted_at: [>=: [date: [add: [field: :inserted_at, count: 7, interval: "day"]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `datetime_add/3` function to the `:inserted_at` field with count `7` and interval `"day"` using the `:date` wrapper
**And** it must compare the `:inserted_at` field to the result using `>=`
**And** the resulting expression is: `p.inserted_at >= datetime_add(p.inserted_at, 7, "day")`

_Test reference: test/examples/ecto_query_dsl.exs:1625_

**Rule Statement 12:**

**Given** filter params: `[inserted_at: [<: [date: [ago: [count: 1, interval: "month"]]]]]`
**When** the filter params are converted into a query expression
**Then** it must apply the `ago/2` function with count `1` and interval `"month"` using the `:date` wrapper
**And** it must compare the `:inserted_at` field to the result using `<`
**And** the resulting expression is: `p.inserted_at < ago(1, "month")`

_Test reference: test/examples/ecto_query_dsl.exs:1637_

Note: Both `:datetime` and `:date` support the same sub-operations (add, ago, from_now). All work with all comparison operators. Use `:date` for date-only comparisons and `:datetime` for timestamp comparisons.

---

## Binding Selector Directives

- `:bind`: specify binding context
  - `:as`: named binding
  - `:at`: positional binding

### Examples

    [bind: [as: :post, published: true]]
    [bind: [[as: :post, published: true], [as: :author, first_name: "John"]]]
    [bind: [at: 1, published: true]]
    [bind: [[at: 1, published: true]]]
    [bind: [at: :first, published: true]]
    [bind: [at: :last, first_name: "John"]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[bind: [as: :post, published: true]]`
**When** the filter params are converted into a query condition
**Then** it must apply the filter to the named binding `:post`
**And** it must compare the `:published` field to `true` using equality
**And** the resulting expression is: `post.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:1932_

**Rule Statement 2:**

**Given** filter params: `[bind: [[as: :post, published: true], [as: :author, first_name: "John"]]]`
**When** the filter params are converted into a query condition
**Then** it must apply filters to multiple named bindings
**And** it must compare `:published` to `true` on the `:post` binding
**And** it must compare `:first_name` to `"John"` on the `:author` binding
**And** the resulting expression is: `post.published == true and author.first_name == "John"`

_Test reference: test/examples/ecto_query_dsl.exs:1945_

**Rule Statement 3:**

**Given** filter params: `[bind: [at: 1, published: true]]`
**When** the filter params are converted into a query condition
**Then** it must apply the filter to the positional binding at index `1`
**And** it must compare the `:published` field to `true` using equality
**And** the resulting expression is: `binding_at_1.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:1965_

**Rule Statement 4:**

**Given** filter params: `[bind: [[at: 1, published: true]]]`
**When** the filter params are converted into a query condition
**Then** it must apply the filter to the positional binding at index `1`
**And** it must compare the `:published` field to `true` using equality
**And** the resulting expression is: `binding_at_1.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:1978_

**Rule Statement 5:**

**Given** filter params: `[bind: [at: :first, published: true]]`
**When** the filter params are converted into a query condition
**Then** it must apply the filter to the first binding
**And** it must compare the `:published` field to `true` using equality
**And** the resulting expression is: `first_binding.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:1991_

**Rule Statement 6:**

**Given** filter params: `[bind: [at: :last, first_name: "John"]]`
**When** the filter params are converted into a query condition
**Then** it must apply the filter to the last binding
**And** it must compare the `:first_name` field to `"John"` using equality
**And** the resulting expression is: `last_binding.first_name == "John"`

_Test reference: test/examples/ecto_query_dsl.exs:2004_

---

## Schema Filter Directives

- `:where`: explicit where clause
- `:or_where`: explicit OR where clause

### Examples

    [where: [published: true]]
    [where: [published: true, views: 10]]
    [or_where: [published: false]]
    [where: [published: true], or_where: [published: false]]
    [or_where: [or: [views: [>: 10, <: 5]]], published: true]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[where: [published: true]]`
**When** the filter params are converted into a query condition
**Then** it must apply an explicit WHERE clause
**And** it must compare the `:published` field to `true` using equality
**And** the resulting expression is: `where: p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:1651_

**Rule Statement 2:**

**Given** filter params: `[where: [published: true, views: 10]]`
**When** the filter params are converted into a query condition
**Then** it must apply an explicit WHERE clause with multiple conditions
**And** it must compare `:published` to `true` and `:views` to `10`
**And** the resulting expression is: `where: p.published == true and p.views == 10`

_Test reference: test/examples/ecto_query_dsl.exs:1664_

**Rule Statement 3:**

**Given** filter params: `[or_where: [published: false]]`
**When** the filter params are converted into a query condition
**Then** it must apply an explicit OR WHERE clause
**And** it must compare the `:published` field to `false` using equality
**And** the resulting expression is: `or_where: p.published == false`

_Test reference: test/examples/ecto_query_dsl.exs:1678_

**Rule Statement 4:**

**Given** filter params: `[where: [published: true], or_where: [published: false]]`
**When** the filter params are converted into a query condition
**Then** it must apply both WHERE and OR WHERE clauses
**And** it must compare `:published` to `true` in the WHERE clause
**And** it must compare `:published` to `false` in the OR WHERE clause
**And** the resulting expression is: `(p.published == true) or (p.published == false)`

_Test reference: test/examples/ecto_query_dsl.exs:1691_

**Rule Statement 5:**

**Given** filter params: `[or_where: [or: [views: [>: 10, <: 5]]], published: true]`
**When** the filter params are converted into a query condition
**Then** it must apply an OR WHERE clause with nested OR logic
**And** it must also apply an implicit WHERE for the `:published` field
**And** the resulting expression is: `(p.published == true) or (p.views > 10 or p.views < 5)`

_Test reference: test/examples/ecto_query_dsl.exs:1705_

---

## Terminal Filter Directives

- `:last`: Returns the last N records by reversing order, limiting, then re-ordering
- `:subquery`: Wraps the query and any additional filters in a subquery

### Examples

    [last: 2]
    [last: [title: 2]]
    [subquery: [id: 2]]
    [published: true, subquery: [id: 2]]
    [subquery: [published: true, views: [>: 10]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[last: 2]`
**When** the filter params are converted into a query condition
**Then** it must wrap the query in a subquery ordered descending by primary key with limit 2
**And** it must re-order the subquery result ascending by primary key
**And** the resulting expression is: `SELECT ... FROM (SELECT ... ORDER BY id DESC LIMIT 2) AS subquery ORDER BY id ASC`

_Test reference: test/examples/ecto_query_dsl.exs:1727_

**Rule Statement 2:**

**Given** filter params: `[subquery: [id: 2]]`
**When** the filter params are converted into a query condition
**Then** it must apply the `id: 2` filter to the query
**And** it must wrap the entire filtered query in a subquery
**And** the resulting expression is: `SELECT ... FROM (SELECT ... WHERE id = 2) AS subquery`

_Test reference: test/examples/ecto_query_dsl.exs:1744_

**Rule Statement 3:**

**Given** filter params: `[published: true, subquery: [id: 2]]`
**When** the filter params are converted into a query condition
**Then** it must apply both `published: true` and `id: 2` filters to the query
**And** it must wrap the entire filtered query in a subquery
**And** the resulting expression is: `SELECT ... FROM (SELECT ... WHERE published = true AND id = 2) AS subquery`

_Test reference: test/examples/ecto_query_dsl.exs:1758_

**Rule Statement 4:**

**Given** filter params: `[last: [title: 2]]`
**When** the filter params are converted into a query expression
**Then** it must wrap the query in a subquery ordered descending by `:title` with limit 2
**And** it must re-order the subquery result ascending by `:title`
**And** the resulting expression is: `SELECT ... FROM (SELECT ... ORDER BY title DESC LIMIT 2) AS subquery ORDER BY title ASC`

_Test reference: test/examples/ecto_query_dsl.exs:1772_

**Rule Statement 5:**

**Given** filter params: `[subquery: [published: true, views: [>: 10]]]`
**When** the filter params are converted into a query expression
**Then** it must apply both `published: true` and `views: [>: 10]` filters to the query
**And** it must wrap the entire filtered query in a subquery
**And** the resulting expression is: `SELECT ... FROM (SELECT ... WHERE published = true AND views > 10) AS subquery`

_Test reference: test/examples/ecto_query_dsl.exs:1787_

Note: `:last` can accept an integer, a tuple `{field, limit}`, or a keyword list with filters. `:subquery` wraps the entire query including all filters.

---

## Query Configuration Directives

- `:from`: specify source schema/table
- `:select`: select fields
- `:select_merge`: merge additional selections
- `:distinct`: distinct results
- `:group_by`: group by fields
- `:having`: having clause
- `:or_having`: OR having clause
- `:order_by`: order results
- `:prepend_order_by`: prepend to existing order
- `:limit`: limit results
- `:offset`: skip results
- `:first`: limit from start
- `:after`: pagination after
- `:before`: pagination before
- `:reverse_order`: reverse ordering
- `:exclude`: exclude query parts
- `:put_query_prefix`: set schema prefix
- `:start_date`: filter by start date
- `:end_date`: filter by end date
- `:ids`: filter by IDs
- `:dynamic`: raw dynamic expression
- `:exists`: exists subquery

### Examples

    [dynamic: dynamic([p], p.views > ^10)]
    [where: [dynamic: dynamic([p], p.published === ^true)]]
    [or_where: [dynamic: dynamic([p], p.views > ^100)]]
    [where: [exists: subquery_expr]]
    [where: [not: [exists: subquery_expr]]]
    [published: true, subquery: [id: 2]]
    [from: Post, id: 1, published: true]
    [from: Post, id: 1]
    [from: "posts", id: 1]
    [from: "posts", select: [:id]]
    [select: true]
    [select: :id]
    [select: [:id, :title]]
    [select: [map: [:id, :title]]]
    [select: [map: [custom_id: :id]]]
    [select: [struct: [:id]]]
    [select_merge: [map: [custom_id: :id]]]
    [select_merge: [map: [:id, :title]]]
    [select: [map: [:id]], select_merge: [map: [post_title: :title]]]
    [distinct: true]
    [distinct: false]
    [distinct: :title]
    [distinct: [desc: :title]]
    [distinct: :title, order_by: :id]
    [group_by: :author_id]
    [group_by: [:author_id, :published]]
    [having: [published: true]]
    [having: [views: [>: 10]]]
    [having: [views: [avg: [>: 10]]]]
    [having: dynamic([p], p.views > ^10)]
    [having: [and: [published: true, views: [>: 10]]]]
    [having: [or: [views: [>: 10], views: [<: 5]]]]
    [or_having: [views: [<: 5]]]
    [order_by: :title]
    [order_by: [desc: :title]]
    [order_by: [asc: :title, desc: :id]]
    [prepend_order_by: :title]
    [prepend_order_by: [asc: :published_at, desc: :title]]
    [after: 10]
    [before: 10]
    [limit: 10]
    [offset: 5]
    [first: 10]
    [limit: 10, offset: 5]
    [reverse_order: true]
    [exclude: :order_by]
    [exclude: [:order_by, :limit]]
    [put_query_prefix: "tenant_a"]
    [put_query_prefix: "tenant_a", put_query_prefix: "tenant_b"]
    [start_date: ~U[2026-01-01 00:00:00Z]]
    [end_date: ~U[2026-12-31 23:59:59Z]]
    [ids: [1, 2, 3]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[dynamic: dynamic([p], p.views > ^10)]`
**When** the filter params are converted into a query condition
**Then** it must apply the raw dynamic expression to the query
**And** the resulting expression is: `p.views > 10`

_Test reference: test/examples/ecto_query_dsl.exs:2338_

**Rule Statement 2:**

**Given** filter params: `[where: [dynamic: dynamic([p], p.published === ^true)]]`
**When** the filter params are converted into a query condition
**Then** it must apply the raw dynamic expression within an explicit WHERE clause
**And** the resulting expression is: `where: p.published === true`

_Test reference: test/examples/ecto_query_dsl.exs:2352_

**Rule Statement 3:**

**Given** filter params: `[or_where: [dynamic: dynamic([p], p.views > ^100)]]`
**When** the filter params are converted into a query condition
**Then** it must apply the raw dynamic expression within an OR WHERE clause
**And** the resulting expression is: `or_where: p.views > 100`

_Test reference: test/examples/ecto_query_dsl.exs:2366_

**Rule Statement 4:**

**Given** filter params: `[where: [exists: subquery_expr]]`
**When** the filter params are converted into a query condition
**Then** it must apply an EXISTS subquery check within a WHERE clause
**And** the resulting expression is: `where: exists(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:2380_

**Rule Statement 5:**

**Given** filter params: `[where: [not: [exists: subquery_expr]]]`
**When** the filter params are converted into a query condition
**Then** it must apply a negated EXISTS subquery check within a WHERE clause
**And** the resulting expression is: `where: not exists(subquery_expr)`

_Test reference: test/examples/ecto_query_dsl.exs:2396_

**Rule Statement 6:**

**Given** filter params: `[published: true, subquery: [id: 2]]`
**When** the filter params are converted into a query condition
**Then** it must apply the filter and convert to a subquery
**And** the resulting expression is: `from(s in subquery(from p in Post, where: p.id == 2), where: s.published == true)`

_Test reference: test/examples/ecto_query_dsl.exs:2412_

**Rule Statement 7:**

**Given** filter params: `[from: Post, id: 1, published: true]`
**When** the filter params are converted into a query condition
**Then** it must specify the source schema as `Post` and apply filters
**And** the resulting expression is: `from: Post, where: p.id == 1 and p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:1804_

**Rule Statement 8:**

**Given** filter params: `[from: Post, id: 1]`
**When** the filter params are converted into a query condition
**Then** it must specify the source schema as `Post` and filter by `:id`
**And** the resulting expression is: `from: Post, where: p.id == 1`

_Test reference: test/examples/ecto_query_dsl.exs:2426_

**Rule Statement 9:**

**Given** filter params: `[from: "posts", id: 1]`
**When** the filter params are converted into a query condition
**Then** it must specify the source table as `"posts"` and filter by `:id`
**And** the resulting expression is: `from: "posts", where: p.id == 1`

_Test reference: test/examples/ecto_query_dsl.exs:2439_

**Rule Statement 10:**

**Given** filter params: `[from: "posts", select: [:id]]`
**When** the filter params are converted into a query condition
**Then** it must specify the source table and select only the `:id` field
**And** the resulting expression is: `from: "posts", select: [:id]`

_Test reference: test/examples/ecto_query_dsl.exs:2452_

**Rule Statement 11:**

**Given** filter params: `[select: true]`
**When** the filter params are converted into a query condition
**Then** it must select all fields from the schema
**And** the resulting expression is: `select: true`

_Test reference: test/examples/ecto_query_dsl.exs:1817_

**Rule Statement 12:**

**Given** filter params: `[select: :id]`
**When** the filter params are converted into a query condition
**Then** it must select only the `:id` field
**And** the resulting expression is: `select: p.id`

_Test reference: test/examples/ecto_query_dsl.exs:1829_

**Rule Statement 13:**

**Given** filter params: `[select: [:id, :title]]`
**When** the filter params are converted into a query condition
**Then** it must select the `:id` and `:title` fields
**And** the resulting expression is: `select: [p.id, p.title]`

_Test reference: test/examples/ecto_query_dsl.exs:1841_

**Rule Statement 14:**

**Given** filter params: `[select: [map: [:id, :title]]]`
**When** the filter params are converted into a query condition
**Then** it must select `:id` and `:title` fields as an enumerable
**And** the resulting expression is: `select: %{id: p.id, title: p.title}`

_Test reference: test/examples/ecto_query_dsl.exs:1853_

**Rule Statement 15:**

**Given** filter params: `[select: [map: [custom_id: :id]]]`
**When** the filter params are converted into a query condition
**Then** it must select the `:id` field mapped to `:custom_id` key
**And** the resulting expression is: `select: %{custom_id: p.id}`

_Test reference: test/examples/ecto_query_dsl.exs:1865_

**Rule Statement 16:**

**Given** filter params: `[select: [struct: [:id]]]`
**When** the filter params are converted into a query condition
**Then** it must select the `:id` field as a struct
**And** the resulting expression is: `select: struct(p, [:id])`

_Test reference: test/examples/ecto_query_dsl.exs:1877_

**Rule Statement 17:**

**Given** filter params: `[select_merge: [map: [custom_id: :id]]]`
**When** the filter params are converted into a query condition
**Then** it must merge the `:id` field mapped to `:custom_id` into the existing selection
**And** the resulting expression is: `select_merge: %{custom_id: p.id}`

_Test reference: test/examples/ecto_query_dsl.exs:2464_

**Rule Statement 18:**

**Given** filter params: `[select_merge: [map: [:id, :title]]]`
**When** the filter params are converted into a query condition
**Then** it must merge `:id` and `:title` fields into the existing selection
**And** the resulting expression is: `select_merge: %{id: p.id, title: p.title}`

_Test reference: test/examples/ecto_query_dsl.exs:2477_

**Rule Statement 19:**

**Given** filter params: `[select: [map: [:id]], select_merge: [map: [post_title: :title]]]`
**When** the filter params are converted into a query condition
**Then** it must select `:id` and merge `:title` as `:post_title`
**And** the resulting expression is: `select: %{id: p.id, post_title: p.title}`

_Test reference: test/examples/ecto_query_dsl.exs:2490_

**Rule Statement 20:**

**Given** filter params: `[distinct: true]`
**When** the filter params are converted into a query condition
**Then** it must apply DISTINCT to the query results
**And** the resulting expression is: `distinct: true`

_Test reference: test/examples/ecto_query_dsl.exs:1889_

**Rule Statement 21:**

**Given** filter params: `[distinct: false]`
**When** the filter params are converted into a query condition
**Then** it must not apply DISTINCT to the query results
**And** the resulting expression is: `distinct: false`

_Test reference: test/examples/ecto_query_dsl.exs:2503_

**Rule Statement 22:**

**Given** filter params: `[distinct: :title]`
**When** the filter params are converted into a query condition
**Then** it must apply DISTINCT on the `:title` field
**And** the resulting expression is: `distinct: p.title`

_Test reference: test/examples/ecto_query_dsl.exs:2516_

**Rule Statement 23:**

**Given** filter params: `[distinct: [desc: :title]]`
**When** the filter params are converted into a query condition
**Then** it must apply DISTINCT on `:title` with descending order
**And** the resulting expression is: `distinct: [desc: p.title]`

_Test reference: test/examples/ecto_query_dsl.exs:2530_

**Rule Statement 24:**

**Given** filter params: `[distinct: :title, order_by: :id]`
**When** the filter params are converted into a query condition
**Then** it must apply DISTINCT on `:title` and order by `:id`
**And** the resulting expression is: `distinct: p.title, order_by: p.id`

_Test reference: test/examples/ecto_query_dsl.exs:2544_

**Rule Statement 25:**

**Given** filter params: `[group_by: :author_id]`
**When** the filter params are converted into a query condition
**Then** it must group results by the `:author_id` field
**And** the resulting expression is: `group_by: p.author_id`

_Test reference: test/examples/ecto_query_dsl.exs:1902_

**Rule Statement 26:**

**Given** filter params: `[group_by: [:author_id, :published]]`
**When** the filter params are converted into a query condition
**Then** it must group results by `:author_id` and `:published` fields
**And** the resulting expression is: `group_by: [p.author_id, p.published]`

_Test reference: test/examples/ecto_query_dsl.exs:2557_

**Rule Statement 27:**

**Given** filter params: `[having: [published: true]]`
**When** the filter params are converted into a query condition
**Then** it must apply a HAVING clause filtering on `:published` equals `true`
**And** the resulting expression is: `having: p.published == true`

_Test reference: test/examples/ecto_query_dsl.exs:1915_

**Rule Statement 28:**

**Given** filter params: `[having: [views: [>: 10]]]`
**When** the filter params are converted into a query condition
**Then** it must apply a HAVING clause filtering on `:views` greater than `10`
**And** the resulting expression is: `having: p.views > 10`

_Test reference: test/examples/ecto_query_dsl.exs:2570_

**Rule Statement 29:**

**Given** filter params: `[having: [views: [avg: [>: 10]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply a HAVING clause with aggregate function `avg` on `:views`
**And** the resulting expression is: `having: avg(p.views) > 10`

_Test reference: test/examples/ecto_query_dsl.exs:2583_

**Rule Statement 30:**

**Given** filter params: `[having: dynamic([p], p.views > ^10)]`
**When** the filter params are converted into a query condition
**Then** it must apply a HAVING clause with a raw dynamic expression
**And** the resulting expression is: `having: p.views > 10`

_Test reference: test/examples/ecto_query_dsl.exs:2596_

**Rule Statement 31:**

**Given** filter params: `[having: [and: [published: true, views: [>: 10]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply a HAVING clause with AND logic
**And** the resulting expression is: `having: p.published == true and p.views > 10`

_Test reference: test/examples/ecto_query_dsl.exs:2610_

**Rule Statement 32:**

**Given** filter params: `[having: [or: [views: [>: 10], views: [<: 5]]]]`
**When** the filter params are converted into a query condition
**Then** it must apply a HAVING clause with OR logic
**And** the resulting expression is: `having: p.views > 10 or p.views < 5`

_Test reference: test/examples/ecto_query_dsl.exs:2627_

**Rule Statement 33:**

**Given** filter params: `[or_having: [views: [<: 5]]]`
**When** the filter params are converted into a query condition
**Then** it must apply an OR HAVING clause
**And** the resulting expression is: `or_having: p.views < 5`

_Test reference: test/examples/ecto_query_dsl.exs:2644_

**Rule Statement 34:**

**Given** filter params: `[order_by: :title]`
**When** the filter params are converted into a query condition
**Then** it must order results by the `:title` field in ascending order
**And** the resulting expression is: `order_by: [asc: p.title]`

_Test reference: test/examples/ecto_query_dsl.exs:1931_

**Rule Statement 35:**

**Given** filter params: `[order_by: [desc: :title]]`
**When** the filter params are converted into a query condition
**Then** it must order results by the `:title` field in descending order
**And** the resulting expression is: `order_by: [desc: p.title]`

_Test reference: test/examples/ecto_query_dsl.exs:1944_

**Rule Statement 36:**

**Given** filter params: `[order_by: [asc: :title, desc: :id]]`
**When** the filter params are converted into a query condition
**Then** it must order results by `:title` ascending then `:id` descending
**And** the resulting expression is: `order_by: [asc: p.title, desc: p.id]`

_Test reference: test/examples/ecto_query_dsl.exs:2660_

**Rule Statement 37:**

**Given** filter params: `[prepend_order_by: :title]`
**When** the filter params are converted into a query condition
**Then** it must prepend `:title` to the existing order
**And** the resulting expression is: `prepend_order_by: [asc: p.title]`

_Test reference: test/examples/ecto_query_dsl.exs:2675_

**Rule Statement 38:**

**Given** filter params: `[prepend_order_by: [asc: :published_at, desc: :title]]`
**When** the filter params are converted into a query condition
**Then** it must prepend `:published_at` ascending and `:title` descending to the existing order
**And** the resulting expression is: `prepend_order_by: [asc: p.published_at, desc: p.title]`

_Test reference: test/examples/ecto_query_dsl.exs:2690_

**Rule Statement 39:**

**Given** filter params: `[after: 10]`
**When** the filter params are converted into a query condition
**Then** it must apply pagination starting after cursor position `10`
**And** the resulting expression is: `after: 10`

_Test reference: test/examples/ecto_query_dsl.exs:2703_

**Rule Statement 40:**

**Given** filter params: `[before: 10]`
**When** the filter params are converted into a query condition
**Then** it must apply pagination ending before cursor position `10`
**And** the resulting expression is: `before: 10`

_Test reference: test/examples/ecto_query_dsl.exs:2717_

**Rule Statement 41:**

**Given** filter params: `[limit: 10]`
**When** the filter params are converted into a query condition
**Then** it must limit results to `10` records
**And** the resulting expression is: `limit: 10`

_Test reference: test/examples/ecto_query_dsl.exs:1957_

**Rule Statement 42:**

**Given** filter params: `[offset: 5]`
**When** the filter params are converted into a query condition
**Then** it must skip the first `5` records
**And** the resulting expression is: `offset: 5`

_Test reference: test/examples/ecto_query_dsl.exs:1971_

**Rule Statement 43:**

**Given** filter params: `[first: 10]`
**When** the filter params are converted into a query condition
**Then** it must limit results to the first `10` records
**And** the resulting expression is: `first: 10`

_Test reference: test/examples/ecto_query_dsl.exs:2731_

**Rule Statement 44:**

**Given** filter params: `[limit: 10, offset: 5]`
**When** the filter params are converted into a query condition
**Then** it must skip `5` records and limit to `10` records
**And** the resulting expression is: `limit: 10, offset: 5`

_Test reference: test/examples/ecto_query_dsl.exs:1985_

**Rule Statement 45:**

**Given** filter params: `[reverse_order: true]`
**When** the filter params are converted into a query condition
**Then** it must reverse the existing order
**And** the resulting expression is: `reverse_order: true`

_Test reference: test/examples/ecto_query_dsl.exs:2745_

**Rule Statement 46:**

**Given** filter params: `[exclude: :order_by]`
**When** the filter params are converted into a query condition
**Then** it must exclude the ORDER BY clause from the query
**And** the resulting expression is: `exclude: :order_by`

_Test reference: test/examples/ecto_query_dsl.exs:2759_

**Rule Statement 47:**

**Given** filter params: `[exclude: [:order_by, :limit]]`
**When** the filter params are converted into a query condition
**Then** it must exclude both ORDER BY and LIMIT clauses from the query
**And** the resulting expression is: `exclude: [:order_by, :limit]`

_Test reference: test/examples/ecto_query_dsl.exs:2773_

**Rule Statement 48:**

**Given** filter params: `[put_query_prefix: "tenant_a"]`
**When** the filter params are converted into a query condition
**Then** it must set the schema prefix to `"tenant_a"`
**And** the resulting expression is: `put_query_prefix: "tenant_a"`

_Test reference: test/examples/ecto_query_dsl.exs:2788_

**Rule Statement 49:**

**Given** filter params: `[put_query_prefix: "tenant_a", put_query_prefix: "tenant_b"]`
**When** the filter params are converted into a query condition
**Then** it must set the schema prefix to `"tenant_b"` (last one wins)
**And** the resulting expression is: `put_query_prefix: "tenant_b"`

_Test reference: test/examples/ecto_query_dsl.exs:2801_

**Rule Statement 50:**

**Given** filter params: `[start_date: ~U[2026-01-01 00:00:00Z]]`
**When** the filter params are converted into a query condition
**Then** it must filter records with `inserted_at` greater than or equal to the start date
**And** the resulting expression is: `where: p.inserted_at >= ~U[2026-01-01 00:00:00Z]`

_Test reference: test/examples/ecto_query_dsl.exs:1999_

**Rule Statement 51:**

**Given** filter params: `[end_date: ~U[2026-12-31 23:59:59Z]]`
**When** the filter params are converted into a query condition
**Then** it must filter records with `inserted_at` less than or equal to the end date
**And** the resulting expression is: `where: p.inserted_at <= ~U[2026-12-31 23:59:59Z]`

_Test reference: test/examples/ecto_query_dsl.exs:2012_

**Rule Statement 52:**

**Given** filter params: `[ids: [1, 2, 3]]`
**When** the filter params are converted into a query condition
**Then** it must filter records where `:id` is in the list `[1, 2, 3]`
**And** the resulting expression is: `where: p.id in [1, 2, 3]`

_Test reference: test/examples/ecto_query_dsl.exs:2025_

---

## Set Directives

- `:except`: set except
- `:except_all`: set except all
- `:intersect`: set intersect
- `:intersect_all`: set intersect all
- `:union`: set union
- `:union_all`: set union all

### Examples

    [except: [published: false]]
    [except: from(p in Post, where: p.published === ^false)]
    [except_all: [published: false]]
    [intersect: [published: false]]
    [intersect_all: [published: false]]
    [union: [published: false]]
    [union_all: [published: false]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[except: [published: false]]`
**When** the filter params are converted into a query condition
**Then** it must apply a set EXCEPT operation with the filter
**And** the resulting expression is: `except: from(p in Post, where: p.published == false)`

_Test reference: test/examples/ecto_query_dsl.exs:2816_

**Rule Statement 2:**

**Given** filter params: `[except: from(p in Post, where: p.published === ^false)]`
**When** the filter params are converted into a query condition
**Then** it must apply a set EXCEPT operation with the provided query
**And** the resulting expression is: `except: from(p in Post, where: p.published === false)`

_Test reference: test/examples/ecto_query_dsl.exs:2830_

**Rule Statement 3:**

**Given** filter params: `[except_all: [published: false]]`
**When** the filter params are converted into a query condition
**Then** it must apply a set EXCEPT ALL operation with the filter
**And** the resulting expression is: `except_all: from(p in Post, where: p.published == false)`

_Test reference: test/examples/ecto_query_dsl.exs:2844_

**Rule Statement 4:**

**Given** filter params: `[intersect: [published: false]]`
**When** the filter params are converted into a query condition
**Then** it must apply a set INTERSECT operation with the filter
**And** the resulting expression is: `intersect: from(p in Post, where: p.published == false)`

_Test reference: test/examples/ecto_query_dsl.exs:2858_

**Rule Statement 5:**

**Given** filter params: `[intersect_all: [published: false]]`
**When** the filter params are converted into a query condition
**Then** it must apply a set INTERSECT ALL operation with the filter
**And** the resulting expression is: `intersect_all: from(p in Post, where: p.published == false)`

_Test reference: test/examples/ecto_query_dsl.exs:2872_

**Rule Statement 6:**

**Given** filter params: `[union: [published: false]]`
**When** the filter params are converted into a query condition
**Then** it must apply a set UNION operation with the filter
**And** the resulting expression is: `union: from(p in Post, where: p.published == false)`

_Test reference: test/examples/ecto_query_dsl.exs:2886_

**Rule Statement 7:**

**Given** filter params: `[union_all: [published: false]]`
**When** the filter params are converted into a query condition
**Then** it must apply a set UNION ALL operation with the filter
**And** the resulting expression is: `union_all: from(p in Post, where: p.published == false)`

_Test reference: test/examples/ecto_query_dsl.exs:2901_

---

## Lock Directives

- `:lock`: query locking

### Examples

    [lock: fn query -> from(p in query, lock: "FOR UPDATE") end]
    [lock: [name: :for_share, values: []]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[lock: fn query -> from(p in query, lock: "FOR UPDATE") end]`
**When** the filter params are converted into a query condition
**Then** it must apply a lock using the provided function
**And** the resulting expression is: `lock: "FOR UPDATE"`

_Test reference: test/examples/ecto_query_dsl.exs:2918_

**Rule Statement 2:**

**Given** filter params: `[lock: [name: :for_share, values: []]]`
**When** the filter params are converted into a query condition
**Then** it must apply a lock with the specified name and values
**And** the resulting expression is: `lock: "FOR SHARE"`

_Test reference: test/examples/ecto_query_dsl.exs:2930_

---

## CTE Directives

- `:recursive_ctes`: enable recursive CTEs
- `:with_cte`: define CTE

### Examples

    [recursive_ctes: true]
    [recursive_ctes: false]
    [with_cte: [published_posts: [as: cte_query]]]
    [with_cte: [published_posts: [as: cte_query, materialized: false, operation: :all]]]
    [with_cte: [published_posts: [as: [from: [query: Post, published: true]]]]]
    [with_cte: [published_posts: [as: [from: [query: Post, id: 1]]]]]
    [recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[recursive_ctes: true]`
**When** the filter params are converted into a query condition
**Then** it must enable recursive CTEs for the query
**And** the resulting expression is: `recursive_ctes: true`

_Test reference: test/examples/ecto_query_dsl.exs:2944_

**Rule Statement 2:**

**Given** filter params: `[recursive_ctes: false]`
**When** the filter params are converted into a query condition
**Then** it must disable recursive CTEs for the query
**And** the resulting expression is: `recursive_ctes: false`

_Test reference: test/examples/ecto_query_dsl.exs:2959_

**Rule Statement 3:**

**Given** filter params: `[with_cte: [published_posts: [as: cte_query]]]`
**When** the filter params are converted into a query condition
**Then** it must define a CTE named `published_posts` using the provided query
**And** the resulting expression is: `with_cte: [published_posts: [as: cte_query]]`

_Test reference: test/examples/ecto_query_dsl.exs:2971_

**Rule Statement 4:**

**Given** filter params: `[with_cte: [published_posts: [as: cte_query, materialized: false, operation: :all]]]`
**When** the filter params are converted into a query condition
**Then** it must define a CTE with materialization disabled and operation set to `:all`
**And** the resulting expression is: `with_cte: [published_posts: [as: cte_query, materialized: false]]`

_Test reference: test/examples/ecto_query_dsl.exs:2988_

**Rule Statement 5:**

**Given** filter params: `[with_cte: [published_posts: [as: [from: [query: Post, published: true]]]]]`
**When** the filter params are converted into a query condition
**Then** it must define a CTE by building a query from the filter params
**And** the resulting expression is: `with_cte: [published_posts: [as: from(p in Post, where: p.published == true)]]`

_Test reference: test/examples/ecto_query_dsl.exs:3004_

**Rule Statement 6:**

**Given** filter params: `[with_cte: [published_posts: [as: [from: [query: Post, id: 1]]]]]`
**When** the filter params are converted into a query condition
**Then** it must define a CTE by building a query from the filter params
**And** the resulting expression is: `with_cte: [published_posts: [as: from(p in Post, where: p.id == 1)]]`

_Test reference: test/examples/ecto_query_dsl.exs:3021_

**Rule Statement 7:**

**Given** filter params: `[recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]]`
**When** the filter params are converted into a query condition
**Then** it must enable recursive CTEs and define a CTE named `published_posts`
**And** the resulting expression is: `recursive_ctes: true, with_cte: [published_posts: [as: cte_query]]`

_Test reference: test/examples/ecto_query_dsl.exs:3038_

---

## Named Binding Directives

- `:with_named_binding`: create named binding

### Examples

    [with_named_binding: [author: [join: [association: [source: :author, as: :author]]]]]
    [with_named_binding: [author: [join: [association: [source: :author, as: :author]]], users_table: [join: [table: [source: "users", as: :users_table, on: true]]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[with_named_binding: [author: [join: [association: [source: :author, as: :author]]]]]`
**When** the filter params are converted into a query condition
**Then** it must create a named binding `:author` with an association join
**And** the resulting expression is: `with_named_binding: [author: join]`

_Test reference: test/examples/ecto_query_dsl.exs:3057_

**Rule Statement 2:**

**Given** filter params: `[with_named_binding: [author: [join: [association: [source: :author, as: :author]]], users_table: [join: [table: [source: "users", as: :users_table, on: true]]]]]`
**When** the filter params are converted into a query condition
**Then** it must create multiple named bindings with their respective joins
**And** the resulting expression is: `with_named_binding: [author: join, users_table: join]`

_Test reference: test/examples/ecto_query_dsl.exs:3071_

---

## Query Modifier Directives

- `:with_ties`: include ties in limit
- `:update`: update operations
- `:windows`: window functions
- `:preload`: preload associations

### Examples

    [with_ties: true]
    [with_ties: false]
    [with_ties: [bind: [as: :post, value: true]]]
    [with_ties: [bind: [at: 1, value: true]]]
    [update: [set: [title: "After"], inc: [views: 1]]]
    [windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]]
    [preload: :author]
    [preload: [:author]]
    [preload: [author: [:posts]]]
    [preload: [bind: [as: :example, value: :author]]]
    [preload: [bind: [at: 2, value: :author]]]
    [preload: [bind: [at: 2, value: :author], posts: [:comments]]]
    [preload: [bind: [as: :author, value: :author], posts: [:comments]]]
    [preload: [bind: [[as: :author, value: :author], [at: 2, value: :author]], posts: [:comments]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[with_ties: true]`
**When** the filter params are converted into a query condition
**Then** it must include ties in the limit clause
**And** the resulting expression is: `with_ties: true`

_Test reference: test/examples/ecto_query_dsl.exs:3089_

**Rule Statement 2:**

**Given** filter params: `[with_ties: false]`
**When** the filter params are converted into a query condition
**Then** it must not include ties in the limit clause
**And** the resulting expression is: `with_ties: false`

_Test reference: test/examples/ecto_query_dsl.exs:3103_

**Rule Statement 3:**

**Given** filter params: `[with_ties: [bind: [as: :post, value: true]]]`
**When** the filter params are converted into a query condition
**Then** it must apply with_ties to the named binding `:post`
**And** the resulting expression is: `with_ties: [bind: [as: :post, value: true]]`

_Test reference: test/examples/ecto_query_dsl.exs:3116_

**Rule Statement 4:**

**Given** filter params: `[with_ties: [bind: [at: 1, value: true]]]`
**When** the filter params are converted into a query condition
**Then** it must apply with_ties to the positional binding at index `1`
**And** the resulting expression is: `with_ties: [bind: [at: 1, value: true]]`

_Test reference: test/examples/ecto_query_dsl.exs:3128_

**Rule Statement 5:**

**Given** filter params: `[update: [set: [title: "After"], inc: [views: 1]]]`
**When** the filter params are converted into a query condition
**Then** it must set `:title` to `"After"` and increment `:views` by `1`
**And** the resulting expression is: `update: [set: [title: "After"], inc: [views: 1]]`

_Test reference: test/examples/ecto_query_dsl.exs:3140_

**Rule Statement 6:**

**Given** filter params: `[windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]]`
**When** the filter params are converted into a query condition
**Then** it must define a window function named `post_window` partitioned by `:author_id` and ordered by `:inserted_at` descending
**And** the resulting expression is: `windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]`

_Test reference: test/examples/ecto_query_dsl.exs:3154_

**Rule Statement 7:**

**Given** filter params: `[preload: :author]`
**When** the filter params are converted into a query condition
**Then** it must preload the `:author` association
**And** the resulting expression is: `preload: :author`

_Test reference: test/examples/ecto_query_dsl.exs:3169_

**Rule Statement 8:**

**Given** filter params: `[preload: [:author]]`
**When** the filter params are converted into a query condition
**Then** it must preload the `:author` association
**And** the resulting expression is: `preload: [:author]`

_Test reference: test/examples/ecto_query_dsl.exs:3182_

**Rule Statement 9:**

**Given** filter params: `[preload: [author: [:posts]]]`
**When** the filter params are converted into a query condition
**Then** it must preload the `:author` association with nested `:posts` association
**And** the resulting expression is: `preload: [author: [:posts]]`

_Test reference: test/examples/ecto_query_dsl.exs:3195_

**Rule Statement 10:**

**Given** filter params: `[preload: [bind: [as: :example, value: :author]]]`
**When** the filter params are converted into a query condition
**Then** it must preload the `:author` association from the named binding `:example`
**And** the resulting expression is: `preload: [bind: [as: :example, value: :author]]`

_Test reference: test/examples/ecto_query_dsl.exs:3208_

**Rule Statement 11:**

**Given** filter params: `[preload: [bind: [at: 2, value: :author]]]`
**When** the filter params are converted into a query condition
**Then** it must preload the `:author` association from the positional binding at index `2`
**And** the resulting expression is: `preload: [bind: [at: 2, value: :author]]`

_Test reference: test/examples/ecto_query_dsl.exs:3223_

**Rule Statement 12:**

**Given** filter params: `[preload: [bind: [at: 2, value: :author], posts: [:comments]]]`
**When** the filter params are converted into a query condition
**Then** it must preload `:author` from binding at index `2` and `:posts` with nested `:comments`
**And** the resulting expression is: `preload: [bind: [at: 2, value: :author], posts: [:comments]]`

_Test reference: test/examples/ecto_query_dsl.exs:3238_

**Rule Statement 13:**

**Given** filter params: `[preload: [bind: [as: :author, value: :author], posts: [:comments]]]`
**When** the filter params are converted into a query condition
**Then** it must preload `:author` from named binding `:author` and `:posts` with nested `:comments`
**And** the resulting expression is: `preload: [bind: [as: :author, value: :author], posts: [:comments]]`

_Test reference: test/examples/ecto_query_dsl.exs:3253_

**Rule Statement 14:**

**Given** filter params: `[preload: [bind: [[as: :author, value: :author], [at: 2, value: :author]], posts: [:comments]]]`
**When** the filter params are converted into a query condition
**Then** it must preload `:author` from multiple bindings and `:posts` with nested `:comments`
**And** the resulting expression is: `preload: [bind: [[as: :author, value: :author], [at: 2, value: :author]], posts: [:comments]]`

_Test reference: test/examples/ecto_query_dsl.exs:3268_

---

## Join Directives

- `:join`: explicit join
  - `:association`: (association join type)
  - `:schema`: (schema join type)
  - `:table`: (table join type)
  - `:query`: (query join type)
  - `:fragment`: (fragment join type)
  - `:on`: (join condition)
  - `:type`: (join type - :left, etc.)

### Examples

    [author: [as: :author, first_name: "John"]]
    [author: [first_name: "John"]]
    [author: [as: :author, on: true, first_name: "John"]]
    [author: [as: :author, type: :left, first_name: "John"]]
    [join: [author: [as: :author]]]
    [join: [schema: [source: User, as: :user_join, on: true]]]
    [join: [table: [source: "users", as: :users_table, on: true]]]
    [join: [query: [source: user_query, as: :adult_users, on: true]]]
    [join: [subquery: [source: user_query, as: :name, on: true]]]
    [join: [subquery: [source: [from: [query: User, age: [>=: 18]]], as: :name, on: true]]]
    [join: [subquery: [source: [from: [published: true]], as: :name, on: true]]]
    [join: [fragment: [source: [name: :active_users, values: [min_age: 21]], as: :active_users, on: true]]]
    [join: [fragment: [source: [name: :active_users, values: [min_age: 21]], hints: :test_index, as: :active_users, on: true]]]
    [join: [author: [as: :author], table: [source: "users", as: :users_table, on: true]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[author: [as: :author, first_name: "John"]]`
**When** the filter params are converted into a query condition
**Then** it must create an association join for `:author` with alias `:author` and filter on `:first_name`
**And** the resulting expression is: `join: :author, as: :author, where: author.first_name == "John"`

_Test reference: test/examples/ecto_query_dsl.exs:3285_

**Rule Statement 2:**

**Given** filter params: `[author: [first_name: "John"]]`
**When** the filter params are converted into a query condition
**Then** it must create an association join for `:author` and filter on `:first_name`
**And** the resulting expression is: `join: :author, where: author.first_name == "John"`

_Test reference: test/examples/ecto_query_dsl.exs:3301_

**Rule Statement 3:**

**Given** filter params: `[author: [as: :author, on: true, first_name: "John"]]`
**When** the filter params are converted into a query condition
**Then** it must create an association join for `:author` with explicit ON condition and filter
**And** the resulting expression is: `join: :author, as: :author, on: true, where: author.first_name == "John"`

_Test reference: test/examples/ecto_query_dsl.exs:3317_

**Rule Statement 4:**

**Given** filter params: `[author: [as: :author, type: :left, first_name: "John"]]`
**When** the filter params are converted into a query condition
**Then** it must create a LEFT association join for `:author` with filter
**And** the resulting expression is: `left_join: :author, as: :author, where: author.first_name == "John"`

_Test reference: test/examples/ecto_query_dsl.exs:3332_

**Rule Statement 5:**

**Given** filter params: `[join: [author: [as: :author]]]`
**When** the filter params are converted into a query condition
**Then** it must create an explicit association join for `:author` with alias
**And** the resulting expression is: `join: :author, as: :author`

_Test reference: test/examples/ecto_query_dsl.exs:3348_

**Rule Statement 6:**

**Given** filter params: `[join: [schema: [source: User, as: :user_join, on: true]]]`
**When** the filter params are converted into a query condition
**Then** it must create a schema join with the `User` schema
**And** the resulting expression is: `join: User, as: :user_join, on: true`

_Test reference: test/examples/ecto_query_dsl.exs:3361_

**Rule Statement 7:**

**Given** filter params: `[join: [table: [source: "users", as: :users_table, on: true]]]`
**When** the filter params are converted into a query condition
**Then** it must create a table join with the `"users"` table
**And** the resulting expression is: `join: "users", as: :users_table, on: true`

_Test reference: test/examples/ecto_query_dsl.exs:3376_

**Rule Statement 8:**

**Given** filter params: `[join: [query: [source: user_query, as: :adult_users, on: true]]]`
**When** the filter params are converted into a query condition
**Then** it must create a query join with the provided query
**And** the resulting expression is: `join: user_query, as: :adult_users, on: true`

_Test reference: test/examples/ecto_query_dsl.exs:3391_

**Rule Statement 9:**

**Given** filter params: `[join: [subquery: [source: user_query, as: :name, on: true]]]`
**When** the filter params are converted into a query condition
**Then** it must create a subquery join with the provided query
**And** the resulting expression is: `join: subquery(user_query), as: :name, on: true`

_Test reference: test/examples/ecto_query_dsl.exs:3407_

**Rule Statement 10:**

**Given** filter params: `[join: [subquery: [source: [from: [query: User, age: [>=: 18]]], as: :name, on: true]]]`
**When** the filter params are converted into a query condition
**Then** it must create a subquery join by building the query from filter params
**And** the resulting expression is: `join: subquery(from u in User, where: u.age >= 18), as: :name, on: true`

_Test reference: test/examples/ecto_query_dsl.exs:3423_

**Rule Statement 11:**

**Given** filter params: `[join: [subquery: [source: [from: [published: true]], as: :name, on: true]]]`
**When** the filter params are converted into a query condition
**Then** it must create a subquery join by building the query from filter params
**And** the resulting expression is: `join: subquery(from p in Post, where: p.published == true), as: :name, on: true`

_Test reference: test/examples/ecto_query_dsl.exs:3439_

**Rule Statement 12:**

**Given** filter params: `[join: [fragment: [source: [name: :active_users, values: [min_age: 21]], as: :active_users, on: true]]]`
**When** the filter params are converted into a query condition
**Then** it must create a fragment join with the specified fragment name and values
**And** the resulting expression is: `join: fragment("active_users(?)", 21), as: :active_users, on: true`

_Test reference: test/examples/ecto_query_dsl.exs:3456_

**Rule Statement 13:**

**Given** filter params: `[join: [fragment: [source: [name: :active_users, values: [min_age: 21]], hints: :test_index, as: :active_users, on: true]]]`
**When** the filter params are converted into a query condition
**Then** it must create a fragment join with hints
**And** the resulting expression is: `join: fragment("active_users(?)", 21), as: :active_users, hints: :test_index, on: true`

_Test reference: test/examples/ecto_query_dsl.exs:3468_

**Rule Statement 14:**

**Given** filter params: `[join: [author: [as: :author], table: [source: "users", as: :users_table, on: true]]]`
**When** the filter params are converted into a query condition
**Then** it must create multiple joins (association and table)
**And** the resulting expression is: `join: [:author, "users"], as: [:author, :users_table]`

_Test reference: test/examples/ecto_query_dsl.exs:3480_