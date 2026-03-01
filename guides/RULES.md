# Filter Ordering Rules

This document defines the nesting and precedence rules for special keys in the EctoShorts filtering language.

Nesting controls meaning.

Some keys act as wrappers (for example `:not`, aggregates, `:all`/`:any`, and `:bind`). Wrapper keys must wrap the expression they apply to.

When multiple special keys appear at the same level (sibling keys), the filtering system applies them in a fixed precedence order. Input order does not change the result.

## Negation Rules

**Rule 1:** The `:not` key must wrap (contain) comparison operators and other operators.

When you want to negate a condition, `:not` must be the outermost key wrapping the operator you're negating.

Examples:
- `%{published: %{not: %{in: [true, false]}}}` - `:not` wraps `:in`
- `%{views: %{not: %{>: 10}}}` - `:not` wraps the `>` comparison
- `%{published: %{not: %{!=: [true, false]}}}` - `:not` wraps `!=`

**Rule 2:** The `:not` key must wrap aggregate functions when negating aggregates.

When negating an aggregate function like `:avg`, `:count`, `:sum`, etc., `:not` must be the outer wrapper.

Examples:
- `%{views: %{not: %{avg: %{>: 10}}}}` - `:not` wraps the aggregate `:avg`
- `%{views: %{not: %{avg: %{==: 10}}}}` - `:not` wraps the aggregate `:avg`
- `%{tags: %{not: %{count: %{==: 0}}}}` - `:not` wraps the `:count` aggregate

## Comparison Operator Rules

**Rule 3:** Comparison operators (`:==`, `:!=`, `:>`, `:>=`, `:<`, `:<=`, `:eq`, `:gt`, `:gte`, `:lt`, `:lte`) must wrap their operand values.

Comparison operators are the keys that specify how to compare a field. They must wrap the value being compared.

When the operand is `nil`, the comparison produces an IS NULL or IS NOT NULL check. Only `:==`, `:eq`, and `:!=` are valid operators for `nil` comparisons.

Examples:
- `%{views: %{>: 10}}` - `>` wraps the value `10`
- `%{id: %{==: 1}}` - `==` wraps the value `1`
- `%{published_at: %{!=: nil}}` - `!=` wraps `nil` (IS NOT NULL)
- `%{published_at: %{==: nil}}` - `==` wraps `nil` (IS NULL)

**Rule 4:** When used on a scalar field, the `:in` operator tests membership and must wrap a list of values.

When used on a scalar field, the `:in` operator tests whether the field value is a member of the given list. When negating an `:in` operator, use `:not` to wrap the entire `:in` expression.

Examples:
- `%{published: %{in: [true, false]}}` - `:in` wraps a list
- `%{published: %{not: %{in: [true, false]}}}` - `:not` wraps the `:in` operator
- `%{id: %{in: [1, 2, 3]}}` - `:in` wraps a list of values

**Note:** When `:==` or `:!=` receives a list on a scalar field, it coerces to `IN` or `NOT IN` respectively. For example, `%{published: %{==: [true, false]}}` is equivalent to `%{published: %{in: [true, false]}}`. Negation applies: `%{published: %{not: %{==: [true, false]}}}` produces `NOT IN`.

## String Matching Rules

**Rule 5:** String matching operators (`:like`, `:ilike`) must wrap their pattern values.

These operators specify pattern matching behavior and must wrap the pattern string or list of patterns.

Examples:
- `%{title: %{like: "hello"}}` - `:like` wraps a single pattern
- `%{title: %{ilike: ["hello", "world"]}}` - `:ilike` wraps a list of patterns
- `%{title: %{not: %{like: "hello"}}}` - `:like` is wrapped by `:not`

## String Transformation Rules

**Rule 6:** String transformation operators (`:lower`, `:upper`) must appear as values inside comparison operators.

When transforming a value for comparison, the transformation operator is nested inside the comparison operator's value, not wrapping it.

Examples:
- `%{title: %{==: %{lower: "hello"}}}` - `:lower` is inside the `==` operator's value
- `%{title: %{!=: %{upper: "HELLO"}}}` - `:upper` is inside the `!=` operator's value
- `%{title: %{not: %{==: %{lower: "hello"}}}}` - `:lower` is inside `==` which is inside `:not`

## Aggregate Function Rules

**Rule 7:** When filtering on aggregate values with a comparison operator, the aggregate function wraps the comparison operator.

Aggregate functions can wrap comparison operators to filter based on aggregate results.

When an aggregate receives a non-operator value directly, it defaults to `:==`. For example, `%{views: %{avg: 10}}` is equivalent to `%{views: %{avg: %{==: 10}}}`.

Examples:
- `%{views: %{avg: %{>: 10}}}` - `:avg` wraps the `>` operator
- `%{views: %{count: %{>: 10}}}` - `:count` wraps the `>` operator
- `%{views: %{sum: %{>: 10}}}` - `:sum` wraps the `>` operator

## Subquery and Set Comparison Rules

**Rule 8:** When used with subqueries, the `:all` operator must appear inside the value of a comparison operator, wrapping the subquery expression.

The comparison operator is the outer key and `:all` wraps the subquery value, following the same pattern as arithmetic and date/time expressions. When no comparison operator is provided, `:all` defaults to `:==`.

Examples:
- `%{id: %{>: %{all: subquery_expr}}}` - `>` wraps `:all` which wraps the subquery
- `%{id: %{all: subquery_expr}}` - `:all` wraps a subquery directly (implicit `==`)
- `%{id: %{not: %{>: %{all: subquery_expr}}}}` - `:not` wraps `>` which wraps `:all`

**Rule 9:** The `:any` operator must appear inside the value of a comparison operator, wrapping the subquery expression.

The comparison operator is the outer key and `:any` wraps the subquery value. When no comparison operator is provided, `:any` defaults to `:==`.

Examples:
- `%{id: %{>: %{any: subquery_expr}}}` - `>` wraps `:any` which wraps the subquery
- `%{id: %{any: subquery_expr}}` - `:any` wraps a subquery directly (implicit `==`)
- `%{id: %{not: %{>: %{any: subquery_expr}}}}` - `:not` wraps `>` which wraps `:any`

## Arithmetic Expression Rules

**Rule 10:** Arithmetic operators (`+`, `-`, `*`, `/`) must be used inside the value of a comparison operator.

When a comparison uses an arithmetic expression, put the arithmetic expression where the comparison operator expects a value.

Examples:
- `%{views: %{>: %{+: [:views, 10]}}}` - `+` is inside the `>` operator
- `%{views: %{==: %{*: [:views, 2]}}}` - `*` is inside the `==` operator
- `%{views: %{not: %{>: %{+: [:views, 10]}}}}` - `+` is inside the `>` which is inside `:not`

## Date/Time Expression Rules

**Rule 11:** Date/time type wrappers (`:datetime`, `:date`) must wrap an operation (`:add`, `:ago`, `:from_now`), and the entire expression must be used inside the value of a comparison operator.

When a comparison uses a date/time expression, the type wrapper contains the operation, and the whole structure appears where the comparison operator expects a value.

Examples:
- `%{inserted_at: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}` - `:datetime` wrapping `:add` is inside `>=`
- `%{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: "day"}}}}}` - `:datetime` wrapping `:ago` is inside `>`
- `%{inserted_at: %{>=: %{date: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}` - `:date` wrapping `:add` is inside `>=`
- `%{inserted_at: %{not: %{>=: %{datetime: %{add: ...}}}}}` - `:datetime` wrapping `:add` is inside `>=` which is inside `:not`

## Logical Operator Rules

**Rule 12:** When used at the field level, the `:and` operator must contain a list of comparison operators or other field-level expressions.

The `:and` operator combines multiple conditions with AND logic and must wrap a list of field-level filter entries.

Examples:
- `%{views: %{and: [>: 10, <: 20]}}` - `:and` wraps a list of operators
- `%{views: %{and: []}}` - `:and` can wrap an empty list

**Rule 13:** When used at the field level, the `:or` operator must contain a list of comparison operators or other field-level expressions.

The `:or` operator combines multiple conditions with OR logic and must wrap a list of field-level filter entries.

Examples:
- `%{published: %{or: [==: true, ==: false]}}` - `:or` wraps a list of operators
- `%{views: %{or: [>: 10, <: 5]}}` - `:or` wraps a list of operators

**Rule 14:** Top-level `:and` and `:or` operators must contain a list of filter maps or keyword lists.

When used at the top level of a filter, logical operators must wrap a list of complete filter conditions.

Examples:
- `%{or: [[published: true, views: 20], [published: false, views: 10]]}` - `:or` wraps a list of filter maps
- `%{and: [[published: true, views: 20], [title: "hello", views: 15]]}` - `:and` wraps a list of filter maps

## Binding Selector Rules

**Rule 15:** The `:bind` key must appear at the top level of a filter map or keyword list.

The `:bind` key is used to specify which binding (named or positional) a filter applies to and must be a top-level key.

Examples:
- `%{bind: %{as: %{post: %{published: true}}}}` - `:bind` is at top level
- `%{bind: %{at: %{1 => %{published: true}}}}` - `:bind` is at top level
- `%{bind: [as: [post: %{published: true}]]}` - `:bind` is at top level

**Rule 16:** Within a `:bind` key, the `:as` or `:at` selector must wrap the filter parameters.

The binding mode (`:as` for named bindings or `:at` for positional bindings) must wrap the actual filter parameters.

Examples:
- `%{bind: %{as: %{post: %{published: true}}}}` - `:as` wraps the binding target and filters
- `%{bind: %{at: %{1 => %{published: true}}}}` - `:at` wraps the positional index and filters
- `%{bind: [as: [post: %{published: true}, author: %{first_name: "example"}]]}` - `:as` wraps multiple bindings

## Schema Filter Precedence Rules

**Rule 17:** The filtering system processes `:where` before `:or_where` when both are present.

When combining AND and OR conditions at the top level, explicit `:where` filters are applied before `:or_where` filters.

Examples:
- `%{where: %{published: true}, or_where: %{published: false}}` - `:where` is processed before `:or_where`
- `%{where: [published: true, views: 10], or_where: %{views: %{<: 5}}}` - `:where` is processed before `:or_where`

**Rule 18:** Regular field filters are processed before `:or_where` filters.

When a map contains both regular field filters (implicit WHERE) and explicit `:or_where` filters, the regular fields are processed as if they were `:where` filters.

Examples:
- `%{published: true, or_where: %{published: false}}` - `published: true` is processed before `:or_where`
- `%{views: %{>: 10}, or_where: %{views: %{<: 5}}}` - `views` filter is processed before `:or_where`

**Rule 19:** Terminal filters (`:last`, `:subquery`) are processed after all other filters.

Terminal filters like `:last` and `:subquery` are applied after all other filter operations.

Examples:
- `%{published: true, limit: 10, last: 2}` - `:last` is processed after other filters
- `%{where: %{published: true}, subquery: %{id: 2}}` - `:subquery` is processed after `:where`

## Association Join Rules

**Rule 20:** Association shorthand filters must be keyword lists with optional `:as`, `:on`, and `:type` keys.

When filtering on an association using shorthand notation, the filter parameters can include binding and join configuration keys.

Examples:
- `%{author: [as: :author, first_name: "example"]}` - `:as` is a configuration key
- `%{author: [as: :author, type: :left, first_name: "example"]}` - `:type` is a configuration key
- `%{author: [first_name: "example"]}` - Configuration keys are optional

**Rule 21:** Within association shorthand, when present, the `:as`, `:on`, and `:type` keys must appear before filter field keys.

Configuration keys for the join must come before the actual filter parameters when they are included.

Examples:
- `%{author: [as: :author, on: true, first_name: "example"]}` - `:as` and `:on` come before `first_name`
- `%{author: [as: :author, type: :left, first_name: "example"]}` - `:as` and `:type` come before `first_name`

## Array Field Rules

**Rule 22:** Array field filters can use containment operators (`:in`, `:==`, `:!=`) with array-specific behavior.

Array fields support special operators that check array membership or equality.

Examples:
- `%{tags: %{in: ["elixir"]}}` - `:in` checks if array contains value
- `%{tags: %{==: ["elixir", "erlang"]}}` - `:==` checks array equality
- `%{tags: %{!=: ["elixir"]}}` - `:!=` checks array inequality

**Rule 23:** Array field filters can use the `:all` modifier to require all values in an `:in` list.

This form checks whether an array contains all specified values by wrapping `:in` with `:all`.

Examples:
- `%{tags: %{all: %{in: ["elixir", "erlang"]}}}` - `:all` wraps `:in`
- `%{tags: %{not: %{all: %{in: ["elixir", "erlang"]}}}}` - `:all` is wrapped by `:not`

## Notes on `:all`

The `:all` operator is overloaded. Its meaning is determined by context.

- When `:all` appears inside a comparison operator's value (e.g. `%{>: %{all: subquery}}`), it uses the subquery set-comparison meaning described in Rule 8.
- When `:all` is used with `:in` (e.g. `%{tags: %{all: %{in: [...]}}}`), it uses the array "contains all values" meaning described in Rule 23.

## Summary

Nesting order controls meaning.

- Wrappers like `:not`, aggregate functions, and `:bind` wrap the operator or filter they apply to.
- Comparison and matching operators like `:in`, `:==`, `:>`, `:like`, and `:ilike` wrap values.
- Value expressions like `:lower`, `:upper`, arithmetic operators, `:all`, `:any`, and date/time helpers appear inside comparison operator values.
- Logical operators like `:and` and `:or` wrap lists of conditions.
- At the same level, special keys are applied in a fixed precedence order (for example `:where` before `:or_where`, and terminal filters last).