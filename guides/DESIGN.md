# Internal Shape Design for `append_predicates`

This document defines the input shapes, rules, and examples for `append_predicates`.

## Hard rule

**Never reorder keys in a tuple.** No code may match `{a, b}` and produce `{b, a}`.

## Terminology

- **field**: A schema field name atom like `:views`, `:published`, `:title`
- **bool_op**: `:and` or `:or`
- **operator**: A comparison operator like `:>`, `:<`, `:==`, `:!=`
- **adapter_op**: A special operator like `:ids`, `:before`, `:after`, `:exists`
- **entries**: A list of sub-expressions

---

## Rules

| # | Pattern | Condition | Action |
|---|---------|-----------|--------|
| R1 | `{key, map}` | `is_map(map) and not is_struct(map)` | Convert map to keyword list: `{key, Map.to_list(map)}` |
| R2 | `{bool_op, {field, entries}}` | `bool_op in [:and, :or]` and `is_atom(field)` and `is_list(entries)` | Reduce over entries, wrap each as `{field, entry}`, merge with bool_op |
| R3 | `{key, value}` where `key` is adapter_op | `key in adapter.operators()` | Send to adapter: `build_dynamic(key, value)` |
| R4 | `{bool_op, entries}` | `bool_op in [:and, :or]` and `is_list(entries)` | Flatten keyword entries, reduce, merge with bool_op |
| R5 | `{field, value}` where `value` is keyword | `Keyword.keyword?(value)` | Produce `{:and, {field, value}}` |
| R6 | `{field, value}` | fallback | Send to adapter: `build_dynamic(field, value)` |
| R7 | `map \| keyword_list` | params collection | Iterate each `{k, v}`, call append_predicates on each |

### Rule ordering

R1 fires first (map unwrap, separate function clause). R2 matches before R3/R4 in the cond (field-scoped boolean before catch-all). R5 and R6 are the field branches inside the schema/fallback cond. R7 is the params-level catch-all (separate function clause).

---

## Examples

### Example 1: Scalar equality

User input: `%{views: 10}`
Internal shape: `{:views, 10}`
Rule: R6 → `build_dynamic(:views, 10)`

### Example 2: Operator comparison

User input: `%{views: %{>: 10}}`
After map unwrap (R1): `{:views, [>: 10]}`
R5: keyword → `{:and, {:views, [>: 10]}}`
R2: reduce over `[>: 10]` → wrap as `{:views, {:>, 10}}` → R6 → adapter

### Example 3: Multiple operators (implicit AND)

User input: `%{views: %{>: 10, <: 20}}`
After map unwrap (R1): `{:views, [>: 10, <: 20]}`
R5: keyword → `{:and, {:views, [>: 10, <: 20]}}`
R2: reduce → `{:views, {:>, 10}}` and `{:views, {:<, 20}}` → adapter
Result: `views > 10 AND views < 20` ✓

### Example 4: Field-scoped AND (explicit combiner)

User input: `%{and: %{views: [>: 10, <: 20]}}`
After map unwrap (R1): `{:and, [views: [>: 10, <: 20]]}`
Combiner: `:and`. Data: `[views: [>: 10, <: 20]]`.
R4: flatten → `[{:views, {:>, 10}}, {:views, {:<, 20}}]`
R4: reduce with `:and` → `{:views, {:>, 10}}` → adapter, `{:views, {:<, 20}}` → adapter, merge with `:and`
Result: `views > 10 AND views < 20` ✓

### Example 5: Field-scoped OR (explicit combiner)

User input (map): `%{or: %{views: [>: 10, <: 5]}}`
User input (keyword): `[or: %{views: [>: 10, <: 5]}]`
Both unwrap to: `{:or, [views: [>: 10, <: 5]]}`
Combiner: `:or`. Data: `[views: [>: 10, <: 5]]`.
R4: flatten → `[{:views, {:>, 10}}, {:views, {:<, 5}}]`
R4: reduce with `:or` → `{:views, {:>, 10}}` → adapter, `{:views, {:<, 5}}` → adapter, merge with `:or`
Result: `views > 10 OR views < 5` ✓

`:and` and `:or` are combiners. They control how the flattened entries are merged. The data inside is always flattened the same way.

### Example 6: Top-level OR

User input: `%{or: [[published: true], [published: false]]}`
After map unwrap: `{:or, [[published: true], [published: false]]}`
R4: `:or` is bool_op, value is list → reduce over list, merge with `:or`
Result: `published = true OR published = false` ✓

### Example 7: Top-level AND

User input: `%{and: [[published: true, views: 20], [title: "hello"]]}`
After map unwrap: `{:and, [[published: true, views: 20], [title: "hello"]]}`
R4: reduce over list, merge with `:and` ✓

### Example 8: Adapter operator

User input: `%{ids: [1, 2, 3]}`
Internal shape: `{:ids, [1, 2, 3]}`
R3: `:ids` in adapter.operators() → `build_dynamic(:ids, [1, 2, 3])` ✓

### Example 9: Params map

User input: `%{published: true, views: 10}`
R7: iterate → `{:published, true}` → R6, `{:views, 10}` → R6 ✓

### Example 10: Empty boolean lists

User input: `%{or: %{views: []}}`
After map unwrap: `{:or, [views: []]}`
R4: flatten → `[]` → reduce over `[]` → `dyn_left` (no-op) ✓

User input: `%{or: []}`
R4: reduce over `[]` → `dyn_left` (no-op) ✓

### Example 11: Nested OR inside top-level OR

User input: `%{or: [%{or: %{published: [==: true, ==: false]}}, %{or: %{published: [==: true, ==: false]}}]}`
R4: reduce over list, merge with `:or`
Each entry: `%{or: %{published: [==: true, ==: false]}}` → map unwrap → `{:or, [published: [==: true, ==: false]]}`
R4: flatten → `[{:published, {:==, true}}, {:published, {:==, false}}]` → reduce with `:or` → `published == true OR published == false`
Top-level merge with `:or` ✓

---

## Verification against criteria

1. **No tuple reordering** — No clause matches `{a, b}` and produces `{b, a}`. R4 flattens keyword entries inline. R5 constructs `{:and, {key, value}}` from variables. ✓
2. **C2 deleted** — The old `{field, {bool_op, entries}}` pattern match is gone. R4 with flattening handles all combiner cases. ✓
3. **C3b extended** — R4 flattens keyword entries before reducing with the combiner. R2 handles the `{field, entries}` tuple from R5. Both are inside C3b's `key in @boolean_operators` cond branch. ✓
4. **Keyword branches stay a one-liner** — R5 constructs `{:and, {key, value}}` instead of the old `{key, {:and, value}}`. ✓
5. **common_filters.ex unchanged** — The user writes `%{or: %{views: ...}}` at the top level. `build_schema_filters` keyword iteration is not affected. ✓
