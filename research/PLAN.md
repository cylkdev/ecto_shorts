# Rule Consistency Checklist

## Summary
Scope is limited to [research/COMMON_FILTERS.md](/Users/kurthogarth/Documents/GitHub/ecto_shorts/research/COMMON_FILTERS.md) and [test/examples/ecto_query_dsl.exs](/Users/kurthogarth/Documents/GitHub/ecto_shorts/test/examples/ecto_query_dsl.exs). The checklist below locks the chosen resolution for every confirmed mismatch so an implementer can update the two files without making any further decisions.

## Locked Checklist
- `[x]` Join `on` semantics: make `COMMON_FILTERS.md` match the relational `on:` clauses already shown in the executable examples.
- `[x]` Multiple-binding preload duplicates: deduplicate identical resolved preloads; the executable example should emit one effective preload.
- `[x]` `with_cte` `operation: :all`: keep it as a supported key; the executable example/test must include it if the rule supports it.
- `[x]` Dynamic equality operator: standardize on `==` in both files.
- `[x]` `exists` / `not exists` outer binding: make `as: :post` explicit whenever the subquery depends on `parent_as(:post)`.
- `[x]` Bare `prepend_order_by`: default to `asc`; update the executable example/test to match.
- `[x]` `after` / `before`: treat them as raw ID filters (`id > value`, `id < value`); update the executable examples/tests to match.
- `[x]` `union` / `union_all`: express them as macro/pipeline composition from the schema itself, not `base_query = from(...)`; choose distinct example data so the set behavior is observable.
- `[x]` Missing terminal-filter rule: add a full rule for `[published: true, subquery: [id: 2]]`.
- `[x]` `:date` wrapper: keep it semantically distinct from `:datetime`; the executable side must show date-only behavior.
- `[x]` Aggregate `count` equality: standardize on `count(...) == 0`; update the executable example/test to match.
- `[x]` Array comparison operators: use field-side semantics (`tag > value`, not `value > tag`); update the executable side to match.
- `[x]` Array `like` / `ilike`: keep them always pattern-based; exact overlap belongs to `in`, not `like`.
- `[x]` Negated membership/list rules: normalize to direct `in` / `not in` forms where possible.

## Implementation Changes
- Update the executable example/test file first for items that intentionally change current executable behavior: `operation: :all`, bare `prepend_order_by`, `after` / `before`, `:date`, aggregate `count == 0`, array comparison direction, and array `like` / `ilike`.
- Update `COMMON_FILTERS.md` to mirror the locked behavior for join `on:` clauses, explicit outer bindings for `exists`, the added terminal-filter rule, normalized negated membership, and the set-operator composition wording.
- For `union` / `union_all`, keep the test structure pipeline-based from the schema and change the example data so the set operation proves something nontrivial.

## Verification
- Re-check every rule/example pair by matching each markdown rule statement to exactly one executable example.
- Confirm there are no remaining cases where the example input, prose meaning, and emitted query disagree.
- Confirm every shorthand rule is self-contained for a beginner and does not rely on hidden context.

## Assumptions
- Only the two allowed files are in scope.
- The goal is a single beginner-readable contract; when a rule name implies behavior, the executable example should follow that behavior rather than rely on surprising SQL translations.
