## Goals

Each directive and each operator must have full defined behaviours through example mapping.
Together, the examples must show every valid data shape this API accepts. In other words,
every possible behaviour that this api has should be covered.

## Task and Key Files

Active task: Define array-field behaviour through example mapping so every distinct accepted data shape and operator combination is covered explicitly.

Key files:
- `PLAN.md`
- `research/COMMON_FILTERS.md`
- `test/examples/ecto_query_dsl.exs`

Document updates to track in the same change:
- Create or refresh the active ExampleMappingDoc, BehaviourSpecDoc, or ExecPlan when the work hands off to one of those documents.
- Keep `PLAN.md` and every companion planning document in sync as the task, proof path, or ownership changes.

## Milestones

Keep the Milestones section up to date as you work. Milestones track your larger portions of work, including required document creation and companion-document sync when the task changes shape.

### Array Fields

Status: Incomplete

- [ ] `:in`
- [ ] `:all`
- [ ] `:count`
- [ ] Comparison operators work element-wise
- [ ] String matching (like, ilike) works on array elements
- [ ] Transformations (lower, upper) work on array elements

Validation:

- [ ] Has an example for each directive and operator
- [ ] Has a rule statement for all possible distinct data shapes
- [ ] Has a rule statement for all possible distinct operator combinations
- [ ] Has a rule statement for all possible distinct field types

## Progress

Keep the progress section up to date as you work.

**Legend:**
[ ] not started
[~] in progress
[x] completed
 
- [ ] Create or refresh the active planning document and keep its `Task and Key Files` section current.
- [ ] Keep `PLAN.md` and every required companion planning document in sync with the current task.
- [ ] Record the next concrete behaviour, example, or proof step here.
