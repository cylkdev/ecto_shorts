# Extract Compound Operations and Transaction Helpers from Actions

This RefactorPlan is a living document. The sections Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agent/REFACTOR_PLANS.md`.

## Purpose / Big Picture

`EctoShorts.Actions` at 1956 lines contains five distinct responsibility groups beyond the CRUD primitives: compound find-and-X operations, transaction wrappers, batch delegation, bulk delegation, and multi delegation. The compound operations and transaction helpers are the last two groups with significant internal logic that have not been extracted. After this refactor, compound operations move to `Actions.Compound` and transaction private helpers move to `Actions.Transaction`. The Multi delegation pattern is also simplified by extracting a shared `run_multi/4` helper. The result is a thinner `Actions` module focused on CRUD primitives and thin delegators.

To verify behavior is preserved, run `mix test --seed 0` from the repository root. All 898 tests and 9 doctests must pass.

## Progress

- [x] (2026-03-02 05:45Z) Milestone 1: Attempted extraction of `Actions.Compound`. Reverted - cyclic call risk (child module calling parent). Compound functions are thin CRUD compositions that belong inline.
- [x] (2026-03-02 05:50Z) Milestone 2: Extracted `Actions.Transaction` with `normalize_transaction_response/2`, `eval_transaction_fun/3`, `run_transaction/2`, and private helpers `call_transaction_fun/2`, `maybe_rollback/2`. 898 tests pass.
- [x] (2026-03-02 05:55Z) Milestone 3: Extracted `run_multi/2` private helper replacing 6 identical 3-line multi delegation patterns. 898 tests pass.
- [x] (2026-03-02 06:00Z) Milestone 4: Final validation. `mix test --seed 0`: 9 doctests, 898 tests, 0 failures. `mix credo --strict`: no new warnings.

## Surprises & Discoveries

- Observation: Extracting compound `find_and_*` functions to `Actions.Compound` created a cyclic call risk. The child module (`Actions.Compound`) called back into the parent module (`Actions.find`, `Actions.create`, etc.). This violates the principle that child modules should only depend on lower-level modules, not their parent.
  Evidence: User identified the issue during code review.

## Decision Log

- Decision: Revert `Actions.Compound` extraction. Keep compound functions inline in `actions.ex`.
  Rationale: Compound functions are thin compositions (1-4 lines each) that call `find`, `create`, `update`, `delete` - all defined in the same module. Extracting them to a child module would require the child to call back into the parent, creating a cyclic dependency risk. The functions are too thin to justify the indirection.
  Date/Author: 2026-03-02 / Cascade

- Decision: Extract `Actions.Transaction` for transaction private helpers.
  Rationale: Transaction helpers (`normalize_transaction_response`, `eval_transaction_fun`, `call_transaction_fun`, `maybe_rollback`) only depend on `Config` and the `repo` module. No cyclic call risk. The helpers are self-contained and reduce the private function count in `actions.ex`.
  Date/Author: 2026-03-02 / Cascade

- Decision: Extract `run_multi/2` as a private helper rather than a separate module.
  Rationale: The 3-line pattern (build multi, transaction, handle response) is used 6 times. A private helper eliminates the duplication without introducing a new module or cyclic risk.
  Date/Author: 2026-03-02 / Cascade

## Outcomes & Retrospective

The refactor achieved two of its three original goals. The Transaction helpers were successfully extracted, and the Multi delegation pattern was deduplicated. The Compound extraction was correctly reverted after identifying cyclic call risk.

Line count change: `actions.ex` went from 1956 to 1901 lines (-55). New `actions/transaction.ex` is 53 lines. Net reduction: -2 lines, but the code is better organized with fewer private helpers in the main module.

The six Multi functions each went from 3 lines to 1 line of body code via the shared `run_multi/2` helper.

Follow-up: The compound functions remain inline as they should. Further size reduction of `actions.ex` would require splitting the documentation, which is a different kind of change.

## Context and Orientation

`lib/ecto_shorts/actions.ex` is the public API module for EctoShorts. It currently has 1956 lines (approximately 700 lines of code and 1250 lines of documentation). Previous refactors extracted `Actions.Multi`, `Actions.Batch`, and `Actions.Bulk`. This pass targets the remaining internal logic groups.

The compound operations (`find_and_create`, `find_and_update`, `find_and_upsert`, `find_and_delete`, `find_or_create`) are compositions of the CRUD primitives (`find`, `create`, `update`, `delete`). They have their own doc group but live interleaved with the primitives.

The transaction helpers (`normalize_transaction_response`, `eval_transaction_fun`, `call_transaction_fun`, `maybe_rollback`) are only used by `transact/2`.

The six Multi functions all follow an identical 3-line pattern: build multi, run transaction, handle response.

## Behavior Boundary (Must Remain Unchanged)

All public functions in `EctoShorts.Actions` retain their exact signatures, return shapes, and error handling. All 898 tests and 9 doctests pass.

## Code Smell Identified

1. **Large Module** (`.agent/refactor/code_smells/bloaters/LARGE_MODULE.md`): `actions.ex` at 1956 lines has multiple responsibility groups that change for different reasons.

2. **Duplicate Code** (`.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`): The six Multi functions follow an identical 3-line pattern.

3. **Divergent Change** (`.agent/refactor/code_smells/change_preventers/DIVERGENT_CHANGE.md`): Compound operations and transaction helpers change independently of CRUD primitives.

## Refactoring Technique Selected

1. **Extract Module**: Move compound operations to `Actions.Compound` and transaction helpers to `Actions.Transaction`.
2. **Extract Function**: Create `run_multi/4` to replace the repeated 3-line multi pattern.

## Plan of Work

See milestones below.

## Validation and Acceptance

Run `mix test --seed 0` and observe 9 doctests, 898 tests, 0 failures after every milestone.

## Milestones

### Milestone 1: Extract Actions.Compound

Create `lib/ecto_shorts/actions/compound.ex` with the internal logic of `find_and_create`, `find_and_update`, `find_and_upsert`, `find_and_delete`, `find_or_create`. Keep public API in `actions.ex` as delegators.

### Milestone 2: Extract Actions.Transaction

Create `lib/ecto_shorts/actions/transaction.ex` with `normalize_transaction_response`, `eval_transaction_fun`, `call_transaction_fun`, `maybe_rollback`. Keep `transaction/2` and `transact/2` as public API in `actions.ex`.

### Milestone 3: Deduplicate Multi delegation

Extract a private `run_multi/4` helper that encapsulates the build-transaction-handle pattern used by all 6 Multi functions.

### Milestone 4: Final validation

Run `mix format`, `mix test --seed 0`, `mix credo --strict`, `mix dialyzer`.
