# Plan Exact Code Changes Before Editing Code

You must avoid surprises.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

Before you change any code, write a plan that shows the exact code you intend to add, remove, or modify.

Your plan must include code examples that match the real files and module names in the codebase.

Your plan must show the final code shape you intend to produce, not a vague sketch.

One coordinator owns the plan. The coordinator decides when the boundary, the intended result, and the candidate file set are stable enough to choose an approach.

After the goal and boundaries are stable, the coordinator may fan out bounded worker passes to inspect candidate files, nearby callers, tests, or interface seams. A worker pass may gather facts, but it must not choose the final approach on its own.

Collect worker-pass results back into the plan before you choose an approach or edit code. Do not start editing from half-collected evidence.

Use the code examples in your plan to make boundaries explicit.

A boundary is where one module hands work to another module, or where a public API is exposed to callers.

For each boundary in your plan, state which module owns the responsibility and which module is only a caller.

You must consider at least two approaches before choosing one.

For each approach, describe what changes you would make and what tradeoff you accept.

Then choose one approach and show its exact code changes in the plan.

After you write the plan, implement only what the plan shows.

If you discover the plan is wrong while implementing, stop and update the plan before making further changes.
