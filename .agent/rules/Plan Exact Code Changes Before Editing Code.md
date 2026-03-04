# Plan exact code changes before editing code

You must avoid surprises.

Before you change any code, you must write a plan that shows the exact code you intend to add, remove, or modify.

Your plan must include code examples that match the real files and module names in the codebase.

Your plan must show the final code shape you intend to produce, not a vague sketch.

After you write the plan, you must implement only what the plan shows.

If you discover the plan is wrong while implementing, you must stop and update the plan before making further changes.

Use the code examples in your plan to make boundaries explicit.

A boundary is where one module hands work to another module, or where a public API is exposed to callers.

For each boundary in your plan, state which module owns the responsibility and which module is only a caller.

You must consider at least two approaches before choosing one.

For each approach, describe what changes you would make and what tradeoff you accept.

Then choose one approach and show its exact code changes in the plan.