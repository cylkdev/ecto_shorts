---
trigger: always_on
---

After implementing a feature or making a change to the codebase, use the documentation workflow to update the documentation.

## Documentation Workflow

1. Read `.agent/DOCS.md` and load it into your context. If it already exists read it again and refresh your memory.

2. Decide the scope before writing anything.

Choose either a focused update that only touches what changed, or a full pass that re-documents the whole area. Write down which scope you picked so the reviewer can see it.

2. Identify the user-visible changes.

List the modules and public APIs whose behavior, inputs, outputs, error cases, or performance characteristics changed. Include new modules or new public functions.

3. Update module documentation for each affected module.

If the module has no `@moduledoc`, add one.
If it already has `@moduledoc`, update it so it matches the current behavior.
Describe what the module is for, what it does that a user can observe, and how it fits into the surrounding system if that matters to correct usage.

4. Update function documentation for each affected public function.

If the function has no `@doc`, add one.
If it already has `@doc`, update it so it matches the current behavior.
Document inputs, outputs, and the important cases a caller must handle. Prefer describing the full returned shape and meaning over repeating field-by-field assertions.

5. Add documentation for new public surfaces.

If you introduced a new type, add a `@typedoc` and a `@type`.
If you introduced a new macro, document what it expands to from a caller’s point of view and any hygiene or quoting constraints a caller must respect.
If you introduced a new callback or behavior, document the contract, when it is called, and what happens if it fails.

6. Include a verification note.

Add a short, concrete way to verify the docs match reality. Prefer pointing at an existing test or a command that demonstrates the behavior. If you add a new doc example, ensure it is correct and runnable if the project’s doc tooling checks examples.

7. Keep changes small and reviewable.

Do not rewrite unrelated docs in a focused update. If you notice unrelated doc debt, record it as a follow-up instead of bundling it into this change.

8. Do a final consistency pass.

Ensure names, terms, and examples match the code. Ensure the docs describe the current public API and do not mention removed options or old behavior.