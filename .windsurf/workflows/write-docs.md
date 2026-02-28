---
description: Run this workflow after you implement a feature or make a system change. This workflow updates code-level documentation (@moduledoc, @doc, @typedoc, @spec) for changed modules and functions.
auto_execution_mode: 3
---

// turbo-all

## Goal

You will identify which modules and public functions changed, update their documentation to match the current behaviour, and verify the docs are accurate.

## Requirements

- Read `.agent/DOCS.md` and follow it to the _letter_ when writing documentation.

- Read the files in `.agent/code-styles` and use those guidelines when writing code.

## Prerequisites

- You must be in the root directory of an Elixir Mix project.

- You must know which files were changed. If you do not know, use `git diff --name-only HEAD` to find them.

## What to do

1. Decide the scope.
   - Choose either a **focused update** (only touch what changed) or a **full pass** (re-document the whole area).
   - State which scope you picked so the reviewer can see it.

2. Identify the user-visible changes.
   - List the modules and public APIs whose behaviour, inputs, outputs, error cases, or performance characteristics changed.
   - Include new modules and new public functions.

3. Update module documentation for each affected module.
   - If the module has no `@moduledoc`, add one.
   - If it already has `@moduledoc`, update it so it matches the current behaviour.
   - Describe what the module is for, what it does that a caller can observe, and how it fits into the surrounding system if that matters to correct usage.

4. Update function documentation for each affected public function.
   - If the function has no `@doc`, add one.
   - If it already has `@doc`, update it so it matches the current behaviour.
   - Document inputs, outputs, and the important cases a caller must handle.
   - Prefer describing the full returned shape and meaning over repeating field-by-field assertions.

5. Add documentation for new public surfaces.
   - New types get a `@typedoc` and a `@type`.
   - New macros get docs describing what they expand to and any hygiene constraints.
   - New callbacks or behaviours get docs describing the contract, when it is called, and what happens if it fails.

6. Include a verification note.
   - Add a short, concrete way to verify the docs match reality.
   - Prefer pointing at an existing test or a command that demonstrates the behaviour.
   - If you add a new doc example, ensure it is correct and runnable if the project's doc tooling checks examples.

7. Keep changes small and reviewable.
   - In a focused update, do not rewrite unrelated docs.
   - If you notice unrelated doc debt, record it as a follow-up instead of bundling it into this change.

8. Do a final consistency pass.
   - Ensure names, terms, and examples match the code.
   - Ensure the docs describe the current public API and do not mention removed options or old behaviour.
