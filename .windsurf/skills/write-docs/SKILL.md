---
name: write-docs
description: Creates or updates project documentation after a code change. It writes module, function, type, macro, and callback docs in the project's standard style, limited to the changed surface area unless a full pass is requested. It also adds a short "how to verify" note so a reviewer can confirm the docs match the code. Use this skill after implementing a feature or making a change to the codebase.
---

## When to use

Use this skill after you implement a feature, change behavior, or refactor code in a way that affects how someone uses a module or function. This covers `@moduledoc`, `@doc`, `@typedoc`, `@spec`, and `@callback` documentation.

## When not to use

Do **not** use this skill for architecture decision records. Use the `write-adr` skill for that.

## Requirements

Read `.agent/DOCS.md` and load it into your context. If it already exists in context, read it again to refresh your memory. Follow it exactly.

## What to do

### 1. Decide the scope

Choose either a **focused update** (only touch what changed) or a **full pass** (re-document the whole area). Write down which scope you picked so the reviewer can see it.

### 2. Identify user-visible changes

List the modules and public APIs whose behavior, inputs, outputs, error cases, or performance characteristics changed. Include new modules and new public functions.

### 3. Update module documentation

For each affected module: if it has no `@moduledoc`, add one. If it already has `@moduledoc`, update it to match the current behavior. Describe what the module is for, what it does that a caller can observe, and how it fits into the surrounding system if that matters to correct usage.

### 4. Update function documentation

For each affected public function: if it has no `@doc`, add one. If it already has `@doc`, update it to match the current behavior. Document inputs, outputs, and the important cases a caller must handle.

### 5. Document new public surfaces

- New types get a `@typedoc` and a `@type`.
- New macros get docs describing what they expand to and any hygiene constraints.
- New callbacks or behaviors get docs describing the contract, when it is called, and what happens if it fails.

### 6. Include a verification note

Add a short, concrete way to verify the docs match reality. Prefer pointing at an existing test or a command that demonstrates the behavior.

### 7. Final consistency pass

Ensure names, terms, and examples match the code. Ensure the docs describe the current public API and do not mention removed options or old behavior.
