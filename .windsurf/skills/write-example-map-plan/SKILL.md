---
name: write-example-map-plan
description: Guides the creation of a self-contained ExampleMapPlan that clarifies a feature or story through structured Example Mapping (Story, Rules, Examples, Questions). Use this skill when you need to define concrete examples before implementation.
---

## When to use

Use this skill when you need to clarify a feature or story before implementation. Example Mapping produces concrete, testable examples that drive design and acceptance tests. Use it when requirements are ambiguous, when a feature has many edge cases, or when you want to define behaviour specifications with concrete examples first.

## When not to use

Do **not** use this skill for implementing features directly. Use the `write-exec-plan` skill for that. Do **not** use this skill for behaviour-preserving refactors. Use the `refactor` skill for that.

## What to do

Read `.agent/EXAMPLE_MAP_PLANS.md` and load it into your context. If it already exists in context, read it again to refresh your memory. Follow it exactly.

### 1. Understand the story

State who wants what and why. Phrase it as observable behaviour from a user's perspective. Define scope boundaries including what is explicitly out of scope.

### 2. Research the codebase

Identify the key files, modules, and existing behaviour relevant to the story. Note any conventions for test structure and naming.

### 3. Write the ExampleMapPlan

Create a new `.md` file at the path specified by the `document-artifacts` rule in `.windsurf/rules/document-artifacts.md`.

Follow the skeleton and all requirements in `.agent/EXAMPLE_MAP_PLANS.md`. The plan must be self-contained.

At minimum the plan must include:

- **Purpose / Big Picture** - what the user gains and how to see it working.
- **Context and Orientation** - key files, modules, and terms defined for a novice.
- **Story** - one or two sentences in "As a / I want / So that" form.
- **Rules** - short, testable statements describing what must always be true.
- **Examples** - concrete cases with inputs and expected observable outcomes for each rule.
- **Questions** - unknowns that block correct implementation, with why each matters.
- **Mapping to acceptance tests** - how examples become tests in this repo.
- **Progress** - checklist updated as work proceeds.
- **Decision Log**, **Surprises & Discoveries**, **Outcomes & Retrospective** - living sections.

### 4. Resolve questions

For each question, attempt to answer it by reading existing code or tests. Convert resolved questions into rules or examples. Record decisions in the Decision Log.

### 5. Map examples to tests

Name the exact test files to create or update. Follow the repo's test conventions. Each rule maps to a test group. Each example maps to a scenario with explicit inputs and expected outputs.

### 6. Validate completeness

Confirm every rule has at least one example. Confirm no unresolved questions block implementation. Confirm the mapping to acceptance tests is concrete and actionable.

### 7. Close the plan

Update the **Outcomes & Retrospective** section with a summary of what was clarified, what remains uncertain, and how the examples should guide implementation.
