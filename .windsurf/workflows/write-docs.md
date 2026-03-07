---
description: Run this workflow after you implement a feature or make a system change. This workflow updates code-level documentation (@moduledoc, @doc, @typedoc, @spec) for changed modules and functions.
auto_execution_mode: 3
---

// turbo-all

## Goal

You will identify which modules and public functions changed, update their documentation to match the current behaviour, and verify the docs are accurate and rich.

## Requirements

- Read `.agent/DOCS.md` and follow it to the _letter_ when writing documentation. It is the single source of truth for documentation style, structure, and formatting.

- Read the files in `.agent/code-styles` and use those guidelines when writing code.

## Prerequisites

- You must be in the root directory of an Elixir Mix project.

- You must know which files were changed. If you do not know, use `git diff --name-only HEAD` to find them.

## What to do

1. **Read the documentation standard.**
   - Read `.agent/DOCS.md` in full. Internalize every formatting rule, structural requirement, and template before writing any documentation.

2. **Decide the scope.**
   - Choose either a **focused update** (only touch what changed) or a **full pass** (re-document the whole area).
   - State which scope you picked so the reviewer can see it.

3. **Identify the user-visible changes.**
   - List the modules and public APIs whose behaviour, inputs, outputs, error cases, or performance characteristics changed.
   - Include new modules and new public functions.

4. **Update module documentation for each affected module.**
   - If the module has no `@moduledoc`, add one.
   - If it already has `@moduledoc`, update it so it matches the current behaviour.
   - Every moduledoc must follow the structure from `.agent/DOCS.md`:
     - One-line summary starting with a verb.
     - Orientation paragraph (when to use, when not to, how it fits, entry points).
     - `##` section headers for each concept (Key concepts, Configuration, Error handling, etc.).
     - Inline code examples in every section - not just in a final Examples section.
     - `## Getting started` or `## Examples` with a copy-paste-able happy-path snippet.
     - `## Configuration` with options in `* `:key` (default: `val`) - effect` format.
     - `## Fields` if the module defines a public struct.
     - Edge cases and warnings using callout boxes (`> #### Title {: .warning}`).
     - Cross-references to related modules at the end.
   - Add `@moduledoc groups:` when the module has 5+ public functions.

5. **Update function documentation for each affected public function.**
   - If the function has no `@doc`, add one.
   - If it already has `@doc`, update it so it matches the current behaviour.
   - Every function doc must include:
     - One-line summary starting with a verb.
     - Argument descriptions in plain language.
     - Exact return value shapes (`{:ok, value}`, `{:error, reason}`).
     - Side effects if any.
   - Use `## Options` with the `*` format when the function takes options.
   - Add `## Examples` with at least a happy path and one edge case or error case. Use `iex>` style for short examples, plain indented blocks with comments for longer ones.
   - Add a "See also" line cross-referencing related functions.
   - For bang variants (`func!/arity`), use "Similar to `func/arity` but raises..." instead of duplicating.
   - Add `@doc group: "Category"` to categorize functions when `@moduledoc groups:` is present.
   - Use `@doc false` for public functions that are internal implementation details (e.g., `__using__/1`, internal delegation targets).

6. **Add documentation for new public surfaces.**
   - New types get a `@typedoc` and a `@type`. For types with multiple states or shapes, document each variant.
   - New macros get docs describing what they expand to and any hygiene constraints.
   - New callbacks get docs stating when the runtime invokes the callback, every possible return value and what it causes, and an implementation example.

7. **Verify the documentation.**
   - Add a short, concrete way to verify the docs match reality.
   - Prefer pointing at an existing test or a command that demonstrates the behaviour.
   - If you add a new doc example, ensure it is correct and runnable if the project's doc tooling checks examples.

8. **Keep changes small and reviewable.**
   - In a focused update, do not rewrite unrelated docs.
   - If you notice unrelated doc debt, record it as a follow-up instead of bundling it into this change.

9. **Final consistency pass.**
   - Ensure names, terms, and examples match the code.
   - Ensure the docs describe the current public API and do not mention removed options or old behaviour.
   - Verify these structural requirements from `.agent/DOCS.md`:
     - Every moduledoc has `##` section headers.
     - Every section contains at least one inline code example.
     - Every option uses the `*` format: `* `:key` (default: `val`) - effect`.
     - Every function has a "See also" or cross-reference to related functions.
     - All bullet lists use `*` as the prefix, never `-`.
     - Complex public types have `@typedoc`.
     - Internal public functions use `@doc false`.
     - Modules with 5+ public functions use `@doc group:` and `@moduledoc groups:`.
