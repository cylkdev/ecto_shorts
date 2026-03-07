# Write Elixir Code Documentation

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose

Use this skill to write or review Elixir code documentation in this repository so it matches the project's documentation standard and stays predictable for beginners.

## Goal

Produce module, function, callback, and type documentation that follows the repo's rules, describes caller-visible behaviour completely, and includes any required supporting annotations.

## When to use

- The task is to add, rewrite, polish, or review Elixir `@moduledoc`, `@doc`, `@typedoc`, `@callback`, `@spec`, `@doc false`, `@doc group:`, or `@moduledoc groups:` content in this repo.
- The task requires documenting a public API, public type, public callback, or a public-but-internal function that should be hidden from generated docs.
- The task requires checking whether existing docs are complete enough for a beginner to use the API correctly on the first try.

## When not to use

- The task is to write README content, ADRs, comments, changelogs, or general prose outside Elixir code docs.
- The task is to design the API itself rather than document an existing or already-specified API boundary.
- The task is purely mechanical formatting with no documentation judgment.

## Inputs

- The target module, function, callback, type, or group of related public APIs.
- The surrounding public API in the same file or neighboring modules so the boundary and cross-references are accurate.
- The authoritative style guide at `.agent/styles/documentation/How to Write Module and Function Documentation.md`.
- A relevant example asset from `.agent/skills/documentation/assets/examples/` when the documentation pattern is non-trivial.

## Outputs

- Updated Elixir code documentation that follows the repo standard.
- Any supporting annotations required by the documented boundary, such as `@typedoc`, `@spec`, `@doc false`, `@doc group:`, or `@moduledoc groups:`.
- Examples and cross-references that let a beginner predict the public behaviour without reading the implementation.

## What to do

1. Open `.agent/styles/documentation/How to Write Module and Function Documentation.md` before drafting anything.
2. Read the target code and the nearby public API so you understand what callers can observe and which related functions or modules should be referenced.
3. Decide which documentation artifacts are required:
   `@moduledoc`, `@doc`, `@typedoc`, `@callback`, `@spec`, `@doc false`, `@doc group:`, or `@moduledoc groups:`.
4. If the pattern is not trivial, open the closest example asset from `.agent/skills/documentation/assets/examples/` before writing.
5. Draft documentation from the caller boundary, not the implementation:
   purpose, accepted input shapes, return and error shapes, raises, side effects, ordering or timeout behaviour, options, warnings, examples, and cross-references.
6. For modules, organize the `@moduledoc` with `##` section headers and include inline code examples throughout the sections, not only at the end.
7. For functions and callbacks, start with a one-line summary, describe the contract in prose, add `## Options` when options exist, add `## Examples`, and include a related cross-reference.
8. For callbacks, state when the runtime invokes them, from which context, and what each return value causes the runtime or caller to do.
9. Add or preserve `@spec` for public functions and callbacks so input and output shapes are explicit.
10. For public types that are not immediately obvious, add `@typedoc` that explains each state or variant in caller terms.
11. Before finishing, validate the draft against the style guide's structural rules and its "Done means fully described" checklist.

## Decision rules

- Write short docs only for simple accessors, predicates, or thin delegations where 2 to 5 lines fully describe the caller-visible behaviour.
- Write rich, sectioned docs when the module or function has multiple input shapes, options, edge cases, side effects, concurrency or timeout behaviour, subtle semantics, or anything that could surprise a caller.
- Add `@typedoc` when a public type has multiple states, variants, or meanings that are not obvious from the type definition alone.
- Add `@spec` when a public function or callback is missing an explicit contract for its input and output shapes.
- Add `@doc false` when a function is public only as an implementation detail and callers should not rely on it.
- Add `@moduledoc groups:` and matching `@doc group:` annotations when a module has five or more public functions.
- For bang variants such as `func!/arity`, use the "Similar to `func/arity` but ..." pattern instead of duplicating the full non-bang doc unless the bang variant has materially different behaviour.
- Open example assets by category:
  - General Ecto documentation patterns: `.agent/skills/documentation/assets/examples/ecto.md`
  - Query and repo APIs: `.agent/skills/documentation/assets/examples/query.md`, `.agent/skills/documentation/assets/examples/queryable.md`, `.agent/skills/documentation/assets/examples/repo.md`
  - Changeset-heavy APIs: `.agent/skills/documentation/assets/examples/changeset.md`
  - Callback or transaction contracts: `.agent/skills/documentation/assets/examples/transaction.md`
  - Channel or socket callback-heavy docs: `.agent/skills/documentation/assets/examples/channel.md`, `.agent/skills/documentation/assets/examples/socket.md`
- If the target involves logging, storage, code reloading, or similar runtime behaviour, inspect `.agent/skills/documentation/assets/examples/logger.md`, `.agent/skills/documentation/assets/examples/storage.md`, or `.agent/skills/documentation/assets/examples/code_reloader.md` if one of them is the closest match.

## Constraints

- Treat `.agent/styles/documentation/How to Write Module and Function Documentation.md` as the source of truth.
- Document only caller-visible behaviour. Do not write implementation walkthroughs.
- Do not omit observable effects such as errors, raises, logs, messages, database writes, file IO, telemetry, retries, ordering, timeouts, or concurrency behaviour when callers can notice them.
- Every `@moduledoc` must use `##` section headers and include inline code examples in the relevant sections.
- Use `*` bullets inside Elixir docstrings, never `-`.
- Format option bullets exactly as `* \`:key\` (default: \`value\`) - effect` when a default exists, or `* \`:key\` - effect` when it does not.
- Function and callback docs must state concrete return or error behaviour and must include a related cross-reference such as "See also `other_func/2`."
- Keep vocabulary consistent with surrounding docs and define non-obvious terms before relying on them.
- Do not guess at behaviour. Read the code and nearby public API until the boundary is clear.

## Validation

- Confirm the skill user opened `.agent/styles/documentation/How to Write Module and Function Documentation.md` and used it to drive the draft.
- Confirm the output includes the right documentation artifacts for the target: `@moduledoc`, `@doc`, `@typedoc`, `@callback`, `@spec`, `@doc false`, `@doc group:`, and `@moduledoc groups:` where applicable.
- Confirm a beginner could answer all of these from the docs alone:
  when to use it, valid inputs, return and error shapes, side effects, and how to verify behaviour from examples.
- Confirm every `@moduledoc` has `##` headers and inline examples.
- Confirm every option list uses the exact required bullet format.
- Confirm every function or callback doc includes a related cross-reference.
- Confirm callbacks state invocation context and return-driven runtime behaviour.
- Confirm public functions and callbacks have `@spec` where the boundary needs an explicit contract.
- Confirm the docs do not leave out relevant ordering, timeout, retry, or concurrency behaviour when those behaviours are user-observable.

## Success criteria

- The resulting docs follow the repo style guide without requiring outside interpretation.
- The resulting docs describe the public boundary completely enough for a beginner to call the API correctly.
- Any required supporting annotations such as `@typedoc`, `@spec`, `@doc false`, `@doc group:`, or `@moduledoc groups:` are present.
- The writer can point to the relevant example asset used for non-trivial patterns.

## Examples

### Example 1

**Input:** A public module that exposes several filtering helpers and currently has no `@moduledoc` or function grouping.

**Action:** Open `.agent/styles/documentation/How to Write Module and Function Documentation.md`, read the module and its public entry points, inspect `.agent/skills/documentation/assets/examples/query.md` and `.agent/skills/documentation/assets/examples/repo.md`, then write a `@moduledoc` with a one-line summary, `##` sections, inline examples, shared options, warnings, and `@moduledoc groups:` plus matching `@doc group:` annotations.

**Output:** A module doc set that explains when to use the filtering API, what each grouped function category does, how options change caller-visible behaviour, and how to start with the happy path from a runnable example.

### Example 2

**Input:** A public `run/2` function with options and a `run!/2` bang variant that raises on failure.

**Action:** Open the style guide, document `run/2` with a summary, prose contract, exact return and error shapes, `## Options`, `## Examples`, and a "See also" cross-reference. Then document `run!/2` with the "Similar to `run/2` but raises ..." pattern instead of duplicating the full contract.

**Output:** A pair of docs where `run/2` fully describes success, failure, side effects, and options, while `run!/2` stays concise and clearly states which exception behaviour differs from the non-bang version.

## Common mistakes

- Starting to write before opening `.agent/styles/documentation/How to Write Module and Function Documentation.md`.
- Explaining how the code works internally instead of what the caller can observe.
- Omitting accepted input shapes, return tuples, raise conditions, or side effects.
- Writing a `@moduledoc` without `##` section headers or without inline examples.
- Using `-` bullets inside Elixir docstrings instead of `*`.
- Forgetting `@typedoc`, `@doc false`, `@doc group:`, or `@moduledoc groups:` when the style guide requires them.
- Duplicating a full non-bang doc for a bang variant instead of using the "Similar to" pattern.
- Skipping a relevant example asset when the documentation pattern is non-trivial.
