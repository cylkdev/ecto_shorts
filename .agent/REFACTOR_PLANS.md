# Refactoring Execution Plans (RefactorPlans)

This document describes the requirements for a refactoring execution plan ("RefactorPlan"), a design document that a coding agent can follow to safely improve code structure without changing behaviour. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single RefactorPlan file you provide. There is no memory of prior refactors and no external context.

## How to use RefactorPlans and REFACTORS.md

When authoring a RefactorPlan, follow this REFACTORS.md _to the letter_. If it is not in your context, refresh your memory by reading the entire REFACTORS.md file. Be thorough in reading (and re-reading) source material so the refactoring work remains behaviour-preserving and accurate. When creating a RefactorPlan, start from the skeleton and flesh it out as you inspect the code.

When implementing a RefactorPlan, do not prompt the user for "next steps"; simply proceed to the next milestone. Keep all sections up to date, add or split entries in the list at every stopping point to affirmatively state the progress made and next steps. Resolve ambiguities autonomously, and commit frequently.

When discussing a RefactorPlan, record decisions in a log in the plan for posterity; it should be unambiguously clear why any change to the plan was made. RefactorPlans are living documents, and it should always be possible to restart from _only_ the RefactorPlan and no other work.

When researching a refactor with challenging requirements or significant unknowns, use milestones to implement proof of concepts or small intermediate extractions that allow validating whether the proposed refactor is safe. Read the code deeply, identify the exact behaviour boundary, and include prototypes to guide a fuller implementation.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every RefactorPlan must be fully self-contained. Self-contained means that in its current form it contains all knowledge and instructions needed for a novice to succeed.
* Every RefactorPlan is a living document. Contributors are required to revise it as progress is made, as discoveries occur, and as refactoring decisions are finalized. Each revision must remain fully self-contained.
* Every RefactorPlan must enable a complete novice to perform the refactoring end-to-end without prior knowledge of this repo.
* Every RefactorPlan must produce demonstrably unchanged behaviour, not merely code changes that "look cleaner".
* Every RefactorPlan must define every term of art in plain language or do not use it.

Purpose and intent come first. Start by explaining, in a few sentences, why the refactoring matters from a maintainer's perspective: what becomes easier to read, change, test, or reason about after this change, and how to verify that behaviour did not change. Then guide the reader through the exact steps to achieve that outcome, including what to edit, what to run, and what they should observe.

The agent executing your plan can list files, read files, search, run the project, and run tests. It does not know any prior context and cannot infer what you meant from earlier milestones. Repeat any assumption you rely on. Do not point to external blogs or docs; if knowledge is required, embed it in the plan itself in your own words. If a RefactorPlan builds upon a prior RefactorPlan and that file is checked in, incorporate it by reference. If it is not, you must include all relevant context from that plan.

## Code Smells and Refactoring Techniques

This repository maintains a catalog of code smells and refactoring techniques in `.agent/refactor/`. When identifying a smell or selecting a technique, reference the appropriate file from this catalog. The catalog is organized as follows:

### Code Smells

Code smells are located in `.agent/refactor/code_smells/` and organized by category:

**Bloaters** - code that has grown too large to work with easily:
- `.agent/refactor/code_smells/bloaters/DATA_CLUMPS.md`
- `.agent/refactor/code_smells/bloaters/LARGE_MODULE.md`
- `.agent/refactor/code_smells/bloaters/LONG_FUNCTION.md`
- `.agent/refactor/code_smells/bloaters/LONG_PARAMETER_LIST.md`
- `.agent/refactor/code_smells/bloaters/PRIMITIVE_OBSESSION.md`

**Change Preventers** - code that makes changes difficult:
- `.agent/refactor/code_smells/change_preventers/DIVERGENT_CHANGE.md`
- `.agent/refactor/code_smells/change_preventers/PARALLEL_MODULE_HIERARCHIES.md`
- `.agent/refactor/code_smells/change_preventers/SHOTGUN_SURGERY.md`

**Couplers** - code with excessive coupling between modules:
- `.agent/refactor/code_smells/couplers/FEATURE_ENVY.md`
- `.agent/refactor/code_smells/couplers/INAPPROPRIATE_INTIMACY.md`
- `.agent/refactor/code_smells/couplers/MESSAGE_CHAINS.md`
- `.agent/refactor/code_smells/couplers/MIDDLE_MAN.md`

**Dispensables** - code that could be removed without loss:
- `.agent/refactor/code_smells/dispensables/COMMENTS.md`
- `.agent/refactor/code_smells/dispensables/DATA_MODULE.md`
- `.agent/refactor/code_smells/dispensables/DEAD_CODE.md`
- `.agent/refactor/code_smells/dispensables/DUPLICATE_CODE.md`
- `.agent/refactor/code_smells/dispensables/LAZY_MODULE.md`
- `.agent/refactor/code_smells/dispensables/SPECULATIVE_GENERALITY.md`

**Abstraction Abusers** - patterns that misuse abstraction mechanisms (behaviours, protocols, use/import, structs):
- `.agent/refactor/code_smells/abstraction_abusers/ALTERNATIVE_MODULES_WITH_DIFFERENT_INTERFACES.md`
- `.agent/refactor/code_smells/abstraction_abusers/REFUSED_BEQUEST.md`
- `.agent/refactor/code_smells/abstraction_abusers/SWITCH_STATEMENTS.md`
- `.agent/refactor/code_smells/abstraction_abusers/TEMPORARY_FIELD.md`

### Refactoring Techniques

Refactoring techniques are located in `.agent/refactor/techniques/` and organized by category:

**Composing Functions** - techniques for restructuring functions:
- `.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`
- `.agent/refactor/techniques/composing_functions/EXTRACT_VARIABLE.md`
- `.agent/refactor/techniques/composing_functions/INLINE_FUNCTION.md`
- `.agent/refactor/techniques/composing_functions/INLINE_TEMP.md`
- `.agent/refactor/techniques/composing_functions/REMOVE_ASSIGNMENTS_TO_PARAMETERS.md`
- `.agent/refactor/techniques/composing_functions/REPLACE_FUNCTION_WITH_MODULE.md`
- `.agent/refactor/techniques/composing_functions/REPLACE_TEMP_WITH_QUERY.md`
- `.agent/refactor/techniques/composing_functions/SPLIT_TEMPORARY_VARIABLE.md`

When writing a RefactorPlan, read the relevant smell and technique files to ensure you understand the pattern and its recommended treatment. Include a brief summary in the plan itself so the document remains self-contained.

## Formatting

Format and envelope are simple and strict. Each RefactorPlan must be one single fenced code block labeled as `md` that begins and ends with triple backticks. Do not nest additional triple-backtick code fences inside; when you need to show commands, transcripts, diffs, or code, present them as indented blocks within that single fence. Use indentation for clarity rather than code fences inside a RefactorPlan to avoid prematurely closing the RefactorPlan's code fence. Use two newlines after every heading, use `#` and `##` and so on, and correct syntax for ordered and unordered lists.

When writing a RefactorPlan to a Markdown (`.md`) file where the content of the file _is only_ the single RefactorPlan, you should omit the triple backticks.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

## Guidelines

Self-containment and plain language are paramount. If you introduce a phrase that is not ordinary English ("code smell", "behaviour boundary", "adapter", "macro hygiene"), define it immediately and remind the reader how it manifests in this repository (for example, by naming the files or commands where it appears). Do not say "as defined previously" or "according to the architecture doc." Include the needed explanation here, even if you repeat yourself.

Avoid common failure modes. Do not rely on undefined jargon. Do not describe "the letter of a refactor" so narrowly that the resulting code compiles but changes behaviour. Do not outsource key decisions to the reader. When ambiguity exists, resolve it in the plan itself and explain why you chose that path. Err on the side of over-explaining behaviour preservation and under-specifying incidental stylistic preferences.

Anchor the plan with observable outcomes. State what behaviour must remain the same, the commands to run, and the outputs they should see. Acceptance should be phrased as behaviour a human can verify ("calling `MyModule.parse/1` with input `\"a,b\"` still returns `{:ok, [\"a\", \"b\"]}`") rather than internal attributes ("added helper function"). If a change is internal, explain how its impact can still be demonstrated (for example, by running tests that fail before a mistaken change and pass after the correct refactor).

Specify repository context explicitly. Name files with full repository-relative paths, name functions and modules precisely, and describe where new files should be created. If touching multiple areas, include a short orientation paragraph that explains how those parts fit together so a novice can navigate confidently. When running commands, show the working directory and exact command line. When outcomes depend on environment, state the assumptions and provide alternatives when reasonable.

Be idempotent and safe. Write the steps so they can be run multiple times without causing damage or drift. If a step can fail halfway, include how to retry or adapt. Prefer additive, testable changes (for example, extracting a helper while keeping the original call path) before subtractive cleanups (for example, deleting duplicated code after tests pass).

Validation is not optional. Include instructions to run tests, to exercise the system if applicable, and to observe behaviour that proves the refactor did not change outcomes. Describe comprehensive testing for the area being refactored. Include expected outputs and failure signals so a novice can distinguish success from regressions. Where possible, show how to prove that the change is effective beyond compilation (for example, through a small end-to-end scenario or test output transcript). State the exact test commands appropriate to the project's toolchain and how to interpret their results.

Capture evidence. When your steps produce terminal output, short diffs, or logs, include them inside the single fenced block as indented examples. Keep them concise and focused on what proves success. If you need to include a patch, prefer file-scoped diffs or small excerpts that a reader can recreate by following your instructions rather than pasting large blobs.

## Elixir-Specific Guidelines

Prefer refactors that improve readability without changing semantics. Prefer smaller focused functions when a function is performing multiple distinct operations. Prefer pattern matching and function heads when they make branching logic clearer and preserve the same outcomes. Prefer guard clauses when they reduce nesting and preserve the same conditions.

Keep public and private APIs clear. Use `def` for public functions that are part of a module's external contract. Use `defp` for internal helpers introduced during refactoring. If moving code across functions, preserve the original return shape, error tuple conventions, raising behaviour, and side effects unless the task explicitly authorizes a behaviour change.

Be careful with bang (`!`) functions and non-bang variants. A bang function should continue to raise on failure if that is the existing behaviour. A non-bang function should continue to return error values in the repository's established style. Do not "normalize" these semantics during refactoring unless the task explicitly states that behaviour changes are allowed.

When refactoring macros or code-generation paths, treat generated behaviour as part of the behaviour boundary. Verify both compile-time and runtime behaviour if the affected code changes quoting, binding, hygiene, or generated function structure.

## Verification

Unless the repository requires different commands, use these commands from the repository root to validate refactoring work:

    mix format
    mix test path/to/changed_test.exs
    mix test

If the project uses a different command (for example, `mix ci`, `mix test --only`, or an umbrella app command from a subdirectory), state the replacement command in the RefactorPlan and explain why it is the correct command in this repository.

## Milestones

Milestones are narrative, not bureaucracy. If you break the work into milestones, introduce each with a brief paragraph that describes the scope, what will exist at the end of the milestone that did not exist before, the commands to run, and the acceptance you expect to observe. Keep it readable as a story: behaviour boundary, smell addressed, technique used, result, proof. Progress and milestones are distinct: milestones tell the story, progress tracks granular work. Both must exist. Never abbreviate a milestone merely for the sake of brevity, do not leave out details that could be crucial to preserving behaviour.

Each milestone must be independently verifiable and incrementally move the code toward the refactored state.

## Living plans and refactoring decisions

* RefactorPlans are living documents. As you make key refactoring decisions, update the plan to record both the decision and the thinking behind it. Record all decisions in the `Decision Log` section.
* RefactorPlans must contain and maintain a `Progress` section, a `Surprises & Discoveries` section, a `Decision Log`, and an `Outcomes & Retrospective` section. These are not optional.
* When you discover edge cases, hidden side effects, duplicate logic in unexpected places, or test gaps that shaped your approach, capture those observations in the `Surprises & Discoveries` section with short evidence snippets (test output is ideal).
* If you change course mid-refactor (for example, extracting a helper first instead of renaming first because tests reveal shared side effects), document why in the `Decision Log` and reflect the implications in `Progress`. Plans are guides for the next contributor as much as checklists for you.
* At completion of a major task or the full plan, write an `Outcomes & Retrospective` entry summarizing what was achieved, what remains, and lessons learned.

## Prototyping milestones and parallel implementations

It is acceptable-and often encouraged-to include explicit prototyping milestones when they de-risk a larger refactor. Examples: extracting a helper in one call site first to validate naming and return shape, or introducing a parallel implementation behind an internal switch to compare outcomes while preserving external behaviour. Keep prototypes additive and testable. Clearly label the scope as "prototyping"; describe how to run and observe results; and state the criteria for promoting or discarding the prototype.

Prefer additive code changes followed by subtractions that keep tests passing. Parallel implementations (for example, preserving the old function path while a new helper path is introduced and validated) are fine when they reduce risk or enable tests to continue passing during a large refactor. Describe how to validate both paths and how to retire one safely with tests.

## Skeleton of a Good RefactorPlan

    # <Short, action-oriented refactor description>

    This RefactorPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

    If REFACTORS.md file is checked into the repo, reference the path to that file here from the repository root and note that this document must be maintained in accordance with REFACTORS.md.

    ## Purpose / Big Picture

    Explain in a few sentences why this refactor matters and what becomes easier to read, change, or test after the refactor. State the exact behaviour that must remain unchanged and how a person can verify it.

    ## Progress

    Use a list with checkboxes to summarize granular steps. Every stopping point must be documented here, even if it requires splitting a partially completed task into two ("done" vs. "remaining"). This section must always reflect the actual current state of the work.

    - [x] (2026-02-21 10:00Z) Identified behaviour boundary for `MyApp.Parser.parse/1` and documented existing return values.
    - [ ] Extract duplicated normalization logic into a private helper while preserving all return shapes.
    - [ ] Run focused tests and full test suite; record evidence and finalize retrospective.

    Use timestamps to measure rates of progress.

    ## Surprises & Discoveries

    Document unexpected behaviours, side effects, hidden coupling, or test gaps discovered during refactoring. Provide concise evidence.

    - Observation: …
      Evidence: …

    ## Decision Log

    Record every decision made while working on the plan in the format:

    - Decision: …
      Rationale: …
      Date/Author: …

    ## Outcomes & Retrospective

    Summarize outcomes, remaining smells or follow-up opportunities, and lessons learned at major milestones or at completion. Compare the result against the original purpose and behaviour boundary.

    ## Context and Orientation

    Describe the current state relevant to this refactor as if the reader knows nothing. Name the key files and modules by full path. Define any non-obvious term you will use. Do not refer to prior plans.

    ## Behaviour Boundary (Must Remain Unchanged)

    State the exact observable behaviour that must remain unchanged during the refactor. Be concrete and specific.

    Examples:
    - `MyApp.Parser.parse/1` returns `{:ok, tokens}` for valid input and `{:error, reason}` for invalid input, with the same `reason` atoms as before.
    - `GET /health` still returns HTTP 200 with body `OK`.
    - `mix my_task` still prints the same documented output lines for the example input.

    ## Code Smell Identified

    Name the code smell from the catalog (e.g., `Long Function` from `.agent/refactor/code_smells/bloaters/LONG_FUNCTION.md`) and explain exactly where it appears (file, module, function). Explain why it makes the code harder to maintain. Include a brief summary of the smell's characteristics so the plan remains self-contained.

    ## Refactoring Technique Selected

    Name the technique(s) from the catalog (e.g., `Extract Function` from `.agent/refactor/techniques/composing_functions/EXTRACT_FUNCTION.md`) and explain why they address the smell without changing the behaviour boundary. Include a brief summary of the technique so the plan remains self-contained.

    ## Plan of Work

    Describe, in prose, the sequence of edits and additions. For each edit, name the file and location (function, module) and what to insert or change. Keep it concrete and minimal. Prefer small, behaviour-preserving steps.

    ## Concrete Steps

    State the exact commands to run and where to run them (working directory). When a command generates output, show a short expected transcript so the reader can compare. This section must be updated as work proceeds.

    ## Validation and Acceptance

    Describe how to exercise the code and what to observe to prove behaviour did not change. Phrase acceptance as behaviour, with specific inputs and outputs. If tests are involved, say "run <project's test command> and expect <N> passed; the refactor preserves the existing tests and any new guard tests pass".

    ## Idempotence and Recovery

    If steps can be repeated safely, say so. If a step is risky, provide a safe retry or rollback path. Keep the environment clean after completion.

    ## Artifacts and Notes

    Include the most important transcripts, diffs, or snippets as indented examples. Keep them concise and focused on what proves behaviour preservation and successful completion.

    ## Interfaces and Dependencies

    Be prescriptive. Name the modules, libraries, or services involved and why they matter to the refactor. Specify the public functions, callback shapes, or contracts that must remain unchanged at the end of the refactor.

    In `lib/my_app/parser.ex`, preserve:

        def parse(input) :: {:ok, [String.t()]} | {:error, atom()}

    Introduce only internal helpers (for example, `defp normalize_token/1`) unless the task explicitly requires a public API change.

    ## Milestones

    Write 2–6 milestones. Each milestone must be independently verifiable.

    For each milestone, include:
    - the goal (what behaviour is preserved, what smell is addressed)
    - the exact files you will add/change
    - the command(s) to run
    - the expected observable outcome (pass/fail signals, output snippets)

    ## Revision Note

    When you revise this RefactorPlan, add a note describing what changed and why.

If you follow the guidance above, a single, stateless agent-or a human novice-can read your RefactorPlan from top to bottom and produce a safe, observable refactor. That is the bar: SELF-CONTAINED, SELF-SUFFICIENT, NOVICE-GUIDING, BEHAVIOUR-PRESERVING.

When you revise a plan, you must ensure your changes are comprehensively reflected across all sections, including the living document sections, and you must write a note at the bottom of the plan describing the change and the reason why. RefactorPlans must describe not just what changed, but why that change is safe.