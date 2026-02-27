# RefactorDocs (Refactor Specification Documents)

This document describes the requirements for a refactoring document ("RefactorDoc"), a document that a coding agent can follow to safely improve code structure without changing behaviour. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single RefactorDoc file you provide. There is no memory of prior refactors and no external context.

## How to use RefactorDocs and REFACTORS.md

When authoring a refactoring document (RefactorDoc), follow this REFACTORS.md _to the letter_. If it is not in your context, refresh your memory by reading the entire REFACTORS.md file. Be thorough in reading (and re-reading) source material so the refactoring work remains behaviour-preserving and accurate. When creating a RefactorDoc, start from the skeleton and flesh it out as you inspect the code.

When implementing a RefactorDoc, do not prompt the user for "next steps" if the document already describes the next action. Simply proceed to the next milestone or step. Keep all sections up to date, and revise the progress entries at every stopping point so the current state is unambiguous. Resolve minor ambiguities autonomously when the document provides enough information to do so safely.

When discussing a refactoring document (RefactorDoc), record decisions in a log in the document for posterity; it should be unambiguously clear why a refactoring choice was made, what behaviour was preserved, and what evidence was used to validate the result. RefactorDocs are living documents, and it should always be possible to restart from _only_ the RefactorDoc and no other work.

When refactoring code with meaningful uncertainty (for example, complex conditionals, macro-heavy code, or performance-sensitive paths), use milestones to create safe proof-of-concept changes or small intermediate extractions that allow you to validate behaviour before proceeding. Read the code deeply, identify the exact behaviour boundary, and include observable verification that proves the code still behaves the same after each change.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every RefactorDoc must be fully self-contained. Self-contained means that in its current form it contains all knowledge and instructions needed for a novice to succeed.
* Every RefactorDoc is a living document. Contributors are required to revise it as progress is made, as discoveries occur, and as refactoring decisions are finalized. Each revision must remain fully self-contained.
* Every RefactorDoc must enable a complete novice to perform the refactoring end-to-end without prior knowledge of this repo.
* Every RefactorDoc must preserve a clearly defined observable behaviour boundary. Refactoring is not feature work unless the task explicitly says otherwise.
* Every RefactorDoc must define every term of art in plain language or do not use it.

Purpose and intent come first. Begin by explaining, in a few sentences, why the refactoring matters from a maintainer's perspective: what becomes easier to read, change, test, or reason about after this change, and how to verify that behaviour did not change. Then guide the reader through the exact steps to achieve that outcome, including what to edit, what to run, and what they should observe.

The agent executing your refactor document can list files, read files, search, run the project, and run tests. It does not know any prior context and cannot infer what you meant from earlier work. Repeat any assumption you rely on. Do not point to external blogs or docs for required knowledge; if knowledge is required to execute the refactor safely, embed it in the RefactorDoc itself in your own words. If a RefactorDoc builds upon a prior checked-in RefactorDoc and that file exists in the repository, incorporate it by reference. If it is not available, include all relevant context directly in the current document.

## Formatting

Format and envelope are simple and strict. Each RefactorDoc must be one single fenced code block labeled as `md` that begins and ends with triple backticks when shown in chat. Do not nest additional triple-backtick code fences inside; when you need to show commands, transcripts, diffs, or code, present them as indented blocks within that single fence. Use indentation for clarity rather than code fences inside a RefactorDoc to avoid prematurely closing the RefactorDoc fence. Use two newlines after every heading, use `#`, `##`, and so on, and correct syntax for ordered and unordered lists.

When writing a RefactorDoc to a Markdown (`.md`) file where the content of the file _is only_ the single RefactorDoc, omit the triple backticks.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

## Guidelines

Self-containment and plain language are paramount. If you introduce a phrase that is not ordinary English (for example, "code smell", "behaviour boundary", "adapter", "macro hygiene", or "side effect"), define it immediately and remind the reader how it appears in this repository (for example, by naming the relevant files, modules, or commands). Do not say "as defined previously" or "see another document." Include the needed explanation here, even if that requires repeating yourself.

Avoid common failure modes. Do not rely on undefined jargon. Do not describe the "shape" of a refactor so narrowly that the result compiles but changes behaviour. Do not outsource key refactoring decisions to the reader when the document can resolve them safely. When ambiguity exists, resolve it in the RefactorDoc itself and explain why that path is safer. Err on the side of over-explaining behaviour preservation and under-specifying incidental stylistic preferences.

Anchor the refactoring work with observable outcomes. State what behaviour must remain the same, the commands to run, and the outputs the reader should see before and after the refactor. Acceptance should be phrased as behaviour a human can verify ("calling `MyModule.parse/1` with input `\"a,b\"` still returns `{:ok, [\"a\", \"b\"]}`") rather than internal attributes ("added helper function"). If the refactor is internal, explain how its safety is still demonstrated (for example, by running focused tests that fail before a mistaken change and pass after the correct refactor, or by showing a small end-to-end scenario).

Specify repository context explicitly. Name files with full repository-relative paths, name functions and modules precisely, and describe where new helper functions or modules should be created if needed. If touching multiple areas, include a short orientation paragraph explaining how those parts fit together so a novice can navigate confidently. When running commands, show the working directory and exact command line. When outcomes depend on environment, state the assumptions and provide alternatives when reasonable.

Be idempotent and safe. Write the steps so they can be run multiple times without causing damage or drift. If a step can fail halfway, include how to retry or recover. Prefer additive, testable changes (for example, extracting a helper while keeping the original call path) before subtractive cleanups (for example, deleting duplicated code after tests pass). If a risky rewrite is being considered, break it into smaller behaviour-preserving steps.

Validation is not optional. Include instructions to run tests, to exercise the system if applicable, and to observe behaviour that proves the refactor did not change outcomes. Describe comprehensive testing for the area being refactored. Include expected outputs and failure signals so a novice can distinguish success from regressions. Where possible, show how to prove the refactor is meaningful beyond compilation (for example, by demonstrating reduced duplication in named functions while preserving exact public return values).

Capture evidence. When your steps produce terminal output, short diffs, or logs, include them inside the single fenced block as indented examples. Keep them concise and focused on what proves behaviour preservation and successful completion.

Prefer refactors that improve readability without changing semantics. Prefer smaller focused functions when a function is performing multiple distinct operations. Prefer pattern matching and function heads when they make branching logic clearer and preserve the same outcomes. Prefer guard clauses when they reduce nesting and preserve the same conditions.

Keep public and private APIs clear. Use `def` for public functions that are part of a module's external contract. Use `defp` for internal helpers introduced during refactoring. If moving code across functions, preserve the original return shape, error tuple conventions, raising behaviour, and side effects unless the task explicitly authorizes a behaviour change.

Be careful with bang (`!`) functions and non-bang variants. A bang function should continue to raise on failure if that is the existing behaviour. A non-bang function should continue to return error values in the repository's established style. Do not "normalize" these semantics during refactoring unless the task explicitly states that behaviour changes are allowed.

When refactoring macros or code-generation paths, treat generated behaviour as part of the behaviour boundary. Verify both compile-time and runtime behaviour if the affected code changes quoting, binding, hygiene, or generated function structure.

## Refactoring workflow

Refactoring work must follow a clear, repeatable sequence. The sequence exists to prevent accidental behaviour changes.

Begin by defining the behaviour boundary. A behaviour boundary is the exact observable behaviour that must remain unchanged during the refactor. Observable means a person or test can detect it. This may be a function return value, an HTTP response, a database write shape, a message sent to another process, a CLI output string, or a raised exception.

Next, identify the code smell. A code smell is a repeatable pattern that often makes code harder to maintain, but is not necessarily a bug. Examples include duplicated logic, long functions, deeply nested conditionals, mixed responsibilities in one function, or misleading names. Name the smell in plain language and tie it to the actual file and function being changed.

Then choose a refactoring technique that directly addresses that smell while keeping the behaviour boundary unchanged. A refactoring technique is a named, behaviour-preserving transformation such as extracting a function, inlining a trivial wrapper, splitting a conditional into pattern-matched clauses, introducing a helper for duplication, or renaming for clarity where behaviour is unchanged.

Apply the smallest change that moves the code toward the target structure. Run verification. Record the result. Repeat until the smell is addressed and the behaviour boundary remains intact. If a second smell is discovered, either capture it as a follow-up or address it only if doing so is directly related and can be verified safely within the same document.

## Verification

Unless the repository requires different commands, use these commands from the repository root to validate refactoring work in Elixir projects:

    mix format
    mix test path/to/changed_test.exs
    mix test

If the project uses a different command (for example, `mix ci`, `mix test --only`, or an umbrella app command from a subdirectory), state the replacement command in the RefactorDoc and explain why it is the correct command in this repository.

## Milestones

Milestones are narrative, not bureaucracy. If you break the refactoring work into milestones, introduce each with a brief paragraph that describes the scope, what will exist at the end of the milestone that did not exist before, the commands to run, and the acceptance you expect to observe. Keep it readable as a story: behaviour boundary, smell addressed, technique used, result, proof. Progress and milestones are distinct: milestones tell the story, progress tracks granular work. Both must exist. Never abbreviate a milestone merely for the sake of brevity, and do not leave out details that could be crucial to preserving behaviour.

Each milestone must be independently verifiable and incrementally move the code toward the refactored state.

## Living documents and refactoring decisions

* RefactorDocs are living documents. As you make key refactoring decisions, update the document to record both the decision and the reasoning behind it. Record all decisions in the `Decision Log` section.
* RefactorDocs must contain and maintain a `Progress` section, a `Surprises & Discoveries` section, a `Decision Log`, and an `Outcomes & Retrospective` section. These are not optional.
* When you discover edge cases, hidden side effects, duplicate logic in unexpected places, or test gaps that shaped your approach, capture those observations in the `Surprises & Discoveries` section with short evidence snippets (test output is ideal).
* If you change course mid-refactor (for example, extracting a helper first instead of renaming first because tests reveal shared side effects), document why in the `Decision Log` and reflect the implications in `Progress`.
* At completion of a major milestone or the full refactor, write an `Outcomes & Retrospective` entry summarizing what was improved, what behaviour was preserved, what remains, and lessons learned.

## Prototyping and parallel implementations during refactoring

It is acceptable-and often safer-to include explicit prototyping milestones when they de-risk a larger refactor. Examples include extracting a helper in one call site first to validate naming and return shape, or introducing a parallel implementation behind an internal switch to compare outcomes while preserving external behaviour. Keep prototypes additive and testable. Clearly label the scope as "prototyping", describe how to run and observe results, and state the criteria for promoting or discarding the prototype.

Prefer additive changes followed by subtractions that keep tests passing. Parallel implementations (for example, preserving the old function path while a new helper path is introduced and validated) are acceptable when they reduce risk or allow incremental verification. Describe how to validate both paths and how to retire one safely with tests.

## Skeleton of a Good RefactorDoc

    # <Short, action-oriented refactor description>

    This RefactorDoc is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

    If `REFACTORS.md` for refactoring is checked into the repo, reference the path to that file here from the repository root and note that this document must be maintained in accordance with it.

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

    Record every decision made while working on the refactor in the format:

    - Decision: …
      Rationale: …
      Date/Author: …

    ## Outcomes & Retrospective

    Summarize outcomes, remaining smells or follow-up opportunities, and lessons learned. Compare the result against the original purpose and behaviour boundary.

    ## Context and Orientation

    Describe the current state relevant to this refactor as if the reader knows nothing. Name the key files and modules by full path. Define any non-obvious term you will use. Do not refer to prior refactor documents unless they are present in the repository and explicitly named.

    ## Behaviour Boundary (Must Remain Unchanged)

    State the exact observable behaviour that must remain unchanged during the refactor. Be concrete and specific.

    Examples:
    - `MyApp.Parser.parse/1` returns `{:ok, tokens}` for valid input and `{:error, reason}` for invalid input, with the same `reason` atoms as before.
    - `GET /health` still returns HTTP 200 with body `OK`.
    - `mix my_task` still prints the same documented output lines for the example input.

    ## Code Smell Identified

    Name the code smell in plain language and explain exactly where it appears (file, module, function). Explain why it makes the code harder to maintain.

    ## Refactoring Technique Selected

    Name the technique(s) you will use and explain why they address the smell without changing the behaviour boundary.

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

If you follow the guidance above, a single, stateless agent-or a human novice-can read your RefactorDoc from top to bottom and perform a safe, observable refactor. That is the bar: SELF-CONTAINED, SELF-SUFFICIENT, NOVICE-GUIDING, BEHAVIOUR-PRESERVING.

When you revise a RefactorDoc, you must ensure your changes are comprehensively reflected across all sections, including the living document sections, and you must write a note at the bottom of the document describing the change and the reason why. RefactorDocs must describe not just what changed, but why that change is safe.