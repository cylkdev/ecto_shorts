---
trigger: always_on
---

For each rule below: if the condition applies, perform the action.

- **Root Guide Routing:** If the current unresolved question is which root `.agent` guide owns the task, read only the owning guide; if ownership is unclear, start with `.agent/AGENTS.md`; if the question changes, open the next matching guide; and if the chosen guide produces one of the seven root planning artifacts, include `Trigger for Using This Document` with the exact trigger facts, full reasoning path, rejected nearby document types with reasons, and a replication rule.
- **Research Context:** If a request or task depends on project research context or project-level constraints, read `research/AGENTS.md` first and follow it exactly.
- **Code Style:** Before writing or revising code, use `.agent/styles/AGENTS.md` to find the relevant style area, then read only the rule files that apply to the current change and treat them as the source of truth; keep each change aligned with those rules as you make it, and if the correct rule is unclear, follow an existing matching example in the codebase.
- **Artifact Routing and Sync:** If the current unresolved question is where a document artifact belongs, how to name it, or whether companion documents remain in sync after artifact-related changes, read `.agent/OUTPUTS.md` for artifact destinations and filename patterns and `.agent/AGENTS.md` for document-maintenance requirements; do not duplicate those rules elsewhere, complete the work only when all affected active documents and companion guides are updated in the same change, and move or rename stale artifacts and repair stale references immediately.
- **Post Change Checks:** If you made a code change or implemented a feature, run workflow `run-checks`.
- **PDF Export:** If a request or task requires exporting, reading, or extracting readable output from one or more PDFs, run workflow `read-pdfs`.

## Root Guide Routing

- Diagnose one visible problem whose cause is not yet proven: `.agent/INVESTIGATION_LOGS.md`
- Clarify intended behaviour at one user-observable boundary: `.agent/EXAMPLE_MAPPING.md`
- Record a proof-ready behaviour specification at one user-observable boundary: `.agent/BEHAVIOURS.md`
- Plan a behaviour-changing implementation sequence: `.agent/PLANS.md`
- Plan a behaviour-preserving structural change: `.agent/REFACTOR_PLANS.md`
- Review system shape, resilience, state ownership, scaling risk, dependency risk, or failure spread: `.agent/ARCHITECTURE_REVIEW.md`
- Record one lasting architectural or design decision: `.agent/ADRS.md`
- Create or revise one reusable style rule: `.agent/CODE_STYLE_RULES.md`
- Choose project-level command guidance: `.agent/PROJECT.md`

## Code Style Rules

* `.agent/styles/code_related_anti_patterns/` — general Elixir modules and functions
* `.agent/styles/design_related_anti_patterns/` — module interfaces, data structures, and return shapes
* `.agent/styles/documentation/` — module docs, function docs, and doctests
* `.agent/styles/ecto/` — Ecto queries, schemas, and changesets
* `.agent/styles/meta_programming_anti_patterns/` — macros, `use`, and compile-time code
* `.agent/styles/naming_conventions/` — modules, functions, variables, files, and atoms
* `.agent/styles/process_related_anti_patterns/` — GenServers, Agents, Tasks, and other process code
* `.agent/styles/public_api_and_interfaces/` — public and private function interfaces
* `.agent/styles/struct_anti_patterns/` — `defstruct`
* `.agent/styles/testing/` — tests and doctests

## Communication Guidelines

- Write so that a complete beginner with no external context can follow your meaning without guessing.
- Choose wording that has only one reasonable interpretation.
- Prefer several short, clear sentences over one compact sentence.
- Add detail when detail prevents misunderstanding.

- Do not omit steps that the reader must understand to follow the explanation.
- Do not assume the reader can infer missing steps.
- State each required step in the order it must happen.
- Explain how one step leads to the next step when that connection is not obvious.
- Define any term, command, file, or concept before you use it in an instruction.
- Include all intermediate actions that a beginner must perform to succeed without guessing.

- When you describe a code implementation or a code change, include exact code examples. Apply this rule in chat explanations. Apply this rule in plans. Apply this rule in any other explanation of intended code work.

- Make your intent explicit.
- State exactly what you intend to do.
- State exactly which files, functions, interfaces, commands, or behaviors you intend to change when that information is available.
- State exactly what will remain unchanged when that information matters for avoiding confusion.

- Do not describe the change only in abstract terms.
- Do not rely on summaries alone.
- Make the planned change visible in the text of your explanation.
- Show the exact code you plan to add, remove, or replace.

- Include examples for every meaningful change.
- Provide enough examples to cover all meaningful changes.
- Do not give only one partial example when multiple separate changes matter.
- Use multiple examples when one example cannot fully show the reader-facing effect of the work.
- Add more examples until a beginner could see what will change and where it will change.

- Treat a change as meaningful if it affects what the reader will see.
- Treat a change as meaningful if it affects how the reader will use the public interface.
- Treat a change as meaningful if it changes inputs, outputs, names, behavior, errors, or configuration that the reader must know about.

## Feature Implementation and Code Change Guidelines

NON-NEGOTIABLE REQUIREMENTS:

Before implementing a feature or making a code change:

1. Systematically review the request. Read (and re-read) all important context and documentation related to the task thoroughly.

2. For each decision you have made identify evidence to support it and consider if there are alternative approaches. Analyze what the blast radius for all decisions and identify gaps and assumptions in understanding. All the steps you take from beginning to end should be clear before you continue.

2. State what you understand the task to be and confirm that there are no gaps in scope, intent, or expected outcome that could cause you to make the wrong change.

3. Plan the work thoroughly before you begin. Break the task down into the smallest possible units of work. For non-trivial tasks you must have clearly defined milestones at every level of the work: the initial task, each task within it and each supporting sub-task. Structure the plan so that large-scope work is divided into many smaller milestones that can be tracked and completed in sequence. Do not begin changing code until this planning is complete.

### What to do

Prioritize accuracy over speed. Make one small change at a time.

Follow these steps for each change:

1. Make the change.

2. Present the change to the user and ask for feedback. Verify that the change matches the intended outcome.

3. Stop and wait for the user’s response.

4. When feedback arrives, record any gaps, corrections, or refinements to your understanding in a log.

5. Do not proceed with normal execution until the user has approved the change.

6. Repeat these steps for each change. Do not move on to the next change until the current one has been reviewed and approved.