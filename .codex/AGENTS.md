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