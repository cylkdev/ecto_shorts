# Refactor Plans (RefactorPlans)

This document defines the standard for a `RefactorPlan`, a living working document used to safely change code structure without intentionally changing observable behaviour. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single RefactorPlan you provide. There is no memory of prior refactors and no external context.

## Purpose / Big Picture

Use a RefactorPlan to turn a risky or unclear refactor into a concrete, restartable sequence that preserves behaviour with evidence. Record the request, the scope boundaries, the visible boundary, the behaviour boundary that must not change, the current code shape, the code smells that explain the maintenance pain, the refactoring techniques selected to address those smells, the exact work sequence, the things tried or ruled out, and the checks that prove the refactor stayed safe.

A RefactorPlan does not diagnose why the current system is wrong and it does not define new behaviour. It owns behaviour-preserving structural change. That makes it the document that turns a known refactor need into a safe sequence of work without competing with diagnosis, behaviour clarification, or feature planning.

## Output

- Primary artifact: A self-contained refactor specification for one behaviour-preserving change, including the current code shape, smell and technique grounding, concrete work sequence, and proof that observable behaviour remains the same.
- Primary consumer: The implementer carrying out the refactor and the reviewer checking that the chosen smells, techniques, and proof path are sound.
- Ready when: The behaviour boundary, current code shape, smells, techniques, concrete steps, and validation path are explicit enough for a beginner to refactor safely without inventing missing decisions.
- Hands off to: See `Handoffs` for the valid next document or work state and the condition for using it.

## How to Use RefactorPlans and REFACTOR_PLANS.md

When you write a RefactorPlan, follow `.agent/REFACTOR_PLANS.md` to the letter. If it is not in your context, read the entire file before you continue.

Use this guide when the job is to improve structure without intentionally changing observable behaviour. Keep the RefactorPlan open while you work. Update it as the current code shape becomes clearer, smells are confirmed, techniques are chosen, experiments succeed or fail, blockers appear, and handoff decisions change. Do not treat the document as a summary you write at the end.

Use `Document Relationships` to understand how this guide differs from the other planning guides. Use `Handoffs` to decide whether work should stay here or move to another document or work state.

Store completed RefactorPlans under `docs/refactor_plans/` and name them with four-digit, zero-padded names such as `docs/refactor_plans/0001-short-title.md`.

## Document Relationships

Use this section to understand how the planning guides relate to each other before you choose or change documents. Focus on purpose first, then intent, then the point where each document becomes the right place to work. Use `Handoffs` for the valid transitions.

### InvestigationLog

Purpose: Diagnose one visible problem and gather evidence.

Intent: Turn unclear failure into proven facts, interpretation, and a safe next action.

Use it when: A visible problem exists, but the cause is not yet proven.

### ExampleMappingDoc

Purpose: Clarify intended behaviour at one visible boundary.

Intent: Turn ambiguous or disputed behaviour into explicit rules, examples, and acceptance-test targets.

Use it when: The boundary is known, but the intended behaviour is still unclear.

### BehaviourSpecDoc

Purpose: Record a proof-ready behaviour specification at one visible boundary.

Intent: Turn accepted behaviour into concrete specification and proof mapping that implementation can follow without inventing behaviour.

Use it when: Intended behaviour is accepted, but implementation planning should not begin until the proof path is explicit.

### ExecPlan

Purpose: Define a concrete implementation sequence for a behaviour-changing result.

Intent: Turn a clear change request into exact edits, commands, and validation that produce a working result.

Use it when: Diagnosis, behaviour, and proof expectations are already clear enough to implement.

### RefactorPlan

Purpose: Define a safe structural change that preserves observable behaviour.

Intent: Turn a known refactor need into a restartable, behaviour-preserving work sequence with proof.

Use it when: The goal is to improve structure without intentionally changing observable behaviour.

### ArchitectureReview

Purpose: Review system shape, risk, and failure behaviour.

Intent: Turn a complex system into an explicit, evidence-backed architecture risk review and mitigation direction.

Use it when: The question is about resilience, scaling, state ownership, dependency risk, or system-level failure spread.

### ADR

Purpose: Record one lasting architectural or design decision.

Intent: Turn an important choice into a durable record of drivers, options, outcome, consequences, and validation.

Use it when: A decision must stay explicit over time so future maintainers can understand and apply it.

## Handoffs

Only one document owns a question at a time. Use this section to decide when work should arrive in this document, when it should leave it, and how to record that transition.

### Incoming Handoffs

- From `InvestigationLog`: Use this guide when diagnosis is complete and the next work should preserve observable behaviour.
- From `ExampleMappingDoc`: Use this guide when accepted behaviour is clear enough to support a behaviour-preserving refactor.
- From `BehaviourSpecDoc`: Use this guide when accepted behaviour and proof are clear enough to support a behaviour-preserving refactor.
- From `ArchitectureReview`: Use this guide when mitigation is structural and behaviour-preserving.
- From `ADR`: Use this guide when a recorded decision needs behaviour-preserving structural work.

### Outgoing Handoffs

- To refactor implementation: Hand off when the refactor sequence is ready to execute.
- To `InvestigationLog`: Hand off when refactoring reopens a diagnosis question.
- To `ExampleMappingDoc`: Hand off when refactoring reopens an intended-behaviour question.
- To `BehaviourSpecDoc`: Hand off when refactoring reopens a proof-ready specification question.
- To `ExecPlan`: Hand off when the work stops being behaviour-preserving.
- To `ADR`: Hand off when the refactor creates a lasting design decision.

### Recording the Handoff

Use the `Next Handoff` section to name one valid next document or work state from `Handoffs` and explain why it applies now.

If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every RefactorPlan must be fully self-contained, meaning a complete beginner can continue from the document and the current project files alone.
* Every RefactorPlan must be a living document, meaning you update it as the current code shape, smells, techniques, decisions, experiments, blockers, and checks change.
* Every RefactorPlan must keep the shared skeleton order defined in this guide.
* Every RefactorPlan must include an `Output` section that uses the exact four-line template from this guide.
* Every RefactorPlan must define one refactor at a time.
* Every RefactorPlan must define one concrete visible boundary at a time.
* Every RefactorPlan must restate the request in concrete language.
* Every RefactorPlan must define the behaviour boundary explicitly.
* Every RefactorPlan must record the current code shape before it prescribes changes.
* Every RefactorPlan must ground the refactor in explicit code smells and refactoring techniques from `.agent/refactor/`, or explain clearly why a catalog entry does not exist.
* Every RefactorPlan must define every technical term in plain language when it first appears.
* Every RefactorPlan must distinguish settled refactor decisions from open questions or blockers.
* Every RefactorPlan must keep tried, rejected, or deferred approaches visible when they materially affect the next safe step.
* Every RefactorPlan must include exact validation instructions that prove observable behaviour stayed the same.
* Every RefactorPlan must be readable by a complete beginner.
* Every RefactorPlan must end with a clear current status and an explicit next handoff.

Treat these rules as mandatory. If one is missing, the RefactorPlan is incomplete and the refactor is not ready to guide safe changes.

## Workflow

1. Write `Request Restated`, `Scope Boundaries`, `Visible Boundary`, and `Behaviour Boundary` so a complete beginner can see exactly what structure is changing and what behaviour must remain unchanged.
2. Re-check the repository before you choose a refactor path. Look for the current code shape, nearby tests, callers, docs, stacktraces, duplication, coupling, naming patterns, and any prior plans or refactor artifacts that already explain the area.
3. Record `Current Code Shape` in concrete terms. Name the relevant files, modules, functions, data flow, and duplication or coupling points that make the code hard to change today.
4. Identify the `Code Smells Identified`. Use `.agent/refactor/AGENTS.md` to find the closest smell documents, cite the exact files, and summarize why they fit this code.
5. Select `Refactoring Techniques Selected`. Cite the exact technique documents from `.agent/refactor/`, explain why they address the smells safely, and record any additive prototype or parallel path you will use to reduce risk.
6. Build `Plan of Work` in small, behaviour-preserving steps. Prefer additive and testable changes before subtractive cleanup. If a step is risky, write the safe retry or rollback note in `Concrete Steps`.
7. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers` up to date as you inspect the code, try approaches, rule things out, and learn more.
8. Set `Next Handoff` using `Handoffs` when the refactor reaches a real transition point or when the question stops being behaviour-preserving structural work.
9. Complete `Validation and Acceptance` before you stop. Confirm that the behaviour boundary is explicit, the smells and techniques are grounded, the work sequence is concrete, and the proof path shows behaviour did not change.

## Communication Rules

Do not refactor in silence. Record code-shape findings, smells, technique choices, blockers, rejected approaches, surprises, and changes in direction in the RefactorPlan as they happen.

Before you settle on a refactor path, check whether the request has one reasonable meaning or more than one. If it has more than one reasonable meaning, stop and name the competing interpretations instead of silently choosing one.

Even when the request appears to have only one meaning, look for support in the project before you act. Support means existing evidence in the project that points to the same diagnosis, behaviour boundary, smell, or technique. Examples include nearby tests, callers, documentation, prior plans, naming patterns, and `.agent/refactor/` catalog entries.

If you cannot find support and you are not intentionally redefining behaviour, pause. Record that the current interpretation is unsupported. Then ask for clarification or record the most likely interpretations and the facts that would confirm each one.

Keep your communication concrete. State what code you inspected, what smell it matches, which techniques are safe, what you tried, what failed, what you ruled out, and what will prove the refactor preserved behaviour.

## Document-Specific Guidance

### Formatting

Each document written from this guide must be one single fenced code block labeled `md` when it is embedded inside another document or message. When writing the document to a Markdown file where the entire file is only that document, omit the outer triple backticks. Do not nest additional triple-backtick fences inside the document. When you need to show commands, transcripts, diffs, examples, scenarios, or code, present them as indented blocks inside the single `md` fence.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

### Catalog Usage

Use `.agent/refactor/AGENTS.md` as the map for code smells and refactoring techniques. Prefer the smallest smell that explains the maintenance pain and the smallest technique that addresses the root cause safely.

When you cite a smell or technique, name the exact catalog file and include a brief summary in the RefactorPlan itself so the document stays self-contained. If more than one smell or technique applies, name the primary one first and explain the order in which the techniques will be used.

### Additive Refactors, Prototypes, and Parallel Paths

Prefer additive, testable steps before destructive cleanup. Examples include extracting a helper while keeping the old call path in place, introducing a new internal module before removing the old logic, or isolating one duplicated branch before consolidating the rest.

Prototypes and parallel paths are acceptable when they reduce risk. If you use one, state the scope clearly, record how to run and compare both paths, and define the criteria for promoting or discarding the approach. Keep the prototype behaviour-preserving at the visible boundary.

### Elixir-Specific Behaviour Preservation

Prefer smaller focused functions when a function is doing multiple operations. Prefer pattern matching, function heads, and guard clauses when they make the code clearer without changing outcomes.

Preserve public and private API boundaries unless the task explicitly authorizes a behaviour change. If you introduce helpers, prefer `defp` for internal logic. Preserve return shapes, error tuple conventions, bang and non-bang behaviour, raising behaviour, and side effects unless the task explicitly says otherwise.

When refactoring macros or generated code, treat generated behaviour as part of the behaviour boundary. Verify both compile-time and runtime behaviour when quoting, binding, hygiene, or generated function structure changes.

### Validation Commands and Restartability

Use validation commands that fit the actual repository. If focused tests exist, name them first. If wider checks are required, name them explicitly. In this repository, record the exact `mix` commands or replacements needed for the area under refactor and explain why they are sufficient.

Use the shared `Progress`, `Current State Snapshot`, `Concrete Steps`, `Surprises & Discoveries`, `Decision Log`, `Open Questions / Blockers`, `Next Handoff`, `Outcomes & Retrospective`, and `Change Log` sections to keep the document restartable. These sections let the RefactorPlan stay useful as a living record of what has been tried and what remains safe to try next.

## Skeleton of a Good RefactorPlan

Use this skeleton when you create a new RefactorPlan. Keep it complete enough that a complete beginner can continue from the document alone.

    # <Short, action-oriented refactor description>

    This RefactorPlan is a living document. Keep it up to date as the code shape becomes clearer, smells and techniques are refined, experiments succeed or fail, blockers appear, and the next handoff becomes clearer.

    If `.agent/REFACTOR_PLANS.md` is checked into the repository, maintain this RefactorPlan in accordance with that file.

    ## Status

    Write the current state in one short sentence.

    Examples:

        **Open.** The behaviour boundary is known, and the refactor path is still being refined.

        **Blocked.** The refactor need is known, but one or more blockers still prevent safe structural change.

        **Ready for Refactor.** The code shape, smells, techniques, and proof path are complete enough to execute safely.

        **Resolved.** The refactor is complete and the checks that prove behaviour preservation are recorded below.

    ## Current State Snapshot

    Write a short summary of where the document stands right now.

    State what is known, what is still unknown, what repository evidence has already been checked, what has already been tried, and what a complete beginner should do first if they restart here.

    ## Output

    - Primary artifact: A self-contained refactor specification for one behaviour-preserving change, including the current code shape, smell and technique grounding, concrete work sequence, and proof that observable behaviour remains the same.
    - Primary consumer: The implementer carrying out the refactor and the reviewer checking that the chosen smells, techniques, and proof path are sound.
    - Ready when: The behaviour boundary, current code shape, smells, techniques, concrete steps, and validation path are explicit enough for a beginner to refactor safely without inventing missing decisions.
    - Hands off to: See `Handoffs` for the valid next document or work state and the condition for using it.

    ## Progress

    Use a list with checkboxes to summarize the refactor work and every meaningful stopping point.

    **Legend**

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [x] (YYYY-MM-DD HH:MMZ) Example completed step.
    - [ ] Example incomplete step.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Purpose / Big Picture

    Explain why this refactor matters and what becomes easier to read, change, test, or reason about after it is complete.

    State the observable behaviour that must remain unchanged.

    ## Context and Orientation

    Describe the current state relevant to this refactor as if the reader knows nothing.

    Name the key files, modules, commands, and entry points that a complete beginner must understand before they continue.

    Define any non-obvious term you will use.

    ## Request Restated

    Restate the request so a complete beginner can answer "yes, that is the refactor" or "no, that is not what I meant."

    If more than one interpretation is reasonable, list the competing interpretations here.

    ## Scope Boundaries

    State what structural change is in scope and what related cleanup, feature work, or behaviour changes are explicitly out of scope.

    If two nearby kinds of work could be confused, name the difference directly.

    ## Visible Boundary

    Name the visible boundary where behaviour preservation will be observed and proved.

    Record the exact command, input, request, or action that exercises that boundary when applicable.

    ## Behaviour Boundary

    State the exact observable behaviour that must remain unchanged during the refactor.

    Include concrete return values, outputs, side effects, error cases, or test expectations when applicable.

    ## Current Code Shape

    Record the concrete code currently under refactor.

    Name the exact files, modules, functions, data flow, duplication, coupling, branching, or coordination points that matter to this refactor.

    ## Code Smells Identified

    Name the code smell or smells from `.agent/refactor/` that best describe the current code.

    Cite the exact catalog file for each smell.

    Summarize why each smell fits this code and why it creates maintenance pain here.

    ## Refactoring Techniques Selected

    Name the refactoring technique or techniques from `.agent/refactor/` that you will use.

    Cite the exact catalog file for each technique.

    Explain why each technique addresses the identified smells without changing the behaviour boundary.

    Record any additive prototype, experiment, or parallel path that reduces risk.

    ## Plan of Work

    Describe, in prose, the sequence of edits and checks.

    For each edit, name the file and location and what to insert, move, extract, rename, or remove.

    Keep the description concrete and behaviour-preserving.

    ## Concrete Steps

    Record the exact repository checks, files, commands, reruns, and recovery notes needed to perform, verify, and safely restart the refactor.

    Include enough detail that a complete beginner can restart the work safely.

    ## Surprises & Discoveries

    Record unexpected behaviours, side effects, hidden coupling, failed attempts, or test gaps discovered during refactoring.

    Include the evidence that revealed each one and why it matters.

    ## Decision Log

    Record important refactor decisions in this format:

    - Decision: ...
      Rationale: ...
      Evidence: ...
      Date/Author: ...

    ## Validation and Acceptance

    Describe how to prove the refactor preserved behaviour.

    State the exact commands to run and what observable result must remain the same.

    If tests are involved, name the exact focused and broader test commands and what should pass.

    ## Open Questions / Blockers

    List only unresolved blocker questions.

    For each blocker, state why it matters, what decision changes based on the answer, what has already been tried, and what evidence has already been checked.

    If there are no remaining blockers, say that explicitly.

    ## Next Handoff

    State the next safe handoff using `Handoffs`.

    Name the exact next document or work state and explain why it applies now.

    If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

    ## Outcomes & Retrospective

    State the current outcome in one short paragraph.

    Explain what structural improvement was achieved, what remains, what was learned, and what a complete beginner should know before continuing.

    ## Change Log

    Record every revision so a newcomer can see how the document changed over time.

    - YYYY-MM-DD: ...
      Rationale: ...

## Final Reminder

Refactoring is for making structure safer and clearer without silently changing behaviour. Use `Document Relationships` to confirm what this guide owns. Use `Handoffs` to choose the next document or work state when the question changes.
