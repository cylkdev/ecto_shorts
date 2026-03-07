# Refactor Plans (RefactorPlans)

This document defines the standard for a `RefactorPlan`, a living working document used to safely change code structure without intentionally changing observable behaviour. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single RefactorPlan you provide. There is no memory of prior refactors and no external context.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose / Big Picture

Use a RefactorPlan to turn a risky or unclear refactor into a concrete, restartable sequence that preserves behaviour with evidence. Record the request, the scope boundaries, the user-observable boundary, the behaviour boundary that must not change, the current code shape, the code smells that explain the maintenance pain, the refactoring techniques selected to address those smells, the exact work sequence, the things tried or ruled out, and the checks that prove the refactor stayed safe.

A RefactorPlan does not diagnose why the current system is wrong and it does not define new behaviour. It owns behaviour-preserving structural change. That makes it the document that turns a known refactor need into a safe sequence of work without competing with diagnosis, behaviour clarification, or feature planning.

## Output

- Primary artifact: A self-contained refactor specification for one behaviour-preserving change, including the current code shape, smell and technique grounding, concrete work sequence, and proof that observable behaviour remains the same.
- Primary consumer: The implementer carrying out the refactor and the reviewer checking that the chosen smells, techniques, and proof path are sound.
- Ready when: The behaviour boundary, current code shape, smells, techniques, concrete steps, and validation path are explicit enough for a beginner to refactor safely without inventing missing decisions.
- Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

## How to Use RefactorPlans and REFACTOR_PLANS.md

When you write a RefactorPlan, follow `.agent/REFACTOR_PLANS.md` to the letter. If it is not in your context, read the entire file before you continue.

Use this guide when the job is to improve structure without intentionally changing observable behaviour. Keep the RefactorPlan open while you work. Update it as the current code shape becomes clearer, smells are confirmed, techniques are chosen, experiments succeed or fail, blockers appear, and handoff decisions change. Do not treat the document as a summary you write at the end.

As soon as you choose this guide, write `Trigger for Using This Document`. Record the exact observed trigger facts, the full explicit reasoning path that made `RefactorPlan` the correct document, the nearest competing document types you rejected and why, and a short replication rule a later contributor can reuse. If the owning question changes but the RefactorPlan still owns the work, update that section and record the revision in `Change Log`.

Use `Document Relationships` to understand how this guide differs from the other planning guides. Use `Handoffs` to decide whether this document still owns the next unresolved question or whether a listed destination owns it now.

Use `.agent/OUTPUTS.md` as the source of truth for the RefactorPlan output location and naming rules.

## Safe Parallel Document Maintenance

When you update this document itself, let one coordinator own the final document edit. The coordinator decides the active scope, the canonical wording, and the final structure that lands in the checked-in guide.

After the change scope is stable, worker passes may inspect independent sections, companion files, or stale references in parallel. Each worker pass should return bounded facts such as outdated wording, missing sync updates, stale paths, or terminology drift.

Collect those worker-pass results before you edit this document. Do not update the guide from half-collected scans.

## Document Relationships

Use this section to understand how the planning guides relate to each other before you choose or change documents. Focus on purpose first, then intent, then the point where each document becomes the right place to work. Use `Handoffs` for the valid transitions.

### InvestigationLog

Purpose: Diagnose one visible problem and gather evidence.

Intent: Turn unclear failure into proven facts, interpretation, and a safe next action.

Use it when: A visible problem exists, but the cause is not yet proven.

### ExampleMappingDoc

Purpose: Clarify intended behaviour at one user-observable boundary.

Intent: Turn ambiguous or disputed behaviour into explicit rules, examples, and acceptance-test targets.

Use it when: The boundary is known, but the intended behaviour is still unclear.

### BehaviourSpecDoc

Purpose: Record a proof-ready behaviour specification at one user-observable boundary.

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

This document owns a question only while it is the place where the next missing decision, evidence, or instructions must be added. Use this section to decide whether this document still owns the next unresolved question, which listed destination owns it if not, and what evidence makes the handoff safe now.

### Incoming Handoffs

- From `InvestigationLog`: Use this guide when the failure diagnosis and behaviour boundary are already explicit, and the main unresolved question is how to sequence behaviour-preserving structural change.
- From `ExampleMappingDoc`: Use this guide when the accepted behaviour at the user-observable boundary is already explicit enough to protect, and the main unresolved question is how to sequence behaviour-preserving structural change.
- From `BehaviourSpecDoc`: Use this guide when the accepted behaviour and proof boundary are already explicit enough to protect, and the main unresolved question is how to sequence behaviour-preserving structural change.
- From `ArchitectureReview`: Use this guide when the system risk and mitigation direction are already explicit, and the main unresolved question is how to plan structural change that preserves observable behaviour while reducing that risk.
- From `ADR`: Use this guide when the lasting design choice is already recorded, and the main unresolved question is how to plan behaviour-preserving structural change that applies that decision.

### Outgoing Handoffs

- To direct behaviour-preserving refactor changes in the working tree: Hand off when the behaviour boundary, code-shape analysis, edit sequence, validation, and restart notes are explicit enough to stop refactor planning, and the remaining unresolved question is how to carry out those behaviour-preserving changes in the working tree.
- To `InvestigationLog`: Hand off when the refactor plan is explicit enough to show the remaining unresolved question is what failure or unexpected behaviour is actually happening, not how to sequence structural change.
- To `ExampleMappingDoc`: Hand off when the refactor plan is explicit enough to show the remaining unresolved question is what behaviour should be accepted at the user-observable boundary, not how to sequence structural change.
- To `BehaviourSpecDoc`: Hand off when the refactor plan is explicit enough to show the remaining unresolved question is how accepted behaviour should be specified and proved, not how to sequence structural change.
- To `ExecPlan`: Hand off when the refactor plan is explicit enough to show the remaining unresolved question is how to plan behaviour-changing implementation because the work no longer preserves behaviour.
- To `ADR`: Hand off when the refactor plan is explicit enough to show the remaining unresolved question is which lasting architectural or design decision must be recorded for future work.

### Recording the Handoff

Use the `Next Handoff` section to name one destination listed in this section.

State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every RefactorPlan must be fully self-contained, meaning a complete beginner can continue from the document and the current project files alone.
* Every RefactorPlan must be a living document, meaning you update it as the current code shape, smells, techniques, decisions, experiments, blockers, and checks change.
* Every RefactorPlan must keep the shared skeleton order defined in this guide.
* Every RefactorPlan must include an `Output` section that uses the exact four-line template from this guide.
* Every RefactorPlan must include a `Task and Key Files` section that records the concrete task, the key files, and every companion document, catalog, or artifact update that must stay in sync.
* Every RefactorPlan must include a `Trigger for Using This Document` section immediately after `Task and Key Files` and before `Output`.
* Every RefactorPlan must record the exact observed trigger facts, the full explicit reasoning path that made this the correct document, the nearest competing document types that were rejected and why, and a short replication rule another contributor can reuse.
* Every RefactorPlan must revise `Trigger for Using This Document` whenever the trigger facts or reasoning change while the RefactorPlan remains the correct document, and record that revision in `Change Log`.
* Every RefactorPlan must define one refactor at a time.
* Every RefactorPlan must define one concrete user-observable boundary at a time.
* Every RefactorPlan must restate the request in concrete language.
* Every RefactorPlan must define the behaviour boundary explicitly.
* Every RefactorPlan must record the current code shape before it prescribes changes.
* Every RefactorPlan must ground the refactor in explicit code smells and refactoring techniques from `.agent/refactor/`, or explain clearly why a catalog entry does not exist.
* Every RefactorPlan must use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.
* Every RefactorPlan must distinguish settled refactor decisions from open questions or blockers.
* Every RefactorPlan must keep tried, rejected, or deferred approaches visible when they materially affect the next safe step.
* Every RefactorPlan must include exact validation instructions that prove observable behaviour stayed the same.
* Every RefactorPlan must be readable by a complete beginner.
* Every RefactorPlan must treat creating or refreshing the active document and syncing required companion documents, catalogs, or artifact notes as tracked work in `Progress`.
* Every RefactorPlan must end with a clear current status and an explicit next handoff.

Treat these rules as mandatory. If one is missing, the RefactorPlan is incomplete and the refactor is not ready to guide safe changes.

## Workflow

1. Write `Request Restated`, `Scope Boundaries`, `User-Observable Boundary`, `Behaviour Boundary`, `Task and Key Files`, and `Trigger for Using This Document` so a complete beginner can see exactly what structure is changing, what behaviour must remain unchanged, why this document owns the task, and which files and companion documents must stay in sync.
2. Re-check the repository before you choose a refactor path. Look for the current code shape, nearby tests, callers, docs, stacktraces, duplication, coupling, naming patterns, and any prior plans or refactor artifacts that already explain the area.
3. Record `Current Code Shape` in concrete terms. Name the relevant files, modules, functions, data flow, and duplication or coupling points that make the code hard to change today.
4. Identify the `Code Smells Identified`. Use `.agent/refactor/AGENTS.md` to find the closest smell documents, cite the exact files, and summarize why they fit this code.
5. Let one coordinator own the refactor path. Once the boundary and current code shape are explicit, fan out bounded worker passes only for independent read-only checks such as callers, neighbouring tests, alternate smell matches, or technique comparisons. Collect those findings into `Progress`, `Surprises & Discoveries`, `Decision Log`, or `Open Questions / Blockers` before you choose the final path.
6. Select `Refactoring Techniques Selected`. Cite the exact technique documents from `.agent/refactor/`, explain why they address the smells safely, and record any additive prototype or parallel path you will use to reduce risk.
7. Build `Plan of Work` in small, behaviour-preserving steps. Prefer additive and testable changes before subtractive cleanup. If a step is risky, write the safe retry or rollback note in `Concrete Steps`.
8. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers` up to date as you inspect the code, try approaches, rule things out, and learn more. Include one `Progress` item for creating or refreshing the active document and one for keeping required companion documents, catalogs, or artifact notes in sync.
9. Set `Next Handoff` using `Handoffs` when the refactor reaches a real transition point or when the question stops being behaviour-preserving structural work.
10. Complete `Validation and Acceptance` before you stop. Confirm that the behaviour boundary is explicit, the smells and techniques are grounded, the work sequence is concrete, and the proof path shows behaviour did not change.

## Communication Rules

Do not refactor in silence. Record code-shape findings, smells, technique choices, blockers, rejected approaches, surprises, and changes in direction in the RefactorPlan as they happen.

Do not hide why the RefactorPlan owns the task. Record `Trigger for Using This Document` as soon as the owning question is clear, and revise it whenever the trigger facts or reasoning change while the RefactorPlan remains the correct document.

Before you settle on a refactor path, check whether the request has one reasonable meaning or more than one. If it has more than one reasonable meaning, stop and name the competing interpretations instead of silently choosing one.

Even when the request appears to have only one meaning, look for support in the project before you act. Support means existing evidence in the project that points to the same diagnosis, behaviour boundary, smell, or technique. Examples include nearby tests, callers, documentation, prior plans, naming patterns, and `.agent/refactor/` catalog entries.

If you cannot find support and you are not intentionally redefining behaviour, pause. Record that the current interpretation is unsupported. Then ask for clarification or record the most likely interpretations and the facts that would confirm each one.

Keep one coordinator responsible for the refactor sequence. A worker pass is one bounded inspection or comparison task that can run independently after the shared prerequisite is fixed. Use `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers` as the plan mailbox. Collect worker-pass findings there before the coordinator chooses a smell, technique, prototype, or `Next Handoff`.

Keep your communication concrete. State what code you inspected, what smell it matches, which techniques are safe, what you tried, what failed, what you ruled out, and what will prove the refactor preserved behaviour.

## Document-Specific Guidance

### Formatting

Each document written from this guide must be one single fenced code block labeled `md` when it is embedded inside another document or message. When writing the document to a Markdown file where the entire file is only that document, omit the outer triple backticks. Do not nest additional triple-backtick fences inside the document. When you need to show commands, transcripts, diffs, examples, scenarios, or code, present them as indented blocks inside the single `md` fence.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

### Catalog Usage

Use `.agent/refactor/AGENTS.md` as the map for code smells and refactoring techniques. Prefer the smallest smell that explains the maintenance pain and the smallest technique that addresses the root cause safely.

When you cite a smell or technique, name the exact catalog file and include a brief summary in the RefactorPlan itself so the document stays self-contained. If more than one smell or technique applies, name the primary one first and explain the order in which the techniques will be used.

### Additive Refactors, Prototypes, Parallel Paths, and Collection

Prefer additive, testable steps before destructive cleanup. Examples include extracting a helper while keeping the old call path in place, introducing a new internal module before removing the old logic, or isolating one duplicated branch before consolidating the rest.

Prototypes and parallel paths are acceptable when they reduce risk. If you use one, state the scope clearly, record how to run and compare both paths, and define the criteria for promoting or discarding the approach. Keep the prototype behaviour-preserving at the user-observable boundary.

Use actor-style parallel work only after the owning question is fixed. In this guide, the `Coordinator` is the single owner of refactor order and next-step decisions. A `Worker pass` is one bounded, independently runnable inspection or comparison task. `Fan-out` means starting several worker passes only after the shared prerequisite is fixed. The plan mailbox is the maintained state in `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers`. `Collect` means merging worker-pass findings back into that mailbox before you branch again.

### Elixir-Specific Behaviour Preservation

Prefer smaller focused functions when a function is doing multiple operations. Prefer pattern matching, function heads, and guard clauses when they make the code clearer without changing outcomes.

Preserve public and private API boundaries unless the task explicitly authorizes a behaviour change. If you introduce helpers, prefer `defp` for internal logic. Preserve return shapes, error tuple conventions, bang and non-bang behaviour, raising behaviour, and side effects unless the task explicitly says otherwise.

When refactoring macros or generated code, treat generated behaviour as part of the behaviour boundary. Verify both compile-time and runtime behaviour when quoting, binding, hygiene, or generated function structure changes.

### Validation Commands and Restartability

Use validation commands that fit the actual repository. If focused tests exist, name them first. If wider checks are required, name them explicitly. Use `.agent/PROJECT.md` as the source of truth for the repository root, the exact validation commands, prerequisite-repair steps, and the default widening order for this repository. In the RefactorPlan, record only the narrower subset you selected from its `Command Surface` and `Recommended Validation Paths` sections that proves this refactor is safe.

Use the shared `Task and Key Files`, `Trigger for Using This Document`, `Progress`, `Current State Snapshot`, `Concrete Steps`, `Surprises & Discoveries`, `Decision Log`, `Open Questions / Blockers`, `Next Handoff`, `Outcomes & Retrospective`, and `Change Log` sections to keep the document restartable. These sections let the RefactorPlan stay useful as a living record of what has been tried and what remains safe to try next.

## Skeleton of a Good RefactorPlan

Use this skeleton when you create a new RefactorPlan. Keep it complete enough that a complete beginner can continue from the document alone.

    # <Short, action-oriented refactor description>

    This RefactorPlan is a living document. Keep it up to date as the code shape becomes clearer, smells and techniques are refined, experiments succeed or fail, blockers appear, and the next handoff becomes clearer.

    If `.agent/REFACTOR_PLANS.md` is checked into the repository, maintain this RefactorPlan in accordance with that file.

    ## Definitions

    Use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.

    ## Status

    Write the current state in one short sentence.

    Examples:

        **Open.** The behaviour boundary is known, and the refactor path is still being refined.

        **Blocked.** The refactor need is known, but one or more blockers still prevent safe structural change.

        **Ready for Direct Refactor Changes.** The behaviour boundary, code-shape analysis, edit sequence, validation, and restart notes are explicit enough to stop planning and start behaviour-preserving changes in the working tree.

        **Resolved.** The refactor is complete and the checks that prove behaviour preservation are recorded below.

    ## Current State Snapshot

    Write a short summary of where the document stands right now.

    State what is known, what is still unknown, what repository evidence has already been checked, what has already been tried, and what a complete beginner should do first if they restart here.

    ## Task and Key Files

    Record the concrete task this plan currently owns.

    List the key repository files, commands, tests, catalogs, or companion documents that matter right now.

    List every document, catalog, or artifact that must be created or updated in the same change and keep this section current as the task or handoff changes.

    ## Trigger for Using This Document

    Record the exact trigger that made this RefactorPlan the correct document.

    State the concrete observed conditions from the request, repository, prior artifact, or observed system state that triggered this document choice.

    Write the full explicit reasoning path from those facts to this document. Do not skip intermediate decision steps.

    Name the nearest competing document types you considered and explain why each one does not own the current unresolved question.

    End with a short replication rule another contributor can follow to reach the same document choice.

    ## Output

    - Primary artifact: A self-contained refactor specification for one behaviour-preserving change, including the current code shape, smell and technique grounding, concrete work sequence, and proof that observable behaviour remains the same.
    - Primary consumer: The implementer carrying out the refactor and the reviewer checking that the chosen smells, techniques, and proof path are sound.
    - Ready when: The behaviour boundary, current code shape, smells, techniques, concrete steps, and validation path are explicit enough for a beginner to refactor safely without inventing missing decisions.
    - Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

    ## Progress

    Use a list with checkboxes to summarize the refactor work and every meaningful stopping point.

    Include one item for creating or refreshing this RefactorPlan and one item for keeping required companion documents, catalogs, or artifact notes in sync.

    **Legend**

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [x] (YYYY-MM-DD HH:MMZ) Created or refreshed this RefactorPlan and updated `Task and Key Files` and `Trigger for Using This Document`.
    - [ ] Keep required companion documents, catalogs, or artifact notes in sync with this RefactorPlan.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Purpose / Big Picture

    Explain why this refactor matters and what becomes easier to read, change, test, or reason about after it is complete.

    State the observable behaviour that must remain unchanged.

    ## Context and Orientation

    Describe the current state relevant to this refactor as if the reader knows nothing.

    Name the key files, modules, commands, and entry points that a complete beginner must understand before they continue.

    Use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.

    ## Request Restated

    Restate the request so a complete beginner can answer "yes, that is the refactor" or "no, that is not what I meant."

    If more than one interpretation is reasonable, list the competing interpretations here.

    ## Scope Boundaries

    State what structural change is in scope and what related cleanup, feature work, or behaviour changes are explicitly out of scope.

    If two nearby kinds of work could be confused, name the difference directly.

    ## User-Observable Boundary

    Name the user-observable boundary where behaviour preservation will be observed and proved.

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

    Name one exact destination listed in `Handoffs`.

    State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

    If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

    ## Outcomes & Retrospective

    State the current outcome in one short paragraph.

    Explain what structural improvement was achieved, what remains, what was learned, and what a complete beginner should know before continuing.

    ## Change Log

    Record every revision so a newcomer can see how the document changed over time.

    - YYYY-MM-DD: ...
      Rationale: ...

## Final Reminder

Refactoring is for making structure safer and clearer without silently changing behaviour. Use `Document Relationships` to confirm what this guide owns. Use `Handoffs` to choose the next listed destination when the unresolved question changes.
