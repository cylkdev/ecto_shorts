# Behaviour Specification Documents (BehaviourSpecDocs)

This document defines the standard for a `BehaviourSpecDoc`, a living working document used to turn accepted behaviour into a concrete, proof-ready behaviour specification at one user-observable boundary. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single BehaviourSpecDoc you provide. There is no memory of prior specifications and no external context.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose / Big Picture

Use a BehaviourSpecDoc to turn accepted behaviour into a concrete specification that can be implemented and proved without inventing missing decisions. Record the request, the scope boundaries, the user-observable boundary, the source of the behaviour, the vocabulary that keeps the terms stable, the behaviour specification itself, the exact proof mapping, the open questions or blockers that still matter, and the validation path that shows the specification is ready to drive implementation.

A BehaviourSpecDoc does not diagnose why the current system is wrong and it does not prescribe the implementation sequence. It owns the proof-ready behaviour-specification layer at the user-observable boundary. That makes it the document that turns an accepted ExampleMappingDoc into something an ExecPlan can implement without competing with either one.

## Output

- Primary artifact: A self-contained, proof-ready behaviour specification for one story or change at one user-observable boundary.
- Primary consumer: The ExecPlan author and the person writing or updating the runnable proof.
- Ready when: The behaviour source, vocabulary, specification, proof mapping, and validation path are explicit enough for a beginner to implement without inventing behaviour.
- Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

## How to Use BehaviourSpecDocs and BEHAVIOURS.md

When you write a BehaviourSpecDoc, follow `.agent/BEHAVIOURS.md` to the letter. If it is not in your context, read the entire file before you continue.

Use this guide after intended behaviour is accepted but before implementation planning begins. Keep the BehaviourSpecDoc open while you work. Update it as the behaviour source is confirmed, vocabulary is refined, proof targets are chosen, blockers are resolved, and handoff decisions change. Do not treat the document as a summary you write at the end.

As soon as you choose this guide, write `Trigger for Using This Document`. Record the exact observed trigger facts, the full explicit reasoning path that made `BehaviourSpecDoc` the correct document, the nearest competing document types you rejected and why, and a short replication rule a later contributor can reuse. If the owning question changes but the BehaviourSpecDoc still owns the work, update that section and record the revision in `Change Log`.

Use `Document Relationships` to understand how this guide differs from the other planning guides. Use `Handoffs` to decide whether this document still owns the next unresolved question or whether a listed destination owns it now.

Use `.agent/OUTPUTS.md` as the source of truth for the BehaviourSpecDoc output location and naming rules.

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

- From `InvestigationLog`: Use this guide when the failure diagnosis and expected outcome are already explicit, and the main unresolved question is how to write the accepted behaviour as a concrete specification and proof path at one user-observable boundary.
- From `ExampleMappingDoc`: Use this guide when the story, rules, examples, and acceptance intent are already explicit, and the main unresolved question is how to turn them into a concrete specification and proof path at one user-observable boundary.
- From `ExecPlan`: Use this guide when the implementation attempt has already shown that the remaining unresolved question is not sequencing but how the behaviour should be specified and proved at the user-observable boundary.
- From `RefactorPlan`: Use this guide when the refactor plan has already shown that the remaining unresolved question is not structure but how the behaviour should be specified and proved at the user-observable boundary.

### Outgoing Handoffs

- To `ExecPlan`: Hand off when the behaviour source, specification, and proof mapping are explicit enough to stop specification work, and the remaining unresolved question is how to plan behaviour-changing implementation.
- To `RefactorPlan`: Hand off when the accepted behaviour and proof boundary are explicit enough to stop specification work, and the remaining unresolved question is how to plan behaviour-preserving structural change.
- To `ExampleMappingDoc`: Hand off when the current specification draft shows the remaining unresolved question is what the intended behaviour should be, not how to express or prove it.
- To `InvestigationLog`: Hand off when the current specification draft shows the remaining unresolved question is what is actually happening in the system, not how accepted behaviour should be expressed or proved.

### Recording the Handoff

Use the `Next Handoff` section to name one destination listed in this section.

State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every BehaviourSpecDoc must be fully self-contained, meaning a complete beginner can continue from the document and the current project files alone.
* Every BehaviourSpecDoc must be a living document, meaning you update it as sources, vocabulary, specification details, proof mapping, blockers, decisions, and checks change.
* Every BehaviourSpecDoc must keep the shared skeleton order defined in this guide.
* Every BehaviourSpecDoc must include an `Output` section that uses the exact four-line template from this guide.
* Every BehaviourSpecDoc must include a `Task and Key Files` section that records the concrete task, the key files, and every companion document, catalog, or artifact update that must stay in sync.
* Every BehaviourSpecDoc must include a `Trigger for Using This Document` section immediately after `Task and Key Files` and before `Output`.
* Every BehaviourSpecDoc must record the exact observed trigger facts, the full explicit reasoning path that made this the correct document, the nearest competing document types that were rejected and why, and a short replication rule another contributor can reuse.
* Every BehaviourSpecDoc must revise `Trigger for Using This Document` whenever the trigger facts or reasoning change while the BehaviourSpecDoc remains the correct document, and record that revision in `Change Log`.
* Every BehaviourSpecDoc must define one story or change at a time.
* Every BehaviourSpecDoc must define one concrete user-observable boundary at a time.
* Every BehaviourSpecDoc must restate the request in concrete language.
* Every BehaviourSpecDoc must distinguish settled specification from open questions or blockers.
* Every BehaviourSpecDoc must use repository evidence before inventing new specification language.
* Every BehaviourSpecDoc must define the behaviour source explicitly.
* Every BehaviourSpecDoc must use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.
* Every behaviour statement must describe observable behaviour at the chosen user-observable boundary.
* Every behaviour specification must include concrete inputs, actions, and expected observable outcomes.
* Every BehaviourSpecDoc must map the specification to the exact proof files, runner commands, and observable acceptance signals that should prove it.
* Every BehaviourSpecDoc must be readable by a complete beginner.
* Every BehaviourSpecDoc must treat creating or refreshing the active document and syncing required companion documents, catalogs, or artifact notes as tracked work in `Progress`.
* Every BehaviourSpecDoc must end with a clear current status and an explicit next handoff.

Treat these rules as mandatory. If one is missing, the BehaviourSpecDoc is incomplete and the document is not ready to guide implementation or proof work.

## Workflow

1. Write `Request Restated`, `Scope Boundaries`, `User-Observable Boundary`, `Task and Key Files`, and `Trigger for Using This Document` so a complete beginner can see exactly what behaviour is being specified, where it will be observed, why this document owns the task, and which files and companion documents must stay in sync.
2. Confirm the `Behaviour Source`. Record the exact ExampleMappingDoc, accepted request, test evidence, documentation, caller contract, or explicit decision that authorizes this behaviour. If the behaviour source is still disputed, set `Next Handoff` using `Handoffs` before you continue.
3. Re-check the repository before you write the specification. Look for nearby tests, callers, docs, naming patterns, existing feature-style specs, and current runner commands. If the repository already has a style for this kind of proof, follow it.
4. Write `Behaviour Vocabulary` so every important domain term has one stable meaning. If a term maps to fields, tables, JSON keys, functions, or visible outputs, name them directly.
5. After the behaviour source and vocabulary are stable, let one coordinator fan out bounded worker passes to inspect proof styles, target files, runner commands, acceptance signals, or adjacent behaviour examples. Keep each worker pass narrow enough that it can return one proof candidate or one vocabulary check without changing the specification question.
6. Record each worker pass result in the mailbox sections that fit it, such as `Behaviour Vocabulary`, `Mapping to Proof`, `Progress`, or `Open Questions / Blockers`. Collect those results before you change the proof shape, widen the scope, or hand off.
7. Write `Behaviour Specification` in the clearest proof-ready form for this repository. Use feature-file style when that is the best fit, and use repository-native test or scenario style when that is the real proof shape.
8. Build `Mapping to Proof` so each behaviour statement points to exact files, stable scenario or test names, runner commands, and expected observable outcomes.
9. Record `Concrete Steps` that show how to inspect the current behaviour, create or update the proof, rerun it, and safely restart the work if a step fails halfway.
10. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, `Open Questions / Blockers`, and `Next Handoff` up to date as the document evolves. Include one `Progress` item for creating or refreshing the active document and one for keeping required companion documents, catalogs, or artifact notes in sync.
11. Set `Next Handoff` using `Handoffs` when the work is ready for another listed destination or when the question stops being proof-ready specification.
12. Complete `Validation and Acceptance` before you stop. Confirm that the behaviour source is explicit, the vocabulary is stable, the specification is concrete, the proof mapping is exact, and a beginner could implement the proof and the change without making new behaviour decisions.

## Communication Rules

Do not specify behaviour in silence. Record sources, decisions, blockers, rejected interpretations, and changes in direction in the BehaviourSpecDoc as they happen.

Do not hide why the BehaviourSpecDoc owns the task. Record `Trigger for Using This Document` as soon as the owning question is clear, and revise it whenever the trigger facts or reasoning change while the BehaviourSpecDoc remains the correct document.

Let one coordinator own sequencing. The coordinator decides when the behaviour source and vocabulary are stable enough to fan out worker passes, when proof options have been collected, and when the specification should hand off instead of inventing missing behaviour.

Use worker passes only for bounded proof work. A worker pass is one narrow check such as inspecting one proof style, one target test file, one runner command family, or one terminology conflict. Do not let separate worker passes invent competing specifications.

Use the shared living sections as the mailbox for worker results. `Behaviour Vocabulary`, `Mapping to Proof`, `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers` are where partial results wait until the coordinator collects them.

Collect worker results before you settle specification wording, widen the scope, or set `Next Handoff`. Do not move forward from half-collected proof mapping.

Before you settle on specification language, check whether the request has one reasonable meaning or more than one. If it has more than one reasonable meaning, stop and name the competing interpretations instead of silently choosing one.

Even when the request appears to have only one meaning, look for support in the project before you act. Support means existing evidence in the project that points to the same conclusion. Examples include ExampleMappingDocs, nearby tests, callers, documentation, names, examples, and current proof styles.

If you cannot find support and you are not defining new behaviour, pause. Record that the current interpretation is unsupported. Then ask for clarification or record the most likely interpretations and the facts that would confirm each one.

Keep your communication concrete. State what you checked, what the behaviour source is, what the specification says, what proof will show it, what you ruled out, and what still needs to be decided.

## Document-Specific Guidance

### Formatting

Each document written from this guide must be one single fenced code block labeled `md` when it is embedded inside another document or message. When writing the document to a Markdown file where the entire file is only that document, omit the outer triple backticks. Do not nest additional triple-backtick fences inside the document. When you need to show commands, transcripts, diffs, examples, scenarios, or code, present them as indented blocks inside the single `md` fence.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

### Validation, Evidence, and Revision Discipline

Validation is not optional. Include the exact checks, runner commands, traces, or observable signals that prove the document is complete enough for its next handoff. Capture concise evidence such as short transcripts, outputs, diffs, or cited repository facts when they help a beginner restart safely.

When validation needs repository-specific command selection or wider check breadth, consult `.agent/PROJECT.md` and cite the command you chose here.

Specify repository context explicitly. Name files with repository-relative paths, name modules and functions precisely when they matter, and point to exact artifacts or output locations when the guide requires them.

When you revise the document, ensure the revision is reflected across all relevant sections, including `Trigger for Using This Document`, and record the change in `Change Log`. If the document builds on another checked-in artifact, incorporate the needed context directly or reference the exact file and restate the required facts.

### Behaviour Source and Vocabulary

Use `Behaviour Source` to record where the accepted behaviour comes from. Cite exact files, sections, examples, requests, or decisions. If the source is a checked-in ExampleMappingDoc, name it directly. If the source is an explicit new decision, say that clearly and explain why it is safe to proceed.

Use `Behaviour Vocabulary` to lock down domain terms before you write the proof-ready specification. If two terms could be confused, define the difference directly. If the same term is used differently in code, tests, or docs, name the mismatch and record the decision you are following here.

### Behaviour Specification and Proof Mapping

Write `Behaviour Specification` as observable behaviour, not implementation structure. Use concrete scenarios, examples, or rule-backed statements that a beginner can translate into proof without inventing missing inputs or outcomes.

Feature-file style is allowed when it fits the repository. If the real proof will be ordinary tests, describe the behaviour in the repository's test style instead. In both cases, name the exact target files, stable test or scenario names, runner commands, and expected success signals in `Mapping to Proof`.

### Restartability and Background Use

Use the shared `Task and Key Files`, `Trigger for Using This Document`, `Progress`, `Current State Snapshot`, `Concrete Steps`, `Surprises & Discoveries`, `Decision Log`, `Open Questions / Blockers`, `Next Handoff`, `Outcomes & Retrospective`, and `Change Log` sections to keep the document restartable. These sections are also the mailbox where worker-pass results wait until the coordinator collects them. They let the BehaviourSpecDoc stay open as background context while another document owns a different question.

## Skeleton of a Good BehaviourSpecDoc

Use this skeleton when you create a new BehaviourSpecDoc. Keep it complete enough that a complete beginner can continue from the document alone.

    # <Short, action-oriented description>

    This BehaviourSpecDoc is a living document. Keep it up to date as sources are confirmed, vocabulary is refined, specification details change, proof mapping evolves, blockers are resolved, and the next handoff becomes clearer.

    If `.agent/BEHAVIOURS.md` is checked into the repository, maintain this BehaviourSpecDoc in accordance with that file.

    ## Definitions

    Use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.

    ## Status

    Write the current state in one short sentence.

    Examples:

        **Open.** The behaviour source and user-observable boundary are known, and the proof-ready specification is still being drafted.

        **Blocked.** The behaviour source or proof path is still unclear, and safe implementation cannot begin yet.

        **Ready for Handoff.** The specification and proof mapping are complete enough to set `Next Handoff` using `Handoffs`.

        **Resolved.** The behaviour has been implemented and validated, and this specification records the accepted result.

    ## Current State Snapshot

    Write a short summary of where the document stands right now.

    State what is known, what is still unknown, what repository evidence has already been checked, and what a complete beginner should do first if they restart here.

    ## Task and Key Files

    Record the concrete task this document currently owns.

    List the key repository files, commands, tests, catalogs, or companion documents that matter right now.

    List every document, catalog, or artifact that must be created or updated in the same change and keep this section current as the task or handoff changes.

    ## Trigger for Using This Document

    Record the exact trigger that made this BehaviourSpecDoc the correct document.

    State the concrete observed conditions from the request, repository, prior artifact, or observed system state that triggered this document choice.

    Write the full explicit reasoning path from those facts to this document. Do not skip intermediate decision steps.

    Name the nearest competing document types you considered and explain why each one does not own the current unresolved question.

    End with a short replication rule another contributor can follow to reach the same document choice.

    ## Output

    - Primary artifact: A self-contained, proof-ready behaviour specification for one story or change at one user-observable boundary.
    - Primary consumer: The ExecPlan author and the person writing or updating the runnable proof.
    - Ready when: The behaviour source, vocabulary, specification, proof mapping, and validation path are explicit enough for a beginner to implement without inventing behaviour.
    - Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

    ## Progress

    Use a list with checkboxes to summarize the specification work and every meaningful stopping point.

    Include one item for creating or refreshing this BehaviourSpecDoc and one item for keeping required companion documents, catalogs, or artifact notes in sync.

    **Legend**

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [x] (YYYY-MM-DD HH:MMZ) Created or refreshed this BehaviourSpecDoc and updated `Task and Key Files` and `Trigger for Using This Document`.
    - [ ] Keep required companion documents, catalogs, or artifact notes in sync with this BehaviourSpecDoc.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Purpose / Big Picture

    Explain why this behaviour matters and what someone will be able to do or observe once the behaviour is implemented correctly.

    ## Context and Orientation

    Describe the current state that matters for this document as if the reader knows nothing.

    Name the key files, modules, commands, specifications, tests, or entry points that a complete beginner must understand before they continue.

    ## Request Restated

    Restate the request so a complete beginner can answer "yes, that is the behaviour" or "no, that is not what I meant."

    If more than one interpretation is reasonable, list the competing interpretations here.

    ## Scope Boundaries

    State what behaviour is in scope and what behaviour is explicitly out of scope.

    If two nearby behaviours could be confused, name the difference directly.

    ## User-Observable Boundary

    Name the user-observable boundary where the behaviour will be observed and proved.

    Record the exact command, input, request, or action that exercises that boundary when applicable.

    ## Behaviour Source

    Record where the accepted behaviour comes from.

    Cite the exact ExampleMappingDoc, request, documentation, test evidence, or decision that authorizes this specification.

    ## Behaviour Vocabulary

    Define the domain terms that appear in the specification.

    Each term must have one clear meaning.

    If a term maps to data, functions, files, outputs, or test names, name the exact targets.

    ## Behaviour Specification

    Record the proof-ready behaviour in the clearest format for this repository.

    Use feature-file style when it fits the repository, and use repository-native test or scenario style when that is the real proof shape.

    Include concrete inputs, actions, and expected observable outcomes.

    ## Mapping to Proof

    Explain how the behaviour specification will become runnable proof in this repository.

    Name the exact files to create or update.

    For each behaviour statement, record the stable scenario or test name, the runner command, and the expected observable success signal.

    If an existing file provides the style to follow, cite it explicitly.

    ## Concrete Steps

    Record the exact repository checks, files, commands, reruns, and recovery notes needed to derive, verify, and safely restart the specification work.

    Include enough detail that a complete beginner can restart the work safely.

    ## Surprises & Discoveries

    Record anything unexpected you learned while turning the accepted behaviour into a proof-ready specification.

    Include the evidence that revealed it and why it matters.

    ## Decision Log

    Record important specification decisions in this format:

    - Decision: ...
      Rationale: ...
      Evidence: ...
      Date/Author: ...

    ## Validation and Acceptance

    Describe how to prove the BehaviourSpecDoc is complete and usable before implementation starts.

    State the checks that must pass, such as:

    - the behaviour source is explicit,
    - the vocabulary is stable and defined,
    - the specification uses concrete observable outcomes,
    - the proof files, test or scenario names, and runner commands are explicit,
    - a beginner could implement the proof and the change without inventing behaviour.

    ## Open Questions / Blockers

    List only unresolved blocker questions.

    For each blocker, state why it matters, what decision changes based on the answer, and what evidence has already been checked.

    If there are no remaining blockers, say that explicitly.

    ## Next Handoff

    State the next safe handoff using `Handoffs`.

    Name one exact destination listed in `Handoffs`.

    State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

    If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

    ## Outcomes & Retrospective

    State the current outcome in one short paragraph.

    Explain what behaviour is now explicit, what remains unresolved, and what a complete beginner should know before continuing.

    ## Change Log

    Record every revision so a newcomer can see how the document changed over time.

    - YYYY-MM-DD: ...
      Rationale: ...

## Final Reminder

Behaviour specification is for turning accepted behaviour into proof-ready guidance, not for hiding ambiguity or implementation guesses. Use `Document Relationships` to confirm what this guide owns. Use `Handoffs` to choose the next listed destination when the unresolved question changes.
