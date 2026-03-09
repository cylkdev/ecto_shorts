# Execution Plans (ExecPlans)

This document defines the standard for an execution plan, or `ExecPlan`, a living design document that a coding agent or human can follow to deliver a working feature or system change. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single ExecPlan you provide. There is no memory of prior plans and no external context.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose / Big Picture

Use an ExecPlan to turn a clear change request into a concrete implementation sequence that produces a demonstrably working result. Record what someone gains after the change, what files and interfaces must change, what commands must run, what outputs or behaviours should be observed, and what evidence proves the result is real.

An ExecPlan does not own diagnosis and it does not define intended behaviour from scratch. It owns implementation sequence and validation. That makes it the execution layer that follows a BehaviourSpecDoc and can rely on supporting InvestigationLogs or ExampleMappingDocs without competing with them.

## Output

- Primary artifact: A self-contained implementation specification for one change, including edits, commands, and proof of the result.
- Primary consumer: The implementer carrying out the change end-to-end.
- Ready when: The execution sequence, interfaces, concrete steps, and validation path are explicit enough for a novice to produce a working result.
- Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

## How to Use ExecPlans and PLANS.md

When you author an ExecPlan, follow `.agent/PLANS.md` to the letter. If it is not in your context, read the entire file before you continue.

Use this guide when the desired behaviour and proof mapping are already clear enough to implement. Keep the ExecPlan open while you work. Update it as progress is made, discoveries occur, decisions are finalized, blockers appear, and the handoff state changes. It should always be possible to restart from only the ExecPlan and the working tree.

As soon as you choose this guide, write `Trigger for Using This Document`. Record the exact observed trigger facts, the full explicit reasoning path that made `ExecPlan` the correct document, the nearest competing document types you rejected and why, and a short replication rule a later contributor can reuse. If the owning question changes but the ExecPlan still owns the work, update that section and record the revision in `Change Log`.

Use `Document Relationships` to understand how this guide differs from the other planning guides. Use `Handoffs` to decide whether this document still owns the next unresolved question or whether a listed destination owns it now.

When implementing an approved ExecPlan, do not stop for vague "next steps." Proceed to the next milestone unless a real external blocker appears. Resolve ambiguity by inspecting the code, tests, and current plan evidence, then record the result in the plan.

Use `.agent/OUTPUTS.md` as the source of truth for the ExecPlan output location and naming rules.

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

- From `BehaviourSpecDoc`: Use this guide when the accepted behaviour and proof path are already explicit, and the main unresolved question is how to sequence behaviour-changing implementation.
- From `InvestigationLog`: Use this guide when the failure diagnosis, accepted behaviour, and proof expectations are already explicit, and the main unresolved question is how to sequence behaviour-changing implementation.
- From `ArchitectureReview`: Use this guide when the system risk and mitigation direction are already explicit, and the main unresolved question is how to plan behaviour-changing implementation that reduces that risk.
- From `ADR`: Use this guide when the lasting design choice is already recorded, and the main unresolved question is how to plan behaviour-changing implementation that applies that decision.

### Outgoing Handoffs

- To direct behaviour-changing implementation changes in the working tree: Hand off when the files, edits, commands, validation, and restart instructions are explicit enough to stop planning, and the remaining unresolved question is how to carry out those behaviour-changing changes in the working tree.
- To `InvestigationLog`: Hand off when the plan is explicit enough to show the remaining unresolved question is what failure or unexpected behaviour is actually happening, not how to sequence implementation.
- To `ExampleMappingDoc`: Hand off when the plan is explicit enough to show the remaining unresolved question is what behaviour should be accepted at the user-observable boundary, not how to sequence implementation.
- To `BehaviourSpecDoc`: Hand off when the plan is explicit enough to show the remaining unresolved question is how accepted behaviour should be specified and proved, not how to sequence implementation.
- To `ADR`: Hand off when the plan is explicit enough to show the remaining unresolved question is which lasting architectural or design decision must be recorded for future work.

### Recording the Handoff

Use the `Next Handoff` section to name one destination listed in this section.

State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every ExecPlan must be fully self-contained, meaning that in its current form it contains all knowledge and instructions needed for a novice to succeed.
* Every ExecPlan must be a living document, meaning you revise it as progress is made, discoveries occur, decisions are finalized, blockers appear, and outcomes become clear.
* Every ExecPlan must keep the shared skeleton order defined in this guide.
* Every ExecPlan must include an `Output` section that uses the exact four-line template from this guide.
* Every ExecPlan must include a `Task and Key Files` section that records the concrete task, the key files, and every companion document, catalog, or artifact update that must stay in sync.
* Every ExecPlan must include a `Trigger for Using This Document` section immediately after `Task and Key Files` and before `Output`.
* Every ExecPlan must record the exact observed trigger facts, the full explicit reasoning path that made this the correct document, the nearest competing document types that were rejected and why, and a short replication rule another contributor can reuse.
* Every ExecPlan must revise `Trigger for Using This Document` whenever the trigger facts or reasoning change while the ExecPlan remains the correct document, and record that revision in `Change Log`.
* Every ExecPlan must enable a complete novice to implement the change end-to-end without prior knowledge of this repository.
* Every ExecPlan must produce a demonstrably working behaviour, not merely code changes that appear to satisfy a definition.
* Every ExecPlan must define every term of art in plain language before it is used.
* Every ExecPlan must restate the request in concrete language.
* Every ExecPlan must define `Scope Boundaries` and a `User-Observable Boundary`.
* Every ExecPlan must name the files, modules, functions, commands, and outputs precisely enough that a beginner can follow them.
* Every ExecPlan must include exact validation instructions and the expected observable result.
* Every ExecPlan must be safe to restart. If a step can fail halfway or is risky, the plan must describe how to retry, adapt, or recover.
* Every ExecPlan must treat creating or refreshing the active document and syncing required companion documents, catalogs, or artifact notes as tracked work in `Progress`.
* Every ExecPlan must keep `Task and Key Files`, `Trigger for Using This Document`, `Progress`, `Surprises & Discoveries`, `Decision Log`, `Open Questions / Blockers`, `Next Handoff`, `Outcomes & Retrospective`, and `Change Log` up to date.

Treat these rules as mandatory. If one is missing, the ExecPlan is incomplete and the implementation is not ready to proceed safely.

## Workflow

1. Write `Purpose / Big Picture`, `Output`, `Current State Snapshot`, `Task and Key Files`, and `Trigger for Using This Document` so the implementer understands what will exist after the change, what is already true in the repository, why this document owns the task, and which files and companion documents must stay in sync.
2. Write `Request Restated`, `Scope Boundaries`, and `User-Observable Boundary` so the implementer knows exactly what problem is being solved, what is out of scope, and where the result will be observed.
3. Build `Plan of Work` in concrete prose. Name the files, modules, functions, and interfaces that must change. State what to insert, remove, or update, and why.
4. Let one coordinator own the implementation sequence. Once the boundary, scope, and target files are explicit, fan out bounded worker passes only for independent read-only checks such as callers, interfaces, tests, or configuration. Collect their results into `Progress`, `Surprises & Discoveries`, `Decision Log`, or `Open Questions / Blockers` before you lock the next milestone.
5. Write `Interfaces and Dependencies` so the implementer knows which modules, behaviours, function signatures, libraries, or services must exist at the end.
6. Write `Concrete Steps` with the exact working directory, commands, sequencing, idempotence notes, and expected short transcripts when they matter.
7. Use `Progress` to track granular work at every stopping point. Include one item for creating or refreshing the active document and one item for keeping required companion documents, catalogs, or artifact notes in sync. If a task is partially complete, split it into completed and remaining work instead of leaving it vague.
8. Record unexpected findings in `Surprises & Discoveries` and important choices in `Decision Log` as they happen.
9. Keep `Open Questions / Blockers` and `Next Handoff` explicit. Set `Next Handoff` using `Handoffs` whenever implementation reveals a new question instead of improvising past the ambiguity.
10. Finish with `Validation and Acceptance`, `Outcomes & Retrospective`, and `Change Log`. The plan is complete only when a beginner can follow it to a working, observable result.

## Communication Rules

Do not write an ExecPlan in silence. Record decisions, blockers, rejected approaches, changes in direction, and important evidence in the plan as they happen.

Do not hide why the ExecPlan owns the task. Record `Trigger for Using This Document` as soon as the owning question is clear, and revise it whenever the trigger facts or reasoning change while the ExecPlan remains the correct document.

When ambiguity exists, resolve it in the plan itself when you can do so from repository evidence. If the ambiguity belongs to diagnosis, intended behaviour, or proof-ready specification rather than implementation, set `Next Handoff` using `Handoffs` instead of hiding the question inside the ExecPlan.

Keep one coordinator responsible for sequencing. A worker pass is one bounded inspection or drafting task that can run independently after the shared prerequisite is fixed. Use `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers` as the plan mailbox. Collect worker-pass results there before the coordinator changes the sequence, chooses a milestone, or sets `Next Handoff`.

Keep the plan concrete. State what the user will be able to do, what files will change, what commands will run, what outputs should appear, and what will prove success.

Do not outsource key decisions to the implementer. If the plan depends on an assumption, write the assumption down. If the assumption is risky, make it an explicit blocker or handoff.

## Document-Specific Guidance

### Formatting

Each ExecPlan must be one single fenced code block labeled `md` when it is embedded inside another document or message. When writing an ExecPlan to a Markdown file where the entire file is only the plan, omit the outer triple backticks. Do not nest additional triple-backtick fences inside the plan. When you need to show commands, transcripts, diffs, or code, present them as indented blocks inside the single `md` fence.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

### Milestones, Prototypes, Parallel Work, and Collection

Milestones are narrative, not bureaucracy. If you break the work into milestones, introduce each one with a brief paragraph that describes the scope, what will exist at the end of the milestone that did not exist before, the commands to run, and the acceptance you expect to observe.

Prototyping milestones are acceptable when they reduce risk. Keep prototypes additive and testable. Clearly label the scope as prototyping, describe how to run and observe results, and state the criteria for promoting or discarding the prototype. Parallel implementations are acceptable when they reduce risk or keep tests passing during a larger migration. Explain how both paths will be validated and how one path will be retired safely.

Use actor-style parallel work only after the owning question is fixed. In this guide, the `Coordinator` is the single owner of milestone order and next-step decisions. A `Worker pass` is one bounded, independently runnable inspection or drafting task. `Fan-out` means starting several worker passes only after the shared prerequisite is fixed. The plan mailbox is the maintained state in `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers`. `Collect` means merging worker-pass findings back into that mailbox before you branch again.

### Validation, Evidence, and Revision Discipline

Validation is not optional. Include instructions to run tests, to start the system if applicable, and to observe it doing something useful. Use `.agent/PROJECT.md` as the source of truth for the repository root, the exact validation commands, prerequisite-repair steps, and the widening order for this repository. Cite the exact commands you chose from its `Command Surface` and `Recommended Validation Paths` sections. Capture concise evidence such as short transcripts, outputs, or diffs that prove success.

Specify repository context explicitly. Name files with repository-relative paths, name modules and functions precisely, and describe where new files should be created. For Elixir, prefer explicit paths such as `lib/my_app/...` and `test/...`, and name functions with arity such as `MyApp.Module.function/arity`.

When you revise an ExecPlan, ensure the revision is reflected across all relevant sections, including `Trigger for Using This Document`, and record the revision in `Change Log`. If the plan builds upon another checked-in plan, incorporate the needed context directly or reference the exact file and restate the required facts.

## Skeleton of a Good ExecPlan

Use this skeleton when you create a new ExecPlan. Keep it complete enough that a complete beginner can implement the change from the document alone.

    # <Short, action-oriented description>

    This ExecPlan is a living document. Keep it up to date as progress is made, discoveries occur, decisions are finalized, blockers appear, and outcomes become clear.

    If `.agent/PLANS.md` is checked into the repository, maintain this ExecPlan in accordance with that file.

    ## Definitions

    Use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.

    ## Status

    The status can be one of:

    * `Proposed`: The plan exists as a written proposal. Work has not started.
    * `In Progress`: Implementation of the plan has started and the work is actively being carried out.
    * `Complete`: All planned work has been finished and no remaining planned changes are expected.
    * `Abandoned`: Work on the plan has been stopped without completion.

    Write the status in bold and follow it with one full sentence that describes the current state in plain language.

    Example:  

        **Proposed.** The plan has been written and work has not started.

    ## Milestones

    ### Milestone 1: Define the scope, expected behavior, and completion criteria

    Use this milestone to define exactly how email verification must work before you begin implementation. Clarify the expected user experience, the system behavior, and the conditions that will determine whether the work is complete. This matters because you cannot implement backend or frontend changes correctly until the required behavior is fully defined. By the end of this milestone, you should have a clear understanding of how the feature must work and how success will be measured.

    #### Task 1.1: Confirm how the signup flow should work

    - Determine when the verification email must be sent.
    - Determine whether unverified users are allowed to sign in.
    - Determine what must happen when a verification link expires.
    - Determine how resend verification must work.
    - Complete this task before you begin backend or frontend implementation, because it defines the behavior the system must support.

    #### Task 1.2: Identify the technical dependencies

    - Review the authentication code.
    - Review the email delivery integration.
    - Review the user model fields related to account state.
    - Review the routes and pages involved in signup and account activation.
    - Complete this task before implementation begins, because it identifies the systems, dependencies, and areas of the codebase that will be affected.

    ### Milestone 2: Implement the backend changes

    Use this milestone to build the backend support required for email verification. This includes the data model, token lifecycle, and delivery of the verification email. This matters because the backend defines the core verification flow and provides the functionality the frontend depends on. By the end of this milestone, you should have a working backend implementation that can generate verification tokens, store the required verification state, and send valid verification emails.

    #### Task 2.1: Add data model support

    - Add a field for email verification status.
    - Add a field for the verification token or token reference.
    - Add a field for token expiry time.
    - This task depends on Milestone 1, because the data model must reflect the behavior you defined there.
    - Complete this task before you implement token handling, verification handling, or resend support.

    #### Task 2.2: Implement token generation and storage

    - Add a secure token generation utility.
    - Store the token safely.
    - Set an expiration time.
    - Connect token generation to the signup flow.
    - This task depends on Task 2.1, because the data model must exist before tokens can be stored.
    - Complete this task before you send the verification email or implement verification handling.

    #### Task 2.3: Send the verification email

    - Create the email content.
    - Add the verification link.
    - Trigger email sending after successful signup.
    - Verify delivery in development.
    - This task depends on Task 2.2, because the email requires a valid verification token and link.

    ## Output

    - Primary artifact: A self-contained implementation specification for one change, including edits, commands, and proof of the result.
    - Primary consumer: The implementer carrying out the change end-to-end.
    - Ready when: The execution sequence, interfaces, concrete steps, and validation path are explicit enough for a novice to produce a working result.
    - Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

    ## Progress

    Use a list with checkboxes to summarize granular work and every meaningful stopping point.

    Include one item for creating or refreshing this ExecPlan and one item for keeping required companion documents, catalogs, or artifact notes in sync.

    **Legend**

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [x] (YYYY-MM-DD HH:MMZ) Created or refreshed this ExecPlan and updated `Task and Key Files` and `Trigger for Using This Document`.
    - [ ] Keep required companion documents, catalogs, or artifact notes in sync with this ExecPlan.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    Use timestamps so a future contributor can see the sequence of work.

    ## Purpose / Big Picture

    Explain what someone gains after this change and how they can see it working.

    State the user-visible behaviour you will enable.

    ## Context and Orientation

    Describe the current state relevant to this task as if the reader knows nothing.

    Name the key files, modules, commands, and entry points by full repository-relative path.

    Use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.

    ## Request Restated

    Restate the request so a complete beginner can answer "yes, that is the change" or "no, that is not what I meant."

    If more than one interpretation is still possible, state the chosen interpretation and why it is the correct one for this plan.

    ## Task and Key Files

    Record the concrete task this plan currently owns.

    List the key repository files, commands, tests, catalogs, or companion documents that matter right now.

    List every document, catalog, or artifact that must be created or updated in the same change and keep this section current as the task or handoff changes.

    ## Trigger for Using This Document

    Record the exact trigger that made this ExecPlan the correct document.

    State the concrete observed conditions from the request, repository, prior artifact, or observed system state that triggered this document choice.

    Write the full explicit reasoning path from those facts to this document. Do not skip intermediate decision steps.

    Name the nearest competing document types you considered and explain why each one does not own the current unresolved question.

    End with a short replication rule another contributor can follow to reach the same document choice.

    ## Scope Boundaries

    State what work is in scope and what work is explicitly out of scope.

    If nearby follow-up work is intentionally deferred, say so directly.

    ## User-Observable Boundary

    Name the boundary where the final behaviour will be observed and proved.

    Record the exact command, request, or action that exercises that boundary when applicable.

    ## Plan of Work

    Describe, in prose, the sequence of edits and additions.

    For each edit, name the file and location and what to insert or change.

    Keep the description concrete and minimal.

    ## Interfaces and Dependencies

    Be prescriptive.

    Name the libraries, modules, services, behaviours, and function signatures that must exist at the end of the work, and explain why they are required.

    Prefer stable names such as `MyApp.Module.function/arity` and concrete file paths such as `lib/my_app/module.ex`.

    ## Concrete Steps

    State the exact commands to run and where to run them.

    Include expected short transcripts when they help the reader compare results.

    State any retry, rollback, or recovery notes needed if a step can fail halfway.

    ## Surprises & Discoveries

    Document unexpected behaviours, bugs, tradeoffs, or insights discovered during implementation.

    Provide concise evidence.

    - Observation: ...
      Evidence: ...

    ## Decision Log

    Record every decision made while working on the plan in this format:

    - Decision: ...
      Rationale: ...
      Date/Author: ...

    ## Validation and Acceptance

    Describe how to exercise the system and what to observe.

    Phrase acceptance as behaviour with specific inputs and outputs.

    If tests are involved, name the exact test commands and what should pass.

    ## Open Questions / Blockers

    Record anything still unknown that could change the implementation sequence or the validation path.

    If there are no remaining blockers, say that explicitly.

    ## Current State Snapshot

    Write a short summary of where the plan stands right now.

    State what is already known, what is still undecided, what repository evidence has already been checked, and what a complete beginner should do first if they restart here.

    ## Next Handoff

    State the next safe handoff using `Handoffs`.

    Name one exact destination listed in `Handoffs`.

    State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

    If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

    ## Outcomes & Retrospective

    Summarize what was achieved, what remains, and what was learned.

    Compare the result against the original purpose.

    ## Change Log

    Record every revision so a newcomer can see how the plan changed over time.

    - YYYY-MM-DD: ...
      Rationale: ...

## Final Reminder

An ExecPlan is for delivering working behaviour, not for leaving key implementation choices to the next person. Use `Document Relationships` to confirm what this guide owns. Use `Handoffs` to choose the next listed destination when the unresolved question changes.
