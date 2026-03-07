# Architecture Decision Records (ADRs)

This document defines the standard for an `ADR`, a living working document used to record one lasting architectural or design decision. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single ADR you provide. There is no memory of prior decisions and no external context.

## Purpose / Big Picture

Use an ADR to turn an important design choice into a durable, reviewable record that explains what was chosen, why it was chosen, what alternatives were seriously considered, what consequences follow from it, and how someone can confirm the decision was applied correctly. Record the request, the scope boundaries, the visible boundary where the decision matters, the exact decision statement, the drivers that shaped it, the considered options, the chosen outcome, the consequences, the related artifacts, and the validation path that proves the decision is real.

An ADR does not diagnose why the current system is wrong, it does not define intended behaviour at a visible boundary, and it does not prescribe implementation sequence. It owns the durable record of one lasting decision. That makes it the document other guides can reference when a design choice must stay explicit over time.

## Output

- Primary artifact: A self-contained record of one architectural or design decision, including its drivers, alternatives, chosen outcome, consequences, and validation path.
- Primary consumer: Future maintainers, reviewers, and implementers who need to understand why the decision exists and how to confirm it is being followed.
- Ready when: The decision statement, drivers, real options, chosen outcome, consequences, related artifacts, and validation path are explicit enough for a beginner to understand and apply the decision without extra context.
- Hands off to: See `Handoffs` for the valid next document or work state and the condition for using it.

## How to Use ADRs and ADRS.md

When you write an ADR, follow `.agent/ADRS.md` to the letter. If it is not in your context, read the entire file before you continue.

Use this guide when one important architectural or design choice needs a durable record. Keep the ADR open while you work. Update it as the decision becomes clearer, tradeoffs are refined, consequences are discovered, validation improves, and follow-up work is identified. Do not treat the ADR as a one-shot template you fill in once and abandon.

Use `Document Relationships` to understand how this guide differs from the other planning guides. Use `Handoffs` to decide whether work should stay here or move to another document or work state.

Store completed ADRs under `docs/adrs/` and name them with four-digit, zero-padded names such as `docs/adrs/0001-short-decision-title.md`.

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

- From `ArchitectureReview`: Use this guide when the review establishes a lasting design choice that must be recorded.
- From `ExecPlan`: Use this guide when implementation reveals a lasting design decision worth preserving.
- From `RefactorPlan`: Use this guide when refactoring reveals a lasting design decision worth preserving.
- From implementation work: Use this guide when code changes create a lasting architectural or design decision that future maintainers must understand.

### Outgoing Handoffs

- To `ExecPlan`: Hand off when the recorded decision needs behaviour-changing implementation steps.
- To `RefactorPlan`: Hand off when the recorded decision needs behaviour-preserving structural work.
- To `ArchitectureReview`: Hand off when the recorded decision needs follow-up system review.
- To implementation work: Hand off when the decision is clear and the next work does not require another planning document first.

### Recording the Handoff

Use the `Next Handoff` section to name one valid next document or work state from `Handoffs` and explain why it applies now.

If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every ADR must be fully self-contained, meaning a complete beginner can continue from the document and the current project files alone.
* Every ADR must be a living document, meaning you update it as decision details, tradeoffs, consequences, validation, and follow-up work become clearer.
* Every ADR must keep the shared skeleton order defined in this guide.
* Every ADR must include an `Output` section that uses the exact four-line template from this guide.
* Every ADR must record one decision at a time.
* Every ADR must restate the request or decision need in concrete language.
* Every ADR must define every technical term in plain language when it first appears.
* Every ADR must state the exact decision being made.
* Every ADR must record the real decision drivers that shaped the choice.
* Every ADR must list only real options that a reasonable engineer could have chosen.
* Every ADR must keep the chosen option first in `Considered Options`.
* Every ADR must record explicit consequences, including gains, costs, and neutral tradeoffs when they matter.
* Every ADR must include a concrete validation path that shows how someone can confirm the decision is applied correctly.
* Every ADR must record linked artifacts, revisit signals, and follow-up work in `More Information`.
* Every ADR must be readable by a complete beginner.
* Every ADR must end with a clear current status and an explicit next handoff.

Treat these rules as mandatory. If one is missing, the ADR is incomplete and the decision is not ready to guide future work safely.

## Workflow

1. Write `Request Restated`, `Scope Boundaries`, and `Visible Boundary` so a complete beginner can see what decision is being made, what it affects, and where its consequences matter.
2. Re-check the repository before you record the decision. Look for existing ADRs, architecture reviews, plans, refactor plans, documentation, tests, and code that already constrain or support the choice.
3. Write `Decision Statement` in concrete language. If more than one decision is hiding in the request, split them into separate ADRs.
4. Write `Decision Drivers` so the actual constraints, goals, risks, or tradeoffs that matter are visible. Drivers must be the reasons the choice matters now, not generic good ideas.
5. Write `Considered Options`. Include only options a reasonable engineer could have chosen in this situation. Keep the chosen option first.
6. Write `Decision Outcome` and `Consequences`. State what was chosen, why it wins against the drivers, what improves, what gets harder, and what stays neutral when that matters.
7. Write `More Information` so the ADR points to related artifacts, follow-up work, revisit triggers, and superseding decisions when applicable.
8. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, `Open Questions / Blockers`, and `Next Handoff` up to date. Set `Next Handoff` using `Handoffs` whenever the ADR reaches a real transition point.
9. Complete `Validation and Acceptance` before you stop. Confirm that the decision is explicit, the options are real, the consequences are concrete, and the validation path shows how to confirm the decision is being followed.

## Communication Rules

Do not record decisions in silence. Capture the drivers, options, consequences, blockers, rejected interpretations, and changes in direction in the ADR as they happen.

Before you settle on the decision statement, check whether the request has one reasonable meaning or more than one. If it has more than one reasonable meaning, stop and name the competing interpretations instead of silently choosing one.

Even when the request appears to have only one meaning, look for support in the project before you act. Support means existing evidence in the project that points to the same decision pressure, tradeoff, or constraint. Examples include architecture reviews, plans, refactor plans, existing ADRs, nearby tests, callers, documentation, and naming patterns.

If you cannot find support and you are not intentionally making a new decision, pause. Record that the current interpretation is unsupported. Then ask for clarification or record the most likely interpretations and the facts that would confirm each one.

Keep your communication concrete. State what decision is being made, what drivers matter, what options were considered, why one option was chosen, what the consequences are, and how the choice will be validated.

## Document-Specific Guidance

### Formatting

Each document written from this guide must be one single fenced code block labeled `md` when it is embedded inside another document or message. When writing the document to a Markdown file where the entire file is only that document, omit the outer triple backticks. Do not nest additional triple-backtick fences inside the document. When you need to show commands, transcripts, diffs, examples, scenarios, or code, present them as indented blocks inside the single `md` fence.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

### Decision Drivers and Real Options

Use `Decision Drivers` to record the actual constraints, goals, risks, or operational pressures that shaped the choice. A driver should be concrete enough that a future reader can see how it influenced the outcome.

Use `Considered Options` to compare like with like. Do not include fake options or options at the wrong abstraction level. If one option is already impossible because of repository facts, either exclude it or explain clearly why it was ruled out before serious consideration.

### MADR Inside the Shared Structure

This guide keeps the useful parts of the MADR approach, but inside the shared living-guide shell used by the rest of the planning system. That means the ADR skeleton should still contain MADR-style decision content such as decision drivers, considered options, outcome, and consequences, while also keeping the shared restartable sections like `Status`, `Current State Snapshot`, `Progress`, `Decision Log`, and `Change Log`.

Keep the chosen option first in `Considered Options`. In `Decision Outcome`, tie the rationale directly to the recorded drivers instead of using generic claims. In `Consequences`, record both what improves and what gets harder.

### Validation, Lifecycle, and Revisit Signals

An ADR is not complete until someone can tell how to confirm the decision is being followed. Validation may be a command, a test, an observable behaviour, a code review checklist, or a repository pattern to inspect. Make it concrete.

Use `More Information` to record linked ADRs, superseded or superseding decisions, follow-up plans, and the signal that would trigger revisiting this ADR. If the ADR changes meaning, record a new ADR and mark the old one as superseded instead of silently rewriting history.

## Skeleton of a Good ADR

Use this skeleton when you create a new ADR. Keep it complete enough that a complete beginner can continue from the document alone.

    # <Short title of solved problem and chosen approach>

    This ADR is a living document. Keep it up to date as the decision becomes clearer, consequences are refined, validation improves, follow-up work becomes explicit, and the next handoff becomes clearer.

    If `.agent/ADRS.md` is checked into the repository, maintain this ADR in accordance with that file.

    ## Status

    Write the current state in one short sentence.

    Examples:

        **Proposed.** The decision is being documented and is not yet accepted.

        **Accepted.** The decision is active and should guide current work.

        **Superseded.** A newer ADR has replaced this decision, and the replacement is recorded below.

    ## Current State Snapshot

    Write a short summary of where the ADR stands right now.

    State what is already known, what is still uncertain, what repository evidence has already been checked, and what a complete beginner should do first if they restart here.

    ## Output

    - Primary artifact: A self-contained record of one architectural or design decision, including its drivers, alternatives, chosen outcome, consequences, and validation path.
    - Primary consumer: Future maintainers, reviewers, and implementers who need to understand why the decision exists and how to confirm it is being followed.
    - Ready when: The decision statement, drivers, real options, chosen outcome, consequences, related artifacts, and validation path are explicit enough for a beginner to understand and apply the decision without extra context.
    - Hands off to: See `Handoffs` for the valid next document or work state and the condition for using it.

    ## Progress

    Use a list with checkboxes to summarize the ADR work and every meaningful stopping point.

    **Legend**

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [x] (YYYY-MM-DD HH:MMZ) Example completed step.
    - [ ] Example incomplete step.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Purpose / Big Picture

    Explain why this decision matters and what someone gains from having it recorded clearly.

    State what future readers will be able to understand or do after reading the ADR.

    ## Context and Orientation

    Describe the current state relevant to this decision as if the reader knows nothing.

    Name the key files, modules, systems, documents, or review artifacts that a complete beginner must understand before they continue.

    Define any non-obvious term you will use.

    ## Request Restated

    Restate the request or decision need so a complete beginner can answer "yes, that is the decision" or "no, that is not what I meant."

    If more than one interpretation is reasonable, list the competing interpretations here.

    ## Scope Boundaries

    State what decision is in scope and what nearby decisions or follow-up work are explicitly out of scope.

    If two nearby choices could be confused, name the difference directly.

    ## Visible Boundary

    Name the visible boundary where the impact of this decision will be observed or validated.

    Record the exact command, behaviour, file pattern, review step, or operational signal that exercises that boundary when applicable.

    ## Decision Statement

    State the exact decision being made.

    Keep it short, concrete, and stable enough that a future reader can quote it.

    ## Decision Drivers

    Record the constraints, quality goals, risks, or tradeoffs that shaped this decision.

    Each driver must be concrete enough that a reader can see how it influenced the outcome.

    ## Considered Options

    List the real options that were seriously considered.

    Put the chosen option first.

    For each option, describe what it means in this repository in one or two short paragraphs or bullet points.

    ## Decision Outcome

    State the chosen option and explain why it wins against the recorded drivers.

    Tie the rationale directly to the drivers instead of using generic claims.

    ## Consequences

    Record the consequences of the decision.

    Include what improves, what gets harder, and any neutral tradeoffs that matter to future readers.

    ## More Information

    Link to related ADRs, plans, reviews, code areas, or follow-up artifacts in this repository.

    Record any follow-up work required for the decision to succeed.

    State when the decision should be revisited and what signal would trigger that revisit.

    ## Surprises & Discoveries

    Record anything unexpected you learned while documenting or validating the decision.

    Include the evidence that revealed it and why it matters.

    ## Decision Log

    Record important ADR-writing decisions in this format:

    - Decision: ...
      Rationale: ...
      Evidence: ...
      Date/Author: ...

    ## Validation and Acceptance

    Describe how to confirm the decision is implemented, being followed, or ready to guide work.

    State the exact command, review step, test, observable behaviour, or file pattern that should be checked.

    If validation is ongoing, say who should check it and when.

    ## Open Questions / Blockers

    List only unresolved blocker questions.

    For each blocker, state why it matters, what decision changes based on the answer, and what evidence has already been checked.

    If there are no remaining blockers, say that explicitly.

    ## Next Handoff

    State the next safe handoff using `Handoffs`.

    Name the exact next document or work state and explain why it applies now.

    If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

    ## Outcomes & Retrospective

    State the current outcome in one short paragraph.

    Explain what decision is now clear, what remains unresolved, and what a complete beginner should know before continuing.

    ## Change Log

    Record every revision so a newcomer can see how the ADR changed over time.

    - YYYY-MM-DD: ...
      Rationale: ...

## Important Reminder

An ADR is for preserving one lasting decision, not for hiding uncertainty or replacing other planning documents. Use `Document Relationships` to confirm what this guide owns. Use `Handoffs` to choose the next document or work state when the question changes.
