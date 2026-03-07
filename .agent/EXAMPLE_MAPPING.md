# Example Mappings

This document defines the standard for an `ExampleMappingDoc`, a living working document used to clarify intended behaviour at one user-observable boundary. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single ExampleMappingDoc you provide. There is no memory of prior mappings and no external context.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose / Big Picture

Use an ExampleMappingDoc to turn unclear intended behaviour into explicit, testable behaviour. Record the story, the scope boundaries, the user-observable boundary, the rules that must hold, the concrete examples that show those rules, the open questions or blockers that still matter, and the exact tests or scenarios that should prove the result.

An ExampleMappingDoc does not diagnose why the current system is wrong and it does not prescribe the implementation sequence. It owns intended behaviour at the user-observable boundary. That makes it the behaviour-clarification layer that a BehaviourSpecDoc or an ExecPlan can rely on without competing with it.

## Output

- Primary artifact: A self-contained behaviour-clarification document for one story at one user-observable boundary.
- Primary consumer: The `BehaviourSpecDoc` author and the person writing or updating acceptance tests.
- Ready when: Every rule has concrete example coverage, every blocker is explicit or resolved, and the target tests or scenarios are named.
- Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

## How to Use ExampleMappingDocs and EXAMPLE_MAPPING.md

When you write an ExampleMappingDoc, follow `.agent/EXAMPLE_MAPPING.md` to the letter. If it is not in your context, read the entire file before you continue.

Use this guide before you clarify any feature or story whose intended behaviour is not precise enough to implement safely. Keep the ExampleMappingDoc open while you work. Update it as rules change, examples are added, blockers are resolved, and handoff decisions change. Do not treat the document as a summary you write at the end.

As soon as you choose this guide, write `Trigger for Using This Document`. Record the exact observed trigger facts, the full explicit reasoning path that made `ExampleMappingDoc` the correct document, the nearest competing document types you rejected and why, and a short replication rule a later contributor can reuse. If the owning question changes but the ExampleMappingDoc still owns the work, update that section and record the revision in `Change Log`.

Use `Document Relationships` to understand how this guide differs from the other planning guides. Use `Handoffs` to decide whether this document still owns the next unresolved question or whether a listed destination owns it now.

Use `.agent/OUTPUTS.md` as the source of truth for the ExampleMappingDoc output location and naming rules.

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

- From `InvestigationLog`: Use this guide when the observed problem and likely cause are already explicit, and the main unresolved question is what behaviour should be accepted at one user-observable boundary.
- From `BehaviourSpecDoc`: Use this guide when a specification attempt has already shown the remaining unresolved question is intended behaviour itself, not how to write or prove it.
- From `ExecPlan`: Use this guide when implementation has already shown the remaining unresolved question is intended behaviour at the boundary, not implementation sequence.
- From `RefactorPlan`: Use this guide when refactoring has already shown the remaining unresolved question is intended behaviour at the boundary, not structure.

### Outgoing Handoffs

- To `BehaviourSpecDoc`: Hand off when the story, rules, examples, and acceptance-test mapping are explicit enough to stop behaviour clarification, and the remaining unresolved question is how to turn that accepted behaviour into a concrete specification and proof path.
- To `InvestigationLog`: Hand off when the mapping work shows the remaining unresolved question is what the system is actually doing or why it is failing, not what the intended behaviour should be.

### Recording the Handoff

Use the `Next Handoff` section to name one destination listed in this section.

State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every ExampleMappingDoc must be fully self-contained, meaning a complete beginner can continue from the document and the current project files alone.
* Every ExampleMappingDoc must be a living document, meaning you update it as rules, examples, blockers, decisions, and checks change.
* Every ExampleMappingDoc must keep the shared skeleton order defined in this guide.
* Every ExampleMappingDoc must include an `Output` section that uses the exact four-line template from this guide.
* Every ExampleMappingDoc must include a `Task and Key Files` section that records the concrete task, the key files, and every companion document, catalog, or artifact update that must stay in sync.
* Every ExampleMappingDoc must include a `Trigger for Using This Document` section immediately after `Task and Key Files` and before `Output`.
* Every ExampleMappingDoc must record the exact observed trigger facts, the full explicit reasoning path that made this the correct document, the nearest competing document types that were rejected and why, and a short replication rule another contributor can reuse.
* Every ExampleMappingDoc must revise `Trigger for Using This Document` whenever the trigger facts or reasoning change while the ExampleMappingDoc remains the correct document, and record that revision in `Change Log`.
* Every ExampleMappingDoc must define one feature or story at a time.
* Every ExampleMappingDoc must define one concrete user-observable boundary at a time.
* Every ExampleMappingDoc must restate the request in concrete language.
* Every ExampleMappingDoc must distinguish settled behaviour from open questions or blockers.
* Every ExampleMappingDoc must use repository evidence before inventing new rules.
* Every rule must describe observable behaviour at the chosen user-observable boundary.
* Every example must be concrete and must include explicit inputs, actions, and expected observable outcomes.
* Every example must use `Given`, `When`, `Then`, and `And`.
* Every rule must be covered by at least one example.
* Every example must name the rule or rules it covers.
* Every ExampleMappingDoc must point to the exact test files or scenario files that should implement the mapped behaviour.
* Every ExampleMappingDoc must use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.
* Every ExampleMappingDoc must be readable by a complete beginner.
* Every ExampleMappingDoc must treat creating or refreshing the active document and syncing required companion documents, catalogs, or artifact notes as tracked work in `Progress`.
* Every ExampleMappingDoc must end with a clear current status and an explicit next handoff.

Treat these rules as mandatory. If one is missing, the ExampleMappingDoc is incomplete and the document is not ready to guide implementation or clarification.

## Workflow

1. Write the story in concrete language. If more than one story is hiding in the request, split them into separate ExampleMappingDocs or separate story statements. Record the concrete task, key files, and required companion document updates in `Task and Key Files`, then write `Trigger for Using This Document` as soon as the story is stable enough to name.
2. Write `Request Restated` so a complete beginner can answer "yes, that is the behaviour" or "no, that is not what I meant." If more than one interpretation is reasonable, record the competing interpretations instead of silently choosing one.
3. Choose the nearest `User-Observable Boundary`, which is the nearest place where a person or another part of the system can observe behaviour directly. Examples include a test, a public function call, a CLI command, an HTTP endpoint, a query result, or a visible file output.
4. Write `Scope Boundaries` so the reader can see what is in scope, what is out of scope, and which nearby behaviours must not be confused with this one.
5. Re-check the repository before you write rules. Look for nearby tests, callers, documentation, names, examples, and existing patterns. If the behaviour is best expressed as executable DSL examples in this repository, inspect `research/COMMON_FILTERS.md` and `test/examples/ecto_query_dsl.exs` before you invent new phrasing.
6. After the story, boundary, and scope are stable, let one coordinator fan out bounded worker passes to gather independent examples, existing rules, nearby tests, caller expectations, or accepted wording from the repository. Keep each worker pass narrow enough that it can return one rule candidate or one concrete example without changing the question.
7. Record each worker pass result in the mailbox sections that fit it, such as `Rules`, `Examples`, `Progress`, or `Open Questions / Blockers`. Collect those results before you add new rule language, split the story again, or hand off. Keep one `Progress` item for creating or refreshing the active document and one for keeping required companion documents, catalogs, or artifact notes in sync.
8. Write short, testable `Rules` that describe behaviour a human can verify at the user-observable boundary.
9. Write concrete `Examples` for every rule. Use positive cases, negative cases, and boundary cases where they matter. Record rule coverage in both directions.
10. Map the behaviour to exact tests or scenarios in `Mapping to Acceptance Tests`. Name the target files and the stable test or scenario names.
11. Record only real `Open Questions / Blockers`. If a question can be answered from repository evidence, answer it and convert the answer into a rule or an example.
12. Set `Next Handoff` using `Handoffs` when the question stops being behaviour clarification or becomes ready for another listed destination.
13. Complete `Validation and Acceptance` before you stop. Confirm that every rule has example coverage, every example names its rule coverage, the test mapping is explicit, and the document is usable without inventing missing decisions.

## Communication Rules

Do not map in silence. Record rules, examples, blockers, decisions, rejected interpretations, and changes in direction in the ExampleMappingDoc as they happen.

Do not hide why the ExampleMappingDoc owns the task. Record `Trigger for Using This Document` as soon as the owning question is clear, and revise it whenever the trigger facts or reasoning change while the ExampleMappingDoc remains the correct document.

Let one coordinator own sequencing. The coordinator decides when the story is stable enough to fan out worker passes, when rule and example candidates have been collected, and when the mapping should hand off instead of continuing to guess.

Use worker passes only for bounded collection work. A worker pass is one narrow check such as reading nearby tests, inspecting one caller, comparing one document, or drafting one example family. Do not let separate worker passes invent conflicting story boundaries.

Use the shared living sections as the mailbox for worker results. `Rules`, `Examples`, `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Open Questions / Blockers` are where partial results wait until the coordinator collects them.

Collect worker results before you settle rule wording, widen the story, or set `Next Handoff`. Do not move forward from half-collected examples.

Before you settle on a rule, check whether the request has one reasonable meaning or more than one. If it has more than one reasonable meaning, stop and name the competing interpretations instead of silently choosing one.

Even when the request appears to have only one meaning, look for support in the project before you act. Support means existing evidence in the project that points to the same conclusion. Examples include nearby tests, callers, documentation, names, examples, and existing patterns.

If you cannot find support and you are not defining new behaviour, pause. Record that the current interpretation is unsupported. Then ask for clarification or record the most likely interpretations and the facts that would confirm each one.

Keep your communication concrete. State what you checked, what you learned, what you think it means, what you ruled out, and what still needs to be decided.

## Document-Specific Guidance

### Formatting

Each document written from this guide must be one single fenced code block labeled `md` when it is embedded inside another document or message. When writing the document to a Markdown file where the entire file is only that document, omit the outer triple backticks. Do not nest additional triple-backtick fences inside the document. When you need to show commands, transcripts, diffs, examples, scenarios, or code, present them as indented blocks inside the single `md` fence.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

### Validation, Evidence, and Revision Discipline

Validation is not optional. Include the exact checks, runner commands, traces, or observable signals that prove the document is complete enough for its next handoff. Capture concise evidence such as short transcripts, outputs, diffs, or cited repository facts when they help a beginner restart safely.

When validation needs repository-specific command selection or wider check breadth, consult `.agent/PROJECT.md` and cite the command you chose here.

Specify repository context explicitly. Name files with repository-relative paths, name modules and functions precisely when they matter, and point to exact artifacts or output locations when the guide requires them.

When you revise the document, ensure the revision is reflected across all relevant sections, including `Trigger for Using This Document`, and record the change in `Change Log`. If the document builds on another checked-in artifact, incorporate the needed context directly or reference the exact file and restate the required facts.

### Rules and Examples

Write rules as behaviour, not implementation structure. If two ideas could vary independently, split them into separate rules. Write examples with exact inputs, actions, and expected results so a beginner can turn them into tests without inventing missing decisions.

### Progress and Living Sections

Use the shared `Task and Key Files`, `Trigger for Using This Document`, `Progress`, `Surprises & Discoveries`, `Decision Log`, `Open Questions / Blockers`, `Next Handoff`, `Outcomes & Retrospective`, and `Change Log` sections to show how the mapping evolved over time. These sections are also the mailbox where worker-pass results wait until the coordinator collects them. They let the ExampleMappingDoc stay active in the background while a BehaviourSpecDoc or another document owns a different question.

### Test Mapping and Storage

Use `Mapping to Acceptance Tests` to point to exact files and stable test or scenario names. If an existing file provides the style to follow, cite it directly. Use `.agent/OUTPUTS.md` for the canonical ExampleMappingDoc output location and naming pattern.

## Skeleton of a Good ExampleMappingDoc

Use this skeleton when you create a new ExampleMappingDoc. Keep it complete enough that a complete beginner can continue from the document alone.

    # <Short, action-oriented description>

    This ExampleMappingDoc is a living document. Keep it up to date as rules change, examples are added, blockers are resolved, and the next handoff becomes clearer.

    If `.agent/EXAMPLE_MAPPING.md` is checked into the repository, maintain this ExampleMappingDoc in accordance with that file.

    ## Definitions

    Use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.

    ## Status

    Write the current state in one short sentence.

    Examples:

        **Open.** The story and user-observable boundary are known, and the rules are still being clarified.

        **Blocked.** The story and draft rules are known, but one or more unanswered blockers still prevent safe implementation.

        **Ready for Behaviour Specification.** The rules, examples, and test mapping are complete enough to hand off.

        **Resolved.** The behaviour has been implemented and validated, and this mapping records the accepted result.

    ## Current State Snapshot

    Write a short summary of where the document stands right now.

    State what is known, what is still unknown, what repository evidence has already been checked, and what a complete beginner should do first if they restart here.

    ## Task and Key Files

    Record the concrete task this document currently owns.

    List the key repository files, commands, tests, catalogs, or companion documents that matter right now.

    List every document, catalog, or artifact that must be created or updated in the same change and keep this section current as the task or handoff changes.

    ## Trigger for Using This Document

    Record the exact trigger that made this ExampleMappingDoc the correct document.

    State the concrete observed conditions from the request, repository, prior artifact, or observed system state that triggered this document choice.

    Write the full explicit reasoning path from those facts to this document. Do not skip intermediate decision steps.

    Name the nearest competing document types you considered and explain why each one does not own the current unresolved question.

    End with a short replication rule another contributor can follow to reach the same document choice.

    ## Output

    - Primary artifact: A self-contained behaviour-clarification document for one story at one user-observable boundary.
    - Primary consumer: The `BehaviourSpecDoc` author and the person writing or updating acceptance tests.
    - Ready when: Every rule has concrete example coverage, every blocker is explicit or resolved, and the target tests or scenarios are named.
    - Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

    ## Progress

    Use a list with checkboxes to summarize the mapping work and every meaningful stopping point.

    Include one item for creating or refreshing this ExampleMappingDoc and one item for keeping required companion documents, catalogs, or artifact notes in sync.

    **Legend**

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [x] (YYYY-MM-DD HH:MMZ) Created or refreshed this ExampleMappingDoc and updated `Task and Key Files` and `Trigger for Using This Document`.
    - [ ] Keep required companion documents, catalogs, or artifact notes in sync with this ExampleMappingDoc.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Purpose / Big Picture

    Explain why this story matters and what someone will be able to do or observe once the behaviour is implemented correctly.

    ## Context and Orientation

    Describe the current state that matters for this document as if the reader knows nothing.

    Name the key files, modules, commands, examples, or entry points that a complete beginner must understand before they continue.

    ## Request Restated

    Restate the request so a complete beginner can answer "yes, that is the behaviour" or "no, that is not what I meant."

    If more than one interpretation is reasonable, list the competing interpretations here.

    ## Scope Boundaries

    State what behaviour is in scope and what behaviour is explicitly out of scope.

    If two nearby behaviours could be confused, name the difference directly.

    ## User-Observable Boundary

    Name the user-observable boundary where the behaviour will be clarified and proved.

    Record the exact command, input, request, or action that exercises that boundary when applicable.

    ## Story

    Write one or two sentences that describe who wants what and why.

    ## Rules

    List the rules that must hold at the user-observable boundary.

    Give each rule a stable ID such as `R1`.

    State each rule in ordinary English.

    Record the example IDs that cover each rule.

    ## Examples

    Record concrete examples for the rules.

    Give each example a stable ID such as `E1`.

    Name the rule or rules each example covers.

    Write each example using `Given`, `When`, `Then`, and `And`.

    Include exact inputs and expected observable outcomes.

    ## Mapping to Acceptance Tests

    Explain how the rules and examples will become acceptance tests in this repository.

    Name the exact files to create or update.

    For each example, record the stable scenario or test name and the target file.

    If an existing file provides the style to follow, cite it explicitly.

    ## Concrete Steps

    Record the exact repository checks, files, commands, or examples you used to derive and verify the mapping.

    Include enough detail that a complete beginner can restart the clarification work safely.

    ## Surprises & Discoveries

    Record anything unexpected you learned while clarifying the behaviour.

    Include the evidence that revealed it and why it matters.

    ## Decision Log

    Record important clarification decisions in this format:

    - Decision: ...
      Rationale: ...
      Evidence: ...
      Date/Author: ...

    ## Validation and Acceptance

    Describe how to prove the ExampleMappingDoc is complete and usable before behaviour specification starts.

    State the checks that must pass, such as:

    - every rule has at least one example,
    - every example names its rule coverage,
    - the target tests and scenario names are explicit,
    - a beginner could turn one example into a test without inventing missing decisions.

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

    Explain what behaviour is now clear, what remains unresolved, and what a complete beginner should know before continuing.

    ## Change Log

    Record every revision so a newcomer can see how the document changed over time.

    - YYYY-MM-DD: ...
      Rationale: ...

## Final Reminder

Example mapping is for clarifying behaviour, not for hiding ambiguity. Use `Document Relationships` to confirm what this guide owns. Use `Handoffs` to choose the next listed destination when the unresolved question changes.
