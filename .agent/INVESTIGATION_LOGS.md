# Investigation Logs

This document defines the standard for an `InvestigationLog`, a living working document used to diagnose one visible problem whose cause is not yet proven. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single InvestigationLog you provide. There is no memory of prior investigations and no external context.

## Purpose / Big Picture

Use an InvestigationLog to turn an unclear problem into a clear next safe action supported by facts. Record the visible failure, the expected result, the facts you gathered, the interpretation those facts support, the decision you made, and the checks that prove the original problem is resolved or ready to hand off.

An InvestigationLog does not define intended behaviour in the abstract and it does not prescribe the implementation sequence. It owns diagnosis and evidence. That makes it the shared diagnosis layer that an ExampleMappingDoc, a BehaviourSpecDoc, or an ExecPlan can rely on without competing with it.

## Output

- Primary artifact: A self-contained diagnosis of one visible problem, including the proven failure, facts, interpretation, and next safe action.
- Primary consumer: The person clarifying behaviour in an ExampleMappingDoc or BehaviourSpecDoc, or implementing a fix in an ExecPlan.
- Ready when: The failure, expected result, facts, interpretation, and next safe action are explicit at one visible boundary.
- Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

## How to Use InvestigationLogs and INVESTIGATION_LOGS.md

When you write an InvestigationLog, follow `.agent/INVESTIGATION_LOGS.md` to the letter. If it is not in your context, read the entire file before you continue.

Use this guide before you investigate any problem whose cause is not already proven. Start the InvestigationLog before you make meaningful edits to code, tests, settings, data, or expectations. If you already made changes before the investigation started, stop and record what you changed, why you changed it, and what facts existed at that time.

Keep the InvestigationLog open while you work. Record exact commands, outputs, file paths, function names, error lines, reruns, decisions, rejected interpretations, and handoff changes as they happen. Do not treat the log as a summary you write at the end.

Use `Document Relationships` to understand how this guide differs from the other planning guides. Use `Handoffs` to decide whether this document still owns the next unresolved question or whether a listed destination owns it now.

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

This document owns a question only while it is the place where the next missing decision, evidence, or instructions must be added. Use this section to decide whether this document still owns the next unresolved question, which listed destination owns it if not, and what evidence makes the handoff safe now.

### Incoming Handoffs

- From `ExecPlan`: Use this guide when the implementation sequence is already known, and the main unresolved question reopened by execution is what failure or unexpected behaviour is actually happening.
- From `ExampleMappingDoc`: Use this guide when the intended behaviour discussion is already explicit enough to stop mapping, and the main unresolved question is what the system is actually doing or why it is failing.
- From `BehaviourSpecDoc`: Use this guide when the behaviour specification effort is already explicit enough to stop, and the main unresolved question is what the system is actually doing or why it is failing.
- From `RefactorPlan`: Use this guide when the behaviour boundary and structural work are already explicit enough to stop refactor planning, and the main unresolved question reopened by that work is what the system is actually doing or why it is failing.
- From `ArchitectureReview`: Use this guide when the broader system path is already explicit enough to stop architecture review, and the main unresolved question is which failure or unexpected behaviour must be diagnosed at a nearer visible boundary.

### Outgoing Handoffs

- To `ExampleMappingDoc`: Hand off when the failure, facts, and diagnosis are explicit enough to stop diagnosis, and the remaining unresolved question is what behaviour should be accepted at the visible boundary.
- To `BehaviourSpecDoc`: Hand off when the failure, facts, diagnosis, and expected outcome are explicit enough to stop diagnosis, and the remaining unresolved question is how to write the accepted behaviour as a concrete specification and proof path.
- To `ExecPlan`: Hand off when the failure, facts, diagnosis, accepted behaviour, and proof expectations are explicit enough to stop diagnosis, and the remaining unresolved question is how to plan behaviour-changing implementation.
- To `RefactorPlan`: Hand off when the failure, facts, diagnosis, and behaviour boundary are explicit enough to stop diagnosis, and the remaining unresolved question is how to plan behaviour-preserving structural change.

### Recording the Handoff

Use the `Next Handoff` section to name one destination listed in this section.

State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every InvestigationLog must be fully self-contained, meaning a complete beginner can continue from the log and the current project files alone.
* Every InvestigationLog must be a living document, meaning you update it as facts, decisions, checks, and handoffs change.
* Every InvestigationLog must keep the shared skeleton order defined in this guide.
* Every InvestigationLog must include an `Output` section that uses the exact four-line template from this guide.
* Every InvestigationLog must begin before meaningful edits begin, meaning changes to code, tests, settings, stored data, or expected results.
* Every InvestigationLog must define one concrete visible failure at a time.
* Every InvestigationLog must start from the nearest visible boundary where the problem can be observed directly.
* Every InvestigationLog must record how the current state was reached, not just what the current state is.
* Every InvestigationLog must distinguish facts from interpretation.
* Every InvestigationLog must restate the request in concrete language.
* Every InvestigationLog must force an explicit decision before edits continue.
* Every InvestigationLog must handle ambiguity openly.
* Every InvestigationLog must define every technical term in plain language when it first appears.
* Every InvestigationLog must be readable by a complete beginner.
* Every InvestigationLog must reduce an unclear problem into a clear next action supported by facts.
* Every InvestigationLog must end with validation that proves the original problem is resolved or correctly handed off.

Treat these rules as mandatory. If one is missing, the InvestigationLog is incomplete and the investigation is not ready to guide changes, clarification, or handoff.

## Workflow

1. Write `Request Restated` and `Problem Statement` so a complete beginner can see one visible failure in concrete language.
2. Choose the nearest `Visible Boundary`, which is the nearest place where the failure can be observed directly. Examples include a failing test, a public function call, a CLI command, an HTTP endpoint, a file output, or a visible data change.
3. Reproduce the problem at that boundary and record `Failure` before you explain it. Facts come first.
4. Write `Expected Result` in plain language. If the expected result is still unclear, say so explicitly and gather repository evidence before you move further.
5. Gather `Facts` from commands, outputs, file paths, line numbers, callers, nearby tests, documentation, names, stacktraces, or diffs. Follow the execution path only as far as needed to name the next useful check.
6. Re-check the project before deciding what is wrong. Do not assume the first failing output tells the whole story.
7. Write `Interpretation` and `Decision` explicitly. If the question stops being diagnosis, set `Next Handoff` using `Handoffs` before you keep editing.
8. Use `Concrete Steps`, `Progress`, and `Next Handoff` to record exactly how to restart and what should happen next.
9. Complete `Validation and Acceptance` by rerunning the original visible boundary and any wider checks required by the companion guide. Close the investigation only when the original problem no longer happens or the correct handoff is explicit.

## Communication Rules

Do not investigate in silence. Record facts, decisions, blockers, rejected interpretations, and changes in direction in the InvestigationLog as they happen.

Before you act on a request, check whether it has one reasonable meaning or more than one. If it has more than one reasonable meaning, stop and name the competing interpretations instead of silently choosing one.

Even when the request appears to have only one meaning, look for support in the project before you act. Support means existing evidence in the project that points to the same conclusion. Examples include nearby tests, callers, documentation, names, examples, and existing patterns.

If you cannot find support and you are not defining new behaviour, pause. Record that the current interpretation is unsupported. Then ask for clarification or present the most likely interpretations and the facts that would confirm each one.

When a current test, document, user statement, or visible result conflicts with your interpretation, name that conflict directly. Point to the specific command, output, file, function, or line that created the conflict.

Keep your communication concrete. State what you ran, what you saw, what you think it means, what you ruled out, and what you will verify next.

## Document-Specific Guidance

### Formatting

Each document written from this guide must be one single fenced code block labeled `md` when it is embedded inside another document or message. When writing the document to a Markdown file where the entire file is only that document, omit the outer triple backticks. Do not nest additional triple-backtick fences inside the document. When you need to show commands, transcripts, diffs, examples, scenarios, or code, present them as indented blocks inside the single `md` fence.

Write in plain prose. Prefer sentences over lists. Avoid checklists, tables, and long enumerations unless brevity would obscure meaning. Checklists are permitted only in the `Progress` section, where they are mandatory. Narrative sections must remain prose-first.

### Validation, Evidence, and Revision Discipline

Validation is not optional. Include the exact checks, runner commands, traces, or observable signals that prove the document is complete enough for its next handoff. Capture concise evidence such as short transcripts, outputs, diffs, or cited repository facts when they help a beginner restart safely.

Specify repository context explicitly. Name files with repository-relative paths, name modules and functions precisely when they matter, and point to exact artifacts or output locations when the guide requires them.

When you revise the document, ensure the revision is reflected across all relevant sections and record the change in `Change Log`. If the document builds on another checked-in artifact, incorporate the needed context directly or reference the exact file and restate the required facts.

### Start Before More Editing

Use an InvestigationLog when the first wrong step is not yet proven, when the same complaint repeats, or when the next code change looks obvious but being wrong would mean editing the wrong place or preserving the wrong behaviour. Once you can reproduce the visible failure, stop making new changes until the failure, expected result, facts, and current decision are written down clearly.

### Facts First, Interpretation Second

Facts include commands, outputs, return values, logs, nearby tests, callers, documentation, naming patterns, stacktraces, and diffs. Interpretation is the meaning you assign to those facts. Do not present guesses, preferred fixes, or half-formed explanations as facts.

### Restartability and Background Use

Use the shared `Progress`, `Current State Snapshot`, `Concrete Steps`, `Surprises & Discoveries`, `Decision Log`, `Open Questions / Blockers`, `Next Handoff`, `Outcomes & Retrospective`, and `Change Log` sections to keep the log restartable. These sections let the InvestigationLog stay open as background evidence while another document owns a different question.

## Skeleton of a Good InvestigationLog

Use this skeleton when you create a new InvestigationLog. Keep it complete enough that a complete beginner can continue the investigation from the document alone.

    # <Short, action-oriented description>

    This InvestigationLog is a living document. Keep it up to date as facts appear, decisions change, checks succeed or fail, and handoffs become clearer.

    If `.agent/INVESTIGATION_LOGS.md` is checked into the repository, maintain this InvestigationLog in accordance with that file.

    ## Status

    Write the current state in one short sentence.

    Examples:

        **Open.** The problem is reproduced and the cause is still being investigated.

        **Blocked.** The problem is reproduced, but the intended behaviour is still unclear or the environment still blocks diagnosis.

        **Resolved.** The original failure no longer happens and the checks that prove it are recorded below.

    ## Current State Snapshot

    Write a short summary of where the investigation stands right now.

    State what is known, what is still unknown, what has already been ruled out, and what a complete beginner should do first if they restart here.

    ## Output

    - Primary artifact: A self-contained diagnosis of one visible problem, including the proven failure, facts, interpretation, and next safe action.
    - Primary consumer: The person clarifying behaviour in an ExampleMappingDoc or BehaviourSpecDoc, or implementing a fix in an ExecPlan.
    - Ready when: The failure, expected result, facts, interpretation, and next safe action are explicit at one visible boundary.
    - Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

    ## Progress

    Use a list with checkboxes to summarize the investigation work and every meaningful stopping point.

    **Legend**

    [ ] - Not started
    [~] - In progress
    [x] - Completed

    - [x] (YYYY-MM-DD HH:MMZ) Example completed step.
    - [ ] Example incomplete step.
    - [ ] Example partially completed step (completed: X; remaining: Y).

    ## Purpose / Big Picture

    Explain why this investigation matters and what a complete beginner will be able to prove or decide after reading it.

    ## Context and Orientation

    Describe the current state that matters for this investigation as if the reader knows nothing.

    Name the key files, modules, commands, or entry points that a complete beginner must understand before they continue.

    ## Request Restated

    Restate the request so a complete beginner can answer "yes, that is the problem" or "no, that is not what I meant."

    If more than one interpretation is reasonable, list the competing interpretations here.

    ## Scope Boundaries

    State what failure is in scope and what nearby failures, cleanup work, or refactors are explicitly out of scope.

    ## Visible Boundary

    Name the visible boundary where the problem is reproduced.

    Record the exact command, input, request, or action that reproduces the problem.

    ## Problem Statement

    Write one or two sentences that describe one visible failure.

    ## Failure

    Record the concrete failure that proves the problem exists.

    Include exact outputs, error lines, mismatched values, or other visible results.

    ## Expected Result

    Write the expected result in plain language.

    If the expected result is unclear, say that clearly and point to the evidence or blocker that still needs to be resolved.

    ## Facts

    Record the facts you gathered.

    Facts may include commands, outputs, file paths, functions, line numbers, nearby tests, callers, documentation, names, examples, stacktraces, or diffs.

    ## Interpretation

    State what you think the facts mean right now.

    If more than one interpretation is still possible, list them and say what facts would confirm each one.

    ## Decision

    Record the current decision before you make more changes.

    Examples:

        - The current implementation is wrong.
        - The current expectation is wrong.
        - The intended behaviour is still unclear.
        - Setup must be repaired before the real failure can be investigated.

    ## Concrete Steps

    Record the exact steps needed to reproduce, inspect, verify, and safely restart the investigation.

    Include the working directory, commands, reruns, and any recovery notes needed if one step changes the environment or the visible result.

    ## Surprises & Discoveries

    Record anything unexpected you learned while investigating.

    Include the fact that revealed it and why it matters.

    ## Decision Log

    Record important decisions in this format:

    - Decision: ...
      Rationale: ...
      Evidence: ...
      Date/Author: ...

    ## Validation and Acceptance

    Record the checks that prove the original problem no longer happens or that the correct handoff was reached.

    Include the exact rerun, output, or observable result.

    ## Open Questions / Blockers

    Record anything that is still unknown and could change the next safe action.

    If there are no remaining blockers, say that explicitly.

    ## Next Handoff

    State the next safe handoff using `Handoffs`.

    Name one exact destination listed in `Handoffs`.

    State what question this document no longer owns, what question the next destination now owns, and what evidence or completed sections make the handoff safe now.

    If no listed handoff applies yet, stay in the current document and state what is still missing before work can move.

    ## Outcomes & Retrospective

    State the current outcome in one short paragraph.

    Explain whether the original problem is resolved, clarified, blocked, or handed off. State what was learned, what remains unclear, and what a complete beginner should know before continuing.

    ## Change Log

    Record every revision so a newcomer can see how the document changed over time.

    - YYYY-MM-DD: ...
      Rationale: ...

## Final Reminder

Investigation is for proving what is happening, not for guessing what is probably wrong. Use `Document Relationships` to confirm what this guide owns. Use `Handoffs` to choose the next listed destination when the unresolved question changes.
