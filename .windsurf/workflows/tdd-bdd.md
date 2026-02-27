---
name: tdd-bdd
description: Use these workflow steps to deliver a feature or system change.
auto_execution_mode: 3
---

// turbo-all

## Purpose / Big Picture

This document explains a workflow process that can be used to deliver a working feature or system change in a reproducible way by combining behaviour-first thinking with test-driven implementation.

The process is best understood as a set of stages and loops, not a strict one-way sequence.

In some tasks, you will move through these stages in order. In other tasks, you may revisit earlier stages when new information appears. That is normal and often necessary.

## Glossary

A `boundary` is any place where behaviour can be observed from outside the implementation. This might be an HTTP endpoint, UI action, CLI command, message handler, job execution, public API call, file output, or any other externally visible interaction.

A `behaviour specification` is any written description of expected behaviour at a boundary. It can be a formal document, a ticket with scenarios, acceptance tests, or another shared format. The important part is that it describes observable outcomes clearly.

An `example document` is a way to clarify ambiguous behaviour before implementation. It captures rules and concrete examples so people can agree on what the system should do in specific situations.

An `execution plan` is a practical implementation outline that translates behaviour into work. It may include milestones, likely file changes, commands, checkpoints, and verification steps.

An `architecture decision record (ADR)` is a record of an important design decision, including the reason it was made and the tradeoffs considered.

A `refactor plan` (or refactor document) is a guide for changing structure while preserving behaviour. It helps keep the work safe by naming the preserved boundary and the checks used during the change.

A `boundary test` is an automated test that exercises the system through an external interface or near-external interface. The exact form depends on the system.

A `focused test` is an automated test for a smaller unit of logic. It is usually used after a boundary test reveals a missing piece of behaviour that is easier to drive at a narrower level.

`Red` means a test fails for a meaningful reason related to missing or incorrect behaviour.

`Green` means the relevant test now passes after a minimal change.

`Refactor` means the structure changes while the intended observable behaviour remains the same.

A `seam` is a useful handoff point where it becomes easier to continue progress with a more focused test instead of only driving from the boundary.

## Guidelines

Use the documents described in this repository (`BehaviourSpec`, `ExampleDoc`, `ExecPlan`, `ADR`, and `RefactorDoc`) to capture all of your work.

When refactoring, define what must remain true at the boundary. Once that is explicit, the internal changes can proceed in small steps with repeated checks.

Avoid common failure modes by clarifying the expected behaviour before implementation. Do not start implementation while the expected behaviour is still unclear.

When multiple reasonable interpretations exist, examples are often the fastest way to reach agreement. A few concrete examples can expose hidden assumptions much earlier than code can.

Behaviour is usually ready for implementation when the expected outcome at the boundary can be described clearly enough that different readers are likely to write the same test for it.

When you are making a decision that affect the public interface or the user-observable behaviour, record the decision and rationale in an ADR.

### Common stages

A task usually starts by clarifying what change is being requested and what outcome should be observable.

Next, the work is framed. Some changes are mostly research, some are behaviour changes, and some are structural cleanup with no intended behaviour change.

If the expected behaviour is unclear, examples and rules are used to make the meaning concrete before implementation starts.

Once the behaviour is clear enough, the work is planned at a practical level. This does not require a detailed plan for every task. Small changes may only need a short note. Larger changes benefit from milestones and explicit verification steps.

During implementation, tests and observations provide feedback. The work often moves between boundary-level proof and focused logic-level proof.

As the work progresses, documents or notes are updated so the current state can be understood without relying on memory.

At the end, the change is verified using the checks that prove the intended behaviour (or preserved behaviour, in a refactor) is true.

## What to do

Use the steps in this workflow to guide your work. It is not a strict sequence. Start from the point you need to solve your problem and follow the steps from that point forward.

Your overall task is divided into three levels:

Task-level
  |--> Milestone-level
       |--> Test-level

The task level encompases the entire workflow, from request to proven result. The milestone level is inside the task level and handles one small slice of behaviour or one safe unit of structural change. The test level is inside the milestone level and is the smallest TDD loop.

Move between these levels as needed applying TDD and BDD principles.

### 1. Task-level

This is the highest level of the workflow. It is the loop from request to proven result.

Clarify the request, identify what kind of change it is, define or refine behaviour, implement in small steps, verify outcomes, and update the record of what happened.

New ambiguity may appear during implementation. A design choice may require a decision record. A refactor may expose a hidden behaviour dependency. When that happens, return to the earlier stage that resolves the uncertainty, then continue.

### 2. Milestone-level

This level is inside the task-level loop. It handles one small slice of behaviour or one safe unit of structural change.

A milestone starts with a specific proof target. That target might be one scenario, one edge case, one API contract detail, or one preserved behaviour during refactoring.

The milestone is complete when there is reproducible evidence that the target is satisfied and the related notes or plan reflect what was learned.

### 3. Test-level

This level is inside the milestone-level loop. It is the smallest TDD loop.

You write or choose a failing check that expresses one missing fact, make the smallest change that can satisfy it, and then improve structure while keeping the check green.

This loop can happen at the boundary level or in a focused unit, depending on where the next useful proof is easiest to express.

## How to understand the workflow

This workflow should be understood as a set of connected feedback loops, not a fixed sequence of steps.

The main idea is to start from a requested change, make the expected behaviour clear, use tests and observations to grow the implementation in small pieces, and keep any supporting notes current so the work stays understandable.

### Task-level flow from request to proven result

A task begins with a request and moves toward a proven result, but it does not need to pass through a rigid checklist in one direction.

    Request
      |
      v
    clarify meaning <-> refine scope <-> explore examples
            \              |              /
              \             v             /
              +------ record what matters ------+
                              |
                              v
                        implement small changes
                              |
                              v
                        verify behaviour
                              |
                              v
                          finalize result
                              ^
                              |
                      update records as you learn

In practice, the work moves between a few recurring concerns.

One concern is clarity. This is where the request is restated in simple terms so the intended outcome is easy to recognize. If the request contains multiple changes, it helps to separate them so each one can be reasoned about on its own.

Another concern is task shape. Some work changes behaviour, some improves structure while preserving behaviour, and some is mostly research. Naming the kind of work helps choose the right proof and the right level of documentation, but it is only a framing tool, not a gate.

Another concern is behaviour discovery. When expected behaviour is ambiguous, examples and rules make it concrete. This can happen before coding starts or later during implementation when a hidden case appears.

Another concern is documentation. Documents are not a stage that happens once. They are a running record of the current understanding, the chosen boundary, the plan for the next slice of work, and the proof used to verify results.

Implementation and verification are also not separate worlds. Small changes are made, checked, refined, and checked again. The task is complete when the intended behaviour is proven (or preserved, in a refactor) and the records match what actually happened.

### Milestone-level inside implementation

A milestone is one small, provable slice of work. It may be a behaviour scenario, an edge case, a contract detail, or one safe structural change in a refactor.

    current milestone
          |
          v
    choose a proof at the boundary
          |
          v
    use red -> green -> refactor
          |
          v
      capture evidence
          |
          v
    update notes / plan / records
          |
          v
    next milestone or done

The milestone loop is about keeping progress small and visible.

Each milestone starts with a proof target. The proof target is one thing that can be observed from outside the implementation, such as an endpoint response, a CLI result, a job outcome, or a public function contract.

The implementation then grows through short TDD cycles until that proof passes. Some milestones are completed mostly from boundary tests. Others move inward to focused tests after a boundary test reveals the next missing piece of logic.

Once the milestone is proven, the useful evidence is captured in a reproducible form, and the active records are updated so the current state is visible without relying on memory.

The loop repeats until the task has enough proven slices to satisfy the requested change.

### Micro loop inside red -> green -> refactor

This is the smallest feedback loop. It handles one missing behaviour fact at a time.

    express one missing fact as a failing check
                |
                v
              run it
                |
                v
        see the expected failure
                |
                v
      make the smallest code change
                |
                v
          run the same check
                |
                v
              see it pass
                |
                v
      improve structure safely
                |
                v
        re-run checks, still pass

This loop is intentionally small because small loops make mistakes easier to detect and fix.

The check should express one missing fact, not several at once. The code change should be the smallest change that can satisfy that fact. After the check passes, structure can be improved without changing the proven behaviour.

This same loop can be used in any language, framework, or test layer. The exact tools change, but the concept stays the same: use a failing proof to guide the next change, make it pass, then clean up while keeping the proof true.

### How the flow works as a whole

These three levels work together.

The task-level flow keeps the work tied to a real requested outcome.

The milestone loop keeps progress small enough to verify and explain.

The micro loop keeps each code change grounded in evidence.

Because the process uses small sets of loops, it adapts to different situations without forcing a single path. A simple fix may move through all three levels quickly. A larger change may revisit behaviour discovery, adjust records, and redefine milestones several times before the final result is proven.