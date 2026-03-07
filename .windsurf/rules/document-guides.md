---
trigger: always_on
---

Use this rule when the current unresolved question is which root `.agent` guide owns the task.

Read only the guide that owns the current unresolved question. If the question changes, open the next matching guide.

When you are not yet sure which root guide owns the task, use `.agent/AGENTS.md` first.

When the chosen guide produces one of the seven root planning artifacts, the resulting document must include `Trigger for Using This Document`. Record the exact observed trigger facts, the full explicit reasoning path that led to that document, the nearby document types that were rejected and why, and a replication rule a later contributor can follow.

Use this routing first:

- Diagnose one visible problem whose cause is not yet proven: open `.agent/INVESTIGATION_LOGS.md`.
- Clarify intended behaviour at one user-observable boundary: open `.agent/EXAMPLE_MAPPING.md`.
- Record a proof-ready behaviour specification at one user-observable boundary: open `.agent/BEHAVIOURS.md`.
- Plan a behaviour-changing implementation sequence: open `.agent/PLANS.md`.
- Plan a behaviour-preserving structural change: open `.agent/REFACTOR_PLANS.md`.
- Review system shape, resilience, state ownership, scaling risk, dependency risk, or failure spread: open `.agent/ARCHITECTURE_REVIEW.md`.
- Record one lasting architectural or design decision: open `.agent/ADRS.md`.
- Create or revise one reusable style rule: open `.agent/CODE_STYLE_RULES.md`.
- Choose project-level command guidance: open `.agent/PROJECT.md`.

Use the entries below when you need the minimum routing detail for one file.

### `.agent/ADRS.md`

Purpose: Record one lasting architectural or design decision.

Use it when: The main unresolved question is which durable decision to record so future work can rely on it.

### `.agent/AGENTS.md`

Purpose: Map the root `.agent` guides and help you choose the correct one.

Use it when: You are not yet sure which root guide owns the current task.

### `.agent/ARCHITECTURE_REVIEW.md`

Purpose: Review system shape, resilience, state ownership, scaling risk, and failure spread.

Use it when: The question is about the system as a whole rather than one isolated bug, one user-observable boundary, or one implementation sequence.

### `.agent/BEHAVIOURS.md`

Purpose: Record a proof-ready behaviour specification at one user-observable boundary.

Use it when: Intended behaviour is already accepted, but do not start implementation planning until the proof path is explicit.

### `.agent/CODE_STYLE_RULES.md`

Purpose: Create or revise one reusable code style rule.

Use it when: The task is to write or update style guidance itself, not merely to apply an existing rule during implementation or review.

### `.agent/EXAMPLE_MAPPING.md`

Purpose: Clarify intended behaviour with rules and concrete examples at one user-observable boundary.

Use it when: The boundary is known, but the expected behaviour is still unclear, incomplete, or disputed.

### `.agent/INVESTIGATION_LOGS.md`

Purpose: Diagnose one visible problem with facts, interpretation, and a clear next safe action.

Use it when: A failure or unexpected result is visible, but the cause is not yet proven.

### `.agent/PLANS.md`

Purpose: Define a concrete implementation sequence for a behaviour-changing result.

Use it when: Diagnosis, intended behaviour, and proof expectations are already explicit enough to plan the code change directly.

### `.agent/PROJECT.md`

Purpose: Hold the authoritative repository command guide for setup, linting, static analysis, coverage, and testing.

Use it when: The current unresolved question is which repository command to run, where to run it, how to repair prerequisites, or how wide validation should be.

### `.agent/REFACTOR_PLANS.md`

Purpose: Define a safe structural change that preserves observable behaviour.

Use it when: The goal is to improve structure without intentionally changing behaviour.
