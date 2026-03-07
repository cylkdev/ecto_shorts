# Code Style Rule Documents (CodeStyleRuleDocs)

This document defines the standard for a `CodeStyleRuleDoc`, a living reference document used to record one reusable code style rule. Treat the reader as a complete beginner to this repository: they have only the current working tree, this guide, and the single CodeStyleRuleDoc you provide. There is no memory of prior rule discussions and no external context.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose / Big Picture

Use a CodeStyleRuleDoc to turn a recurring code problem into a short, reusable rule that teaches one thing clearly. Record the discouraged code shape, the concrete cost it creates, the preferred code shape, and the small amount of explanation a beginner needs in order to recognize the difference in real code.

A CodeStyleRuleDoc does not diagnose a live failure, it does not define intended behaviour at a user-observable boundary, and it does not prescribe implementation or refactor sequence. It owns reusable code-writing guidance. That makes it the correct guide when the real task is to capture a repeated pattern so humans and coding agents can write or review code consistently.

## Output

- Primary artifact: A short, self-contained style rule stored in the canonical CodeStyleRuleDoc location defined in `.agent/OUTPUTS.md`, with a title, `**Problem**`, `**Example**`, and `**Refactoring**`.
- Primary consumer: A coding agent or human novice who needs to write, review, or revise code without guessing the preferred shape.
- Ready when: The rule name, concrete problem, bad example, corrected example, plain-language reasoning, category placement, and catalog update are explicit enough for a beginner to apply the rule without extra context.
- Hands off to: See `Handoffs` for the valid next destination, the unresolved question that moves there, and the evidence that makes the handoff safe.

## How to Use CodeStyleRuleDocs and CODE_STYLE_RULES.md

When you write or revise a CodeStyleRuleDoc, follow `.agent/CODE_STYLE_RULES.md` to the letter. If it is not in your context, read the entire file before you continue.

When you need the correct category before drafting the rule, use `.agent/styles/AGENTS.md` first. If no category fits, create the new category and update `.agent/styles/AGENTS.md` in the same change as the new rule.

Before you draft or revise a rule, update the surrounding active document. Record the concrete task, the key files, and every required rule, catalog, or companion-document update in that document's `Task and Key Files` section.

Keep the related document maintenance task visible in the surrounding active document's `Progress` section or checklist so rule creation and catalog sync stay explicit.

Use this guide when you already know the repeated code pattern you want to teach. Keep the guide open while you work. Narrow the rule, name it, shape the bad and corrected examples, and revise the explanation until a complete beginner could follow it without outside help. Do not treat the rule as a private note or a one-shot summary.

Use `Document Relationships` to understand how this guide differs from the other root `.agent` guides. Use `Handoffs` to decide whether this document still owns the next unresolved question or whether a listed destination owns it now.

Use `.agent/OUTPUTS.md` as the source of truth for the CodeStyleRuleDoc output location and naming rules. Use `.agent/styles/AGENTS.md` to choose the category and keep the catalog aligned with the rule file.

## Safe Parallel Document Maintenance

When you update this document itself, let one coordinator own the final document edit. The coordinator decides the active scope, the canonical wording, and the final structure that lands in the checked-in guide.

After the change scope is stable, worker passes may inspect independent sections, companion files, or stale references in parallel. Each worker pass should return bounded facts such as outdated wording, missing sync updates, stale paths, or terminology drift.

Collect those worker-pass results before you edit this document. Do not update the guide from half-collected scans.

## Document Relationships

Use this section to understand how this guide differs from the other root `.agent` guides before you choose or change documents. Focus on purpose first, then intent, then the point where each guide becomes the right place to work. Use `Handoffs` for the valid transitions.

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

### CodeStyleRuleDoc

Purpose: Record one reusable code style rule.

Intent: Turn a recurring code pattern into a beginner-friendly rule with a bad example, a corrected example, and plain-language reasoning.

Use it when: The real question is how code should usually be written or reviewed across the repository.

## Handoffs

This document owns a question only while it is the place where the next missing decision, evidence, or instructions must be added. Use this section to decide whether this document still owns the next unresolved question, which listed destination owns it if not, and what evidence makes the handoff safe now.

### Incoming Handoffs

- From code review or direct repository changes in the working tree: Use this guide when the local code example is already known, and the main unresolved question is how to turn a repeated pattern into reusable code-writing guidance for future work.
- From `InvestigationLog`: Use this guide when the failure cause is already diagnosed, and the main unresolved question is how to capture the durable lesson as reusable code-writing guidance instead of a one-off fix.
- From `ExecPlan`: Use this guide when the implementation approach is already known, and the main unresolved question is how to capture a repeated code pattern that should guide future code reviews and changes.
- From `RefactorPlan`: Use this guide when the behaviour boundary and structural improvement are already clear, and the main unresolved question is how to capture the repeated anti-pattern as reusable code-writing guidance.
- From `ArchitectureReview` or `ADR`: Use this guide when the system-level concern or recorded decision is already explicit, and the main unresolved question is how to translate it into day-to-day code-writing guidance.

### Outgoing Handoffs

- To `.agent/styles/AGENTS.md` companion catalog update: Hand off when the rule title and category path are explicit, and the remaining unresolved step is to update the checked-in style catalog in the same change as the rule file.
- To `InvestigationLog`: Hand off when the draft rule shows the remaining unresolved question is what failure or behaviour is actually happening, not how code should usually be written.
- To `ExampleMappingDoc`: Hand off when the draft rule shows the remaining unresolved question is intended behaviour at a user-observable boundary, not reusable code-writing guidance.
- To `BehaviourSpecDoc`: Hand off when the draft rule shows the remaining unresolved question is how accepted behaviour should be specified and proved before implementation.
- To `ExecPlan`: Hand off when the draft rule shows the remaining unresolved question is how to plan behaviour-changing implementation rather than how to state a reusable rule.
- To `RefactorPlan`: Hand off when the draft rule shows the remaining unresolved question is how to plan behaviour-preserving structural change rather than how to state a reusable rule.
- To `ADR`: Hand off when the draft rule depends on first recording one lasting architectural or design decision that the rule alone should not invent.

### Recording the Handoff

The final rule file intentionally does not include a `Next Handoff` section.

While you are drafting the rule, record the next destination alongside the draft in the surrounding working notes, plan, review comment, or message.

Name one destination listed in this section, state what question the rule draft no longer owns, state what question the next destination now owns, and cite the evidence that makes the transition safe.

If the destination is `.agent/styles/AGENTS.md`, treat it as a required companion catalog update in the same change as the rule file.

If no listed handoff applies yet, keep refining the rule and state what is still missing before work can move.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every CodeStyleRuleDoc must be fully self-contained, meaning a complete beginner can read the rule and apply it using only the rule file and the current working tree.
* Every CodeStyleRuleDoc must teach one narrow rule at a time.
* Every CodeStyleRuleDoc must be stored in the canonical location defined in `.agent/OUTPUTS.md` and must keep `.agent/styles/AGENTS.md` in sync.
* Every CodeStyleRuleDoc must use the canonical final structure defined in this guide: `# <Short rule name>`, `**Problem**`, `**Example**`, and `**Refactoring**`, in that order.
* Every CodeStyleRuleDoc must define the forbidden or discouraged code shape in plain language.
* Every CodeStyleRuleDoc must explain the concrete cost of the bad pattern without relying on taste alone.
* Every CodeStyleRuleDoc must include one realistic bad example that clearly demonstrates the problem.
* Every CodeStyleRuleDoc must include one corrected example that keeps the same intended job as the bad example.
* Every CodeStyleRuleDoc must point to the exact code shape that causes the problem and the exact change that resolves it.
* Every CodeStyleRuleDoc must use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.
* Every CodeStyleRuleDoc task must record its concrete task, key files, and required rule or catalog updates in the surrounding active document's `Task and Key Files` section.
* Every CodeStyleRuleDoc must avoid hidden project history, unwritten team memory, or outside references that are required to understand the rule.
* Every CodeStyleRuleDoc must stay short enough that a beginner can read it in one pass.
* Every CodeStyleRuleDoc must use optional extra notes only when they prevent a likely beginner misunderstanding.
* Every CodeStyleRuleDoc must be readable by a complete beginner.
* Every CodeStyleRuleDoc must be revised when the codebase or the preferred pattern changes.

## Recommended Workflow

1. Start from a bad code example and a corrected version that solves the same problem, and record the concrete task, key files, and required rule or catalog updates in the surrounding active document's `Task and Key Files` section.
2. Use `.agent/styles/AGENTS.md` to choose the correct category before you draft the rule.
3. Once the rule scope and category are stable, let one coordinator fan out bounded worker passes to inspect repeated examples, nearby rules, review comments, or related code. Keep each worker pass narrow enough that it can return one concrete example or one supporting pattern without changing the rule question.
4. Collect those worker pass results in the surrounding active document, review comment, or planning document before you draft the final rule text, and keep the document maintenance task visible there.
5. Reduce both code examples to the smallest realistic pair that still shows the same problem and the same correction.
6. Name the rule from the bad pattern, not from a vague benefit.
7. Write `**Problem**` so the reader knows what the bad shape is and why it causes trouble.
8. Write `**Example**` with the bad code and one plain-language paragraph that points at the exact offending shape.
9. Write `**Refactoring**` with the preferred approach, the corrected code, and the exact reason the change helps.
10. Validate the rule, then update `.agent/styles/AGENTS.md` in the same change if the catalog changed.

## Communication Rules

Do not write style rules in silence. Record the concrete pattern, the concrete cost, the example pair, the chosen category, and any important uncertainty as you work.

Let one coordinator own the decision about rule scope, rule name, and final wording. The coordinator decides when the category is stable enough to fan out worker passes and when the gathered examples are ready to collect into one final rule.

Use worker passes only for bounded catalog work. A worker pass is one narrow check such as reviewing one example pair, inspecting one nearby rule, or scanning one code area for repeated evidence. Do not let separate worker passes invent different rule scopes.

Collect worker results before you finalize the rule text or update `.agent/styles/AGENTS.md`. Do not update the catalog from half-collected examples.

Before you settle on the rule, check whether the proposed problem is really one pattern or several. If it is several, narrow the scope or split the work into multiple rules instead of forcing unrelated advice into one file.

Even when the problem appears obvious, look for support in the project before you turn it into a rule. Support means existing style rules, nearby code, naming patterns, documentation, refactor catalog entries, tests, or repeated review feedback that point to the same code-writing expectation.

If you cannot find support and you are not intentionally creating a new rule, pause. Record that the current interpretation is unsupported. Then gather more repository evidence or ask for clarification instead of turning a guess into policy.

Keep your communication concrete. State what code shape is discouraged, what code shape is preferred, what makes the old shape harder to read or maintain, and what a reviewer should be able to point to in the example.

## Document-Specific Guidance

### Formatting

Each document written from this guide must be one single fenced code block labeled `md` when it is embedded inside another document or message. When writing the document to a Markdown file where the entire file is only that rule, omit the outer triple backticks. Do not nest additional triple-backtick fences inside the rule. When you need to show code inside an embedded rule, present it as an indented block inside the single `md` fence.

Write in plain prose. Prefer short paragraphs. Use lists only when they make the rule easier to scan. The final rule artifact is intentionally small and should not inherit planning sections such as `Status`, `Progress`, `Current State Snapshot`, or `Next Handoff`.

### Category Placement and Catalog Maintenance

Use `.agent/styles/AGENTS.md` before you draft the rule. Choose the narrowest existing category that matches the area of code the rule applies to.

If no existing category fits, create the new category and describe it in `.agent/styles/AGENTS.md` in the same change as the rule file.

When you add, move, rename, or remove a rule file, update `.agent/styles/AGENTS.md` immediately. Do not leave the style catalog behind the working tree, and keep that catalog update listed in the surrounding active document.

### Canonical Final Rule Shape

New and revised rules must use this exact section order in the final rule file:

- `# <Short rule name>`
- `**Problem**`
- `**Example**`
- `**Refactoring**`

Use `**Refactoring**` as the final section label even when the corrected version is a small rewrite rather than a formal refactor. This keeps the rule catalog consistent.

Existing rule files in `.agent/styles/` currently use mixed heading levels and mixed final-section names such as `Solution`. Do not rewrite the existing catalog just to normalize old formatting. Apply the canonical shape to new or newly revised rules going forward.

### Turning a Bad and Corrected Code Pair into a Rule

This guide must be enough for a beginner who starts with only two things: code that has a problem and a corrected version that fixes it.

Start by reducing both versions to the smallest realistic example that still shows the same problem and the same correction. Remove unrelated details, but keep enough real code that the rule is obvious.

Name the rule from the bad pattern, not from a vague benefit. Prefer names such as `Complex Else Clauses in With` or `Alternative Return Types` over names such as `Readable Error Handling` or `Better APIs`.

Write the `**Problem**` section first. Name the bad pattern. Describe what it looks like in code. Then explain what becomes harder, less clear, less safe, or less consistent because of that pattern.

Write the `**Example**` section next. Show the bad example. Then write one short paragraph that points to the exact part of the code that makes the example wrong. Do not assume the reader will infer the problem on their own.

Write the `**Refactoring**` section last. State the better approach in one clear sentence. Show the corrected code. Then explain why the new shape solves the specific problem you already named.

Keep the intended job of the example the same across both versions. The corrected example should not quietly change requirements, input shapes, side effects, or return shapes unless the rule itself is about changing that contract.

### Writing the Sections Well

Use the `**Problem**` section to explain the rule in human terms. Avoid taste language such as "ugly", "cleaner", or "better" unless you immediately explain the specific benefit.

Use the `**Example**` section to show the anti-pattern clearly. The example should be small enough to read in one pass, but strong enough that the problem is undeniable once explained.

Use the `**Refactoring**` section to answer the exact problem from the earlier sections. The corrected example should make the improvement visible in the code itself, not only in the surrounding explanation.

When a brief extra note prevents a likely beginner mistake, include it after the corrected example. If you need several side cases or long exceptions, the rule is probably too broad and should be narrowed.

## Validation and Acceptance

A CodeStyleRuleDoc is complete only when a complete beginner can read the rule, compare the bad and corrected examples, and reach the same interpretation without extra explanation.

Use this checklist to verify the rule before you stop:

- [ ] The title names one concrete bad pattern or one concrete code-writing rule.
- [ ] The `**Problem**` section states what the bad shape is and why it causes a concrete problem.
- [ ] The `**Example**` section shows the bad code and points to the exact offending shape.
- [ ] The `**Refactoring**` section keeps the same intended job while showing the corrected shape.
- [ ] The explanation uses plain language and relies on `.agent/DEFINITIONS.md` for shared terminology.
- [ ] The reader does not need hidden project history or outside explanation to understand the rule.
- [ ] The rule lives in the canonical CodeStyleRuleDoc location defined in `.agent/OUTPUTS.md`.
- [ ] `.agent/styles/AGENTS.md` has been updated if the catalog changed.

The rule fails validation if the reader can only give a vague summary, cannot point to the exact offending code, or cannot explain what change makes the corrected example right.

A good acceptance check is to ask whether a beginner could take a different bad and corrected code pair from the same repository area and write a rule in the same shape without inventing a new format.

## Skeleton of a Good CodeStyleRuleDoc

Use this skeleton when you create a new CodeStyleRuleDoc. Keep it complete enough that a complete beginner can understand the rule from the document alone.

    # <Short rule name>

    ## Definitions

    Use `.agent/DEFINITIONS.md` as the source of truth for shared definitions. If a reusable term is missing, add it there instead of defining it locally.

    **Problem**

    This rule discourages <plain description of the bad pattern>.

    It is a problem because <plain explanation of what becomes harder, less clear, less safe, or less consistent>.

    **Example**

        <bad example code>

    In the code above, <plain explanation of the exact code shape that creates the problem>.

    **Refactoring**

    Instead of <bad approach>, prefer <preferred approach>.

        <corrected example code>

    This version is easier to <plain benefit> because <plain reason>.
