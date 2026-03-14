# Elixir Testing Principles

This document describes how to choose and apply validation methods for Elixir code. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single Elixir Testing Principles document you provide. There is no memory of prior testing guidance and no external context.

Testing is not the same as proving code correct. Treat testing as a first-class method to build confidence that programs behave as intended. Treat a complete validation strategy as broader than testing alone: it also includes social methods such as review and pairing, and, when testing is not sufficient, formal methods.

Write with clarity first, then stop. Include every fact the reader needs to choose the right validation method and to use tests well. Do not add explanation that does not change how the work should be validated.

Treat validation as broader than testing. Use both social methods and automated tests for most day-to-day Elixir changes. Add formal analysis or proof-oriented techniques when the change requires a stronger claim than review and automated tests can support on their own. Do not describe tests as the only validation method.

## How to use testing principles and TESTING_PRINCIPLES.md

When deciding how a change should be validated, how much confidence the current tests actually provide, whether testing alone is sufficient, what claims a passing test suite supports, or whether stronger review or stronger methods are required, follow TESTING_PRINCIPLES.md to the _letter_. If it is not in your context, refresh your memory by reading the entire file. Apply it to the work as an execution standard, not as background reading. Use it to decide what validation steps must happen, what tests should and should not claim, when the current testing approach is insufficient, and when the change should not be treated as sufficiently validated.

Choose the validation strategy before writing or changing tests. State the exact claim the change must support. State whether that claim is limited to behaviour on selected executed cases or whether it requires stronger assurance. Then choose the combination of specification, review, testing, and stronger methods that is sufficient to support that claim.

Use specification, review, and automated tests when the code is deterministic and the claim is limited to observable behaviour on representative executed cases. Do not treat that combination as sufficient when the code is concurrent, timing-sensitive, safety-critical, or otherwise difficult to validate through selected executions alone. In those cases, strengthen the validation method before accepting the change.

Apply these principles during review as direct checks on the validation record. Confirm that the change has a clear specification, meaningful review, and coverage for the important failure modes and claims introduced by the change. If any of those things is missing, treat the validation strategy as incomplete. If the change claims correctness only because tests pass, reject that reasoning. Passing tests support only the cases that were executed.

Re-evaluate the validation strategy whenever the code changes, the required claim changes, or the operating conditions change. Strengthen the validation method when the module becomes more critical, more concurrent, harder to reproduce consistently, or harder to reason about than the existing validation record assumes.

## Use Testing Principles In ExecPlans

A plan does not satisfy testing principles by listing test files alone. The validation section of an `ExecPlan` exists to connect each important claim to evidence and to say what that evidence does not settle. For ordinary behavior changes, that means the reader can see which command exercises which case, what claim the check is meant to support, what evidence should come back from it, and what uncertainty remains afterward. When a claim is carried by review instead of by a test, the plan should say so plainly and explain why review is enough for that part.

For benchmark or compile-time work, the same idea still applies. The reader should be able to see what measurement command will be run, where it will be run, which baseline artifact or output matters, how the comparison will be judged, what threshold or decision rule makes the result acceptable, and which environmental assumptions might affect the reading.

## Requirements

A testing approach is acceptable only when all of the following are true.

- It identifies testing as the validation method being used. It does not describe testing as proof of correctness on all possible inputs.

- It defines one or more sample inputs to execute. It defines the expected result for each sample input. It executes the program on those sample inputs. It compares the actual result of each execution with the expected result for that execution.

- It states its conclusion only in terms of the sample inputs that were executed.

- It does not treat testing alone as sufficient when the program is concurrent and execution is not deterministic.

- It does not choose testing as the only validation method when the required claim is correctness on all possible inputs.

## Operational Validation Workflow

Start validation by stating the observable behavior or claim at the boundary that matters, then name the failure modes or regressions that belong at that same boundary. From there, choose the right form of evidence for each claim, whether that is review, an automated test, a benchmark, or a stronger method. Write boundary-first checks before reaching inward, add narrower checks only when the boundary evidence does not answer the question, and then state plainly what each executed check established and what still remains unproven.

The order matters because it keeps the validation attached to intent rather than to habit. "Run these existing test files" is not enough unless the plan also says what each one is meant to establish.

State test results precisely. A passing ExUnit suite means that the executed cases behaved as expected. It does not establish correctness for inputs, process schedules, mailbox timing, external state, or runtime conditions that were not exercised. Do not describe passing tests as proof of correctness.

Choose the lowest-cost validation method that still covers the change. Start with ordinary automated tests when the logic is deterministic and the observable result can be checked through return values, tagged tuples, changesets, persisted data, or other stable outputs. Add stronger validation when behaviour can vary across runs because it depends on process ordering, message timing, shared state, or other nondeterministic conditions, or when the consequence of failure is too severe to rely on representative tests alone. If the current validation does not let you say exactly which behaviours were exercised and which remain unproven, stop and strengthen the validation.

## Start with confidence, not just test count

Before writing or changing ExUnit tests, list the observable behaviours the change must preserve or establish. List the failures the change could introduce at the same boundary. Express those items in caller-visible terms such as returned values, tagged tuples, raised exceptions, changeset errors, persisted database effects, emitted messages, or other observable results. Write tests only for behaviours and failures on those lists.

Do not count test functions. Count covered claims. A test is justified only if it verifies one distinct observable behaviour, one distinct failure mode, or one distinct regression the change could introduce. If two tests verify the same claim, keep the clearer test and remove or merge the other.

For each test, state the exact claim it proves before keeping it. Write that claim as a specific behaviour or regression, not as a description of implementation steps. If you cannot state the claim precisely, do not keep the test.

Stop adding tests when every listed behaviour, failure mode, and plausible regression introduced by the change is covered either by an ExUnit test or by explicit review. If an item is covered by review instead of a test, name that decision directly rather than leaving the gap implicit.

## Required ExecPlan Validation Matrix

The validation matrix should make it easy to see, for each important rule or claim, the boundary being checked, the proof method being used, the exact command or review step, what that evidence supports, and what it still does not prove. The specific layout may vary, but the reader should not have to guess how a claim connects to evidence or where the remaining uncertainty lives. If an important rule cannot be traced through that chain, the plan is incomplete.

## Use social methods as part of the test strategy

Treat review as part of validation. Use it to check claims that tests do not settle clearly on their own.

Use a code walkthrough when the immediate problem is shared understanding of the intended behaviour or the change approach. Use a code inspection when the change must be reviewed against explicit quality concerns such as correctness, failure behaviour, edge cases, or missing coverage. Use pair programming when the code is still being shaped and design or correctness decisions must be challenged while the code is being written.

During review, require explicit answers to three questions. Is the intended behaviour stated clearly at the boundary being changed. Do the tests verify that stated behaviour. Do any important edge cases or failure modes remain uncovered.

Do not treat the change as sufficiently validated until those questions are answered directly.

## Use formal methods when testing is not enough

Use formal methods when the required claim is stronger than “the program behaved correctly on the cases we tested.”

Choose formal methods when the work requires correctness on all possible inputs rather than evidence from selected executions. Choose them when the program is concurrent or otherwise non-deterministic and execution order can change the result. Choose them when the component is short enough to reason about directly and the user has identified it as critical enough to require stronger assurance than testing provides.

Do not choose formal methods by default. They cost more than ordinary testing and require deeper reasoning about both the code and its specification.

If the work does not require correctness on all possible inputs, does not depend on non-deterministic execution, and has not been identified by the user as critical enough to require stronger assurance, do not choose formal methods. Use testing when evidence from selected inputs is sufficient for the claim being made.

If the work does require correctness on all possible inputs, or depends on non-deterministic execution, or has been identified by the user as critical enough to require stronger assurance, and formal methods will still not be used, record that as an explicit user decision. Do not make that decision on your own.

## Understand what tests can and cannot prove

Treat each test as a check of one executed case. A test runs code on selected inputs and compares the actual behaviour with the expected behaviour for that case only.

Do not claim more than the executed cases establish. Passing tests show that the program behaved as expected for the inputs, environments, and schedules that were actually exercised. They do not establish correctness for inputs, environments, or schedules that were not exercised.

State the result of a green test run precisely. Say which behaviours were checked and which uncertainty remains. Name the remaining uncertainty when it affects the decision to accept the change.

If the remaining uncertainty is larger than the claim you need to make, strengthen the validation approach instead of overclaiming what the tests proved.

Use testing by default when the claim being made is about selected executed cases and that level of evidence is sufficient. Testing remains a practical default because it is much cheaper and easier than formal proof while still catching many real defects quickly.

## Write tests around observable behaviour

Test what a caller or operator can observe. Check return values, failures, side effects, database writes, messages, files, ordering rules, and other boundary-visible behaviour.

Do not anchor tests to private helpers, temporary data shapes, or line-by-line implementation steps unless there is no better seam and the reason is explicit.

Prefer boundary tests first. When a boundary test exposes a missing piece of business logic, step inward with a focused test for that logic, then step back out to the boundary again. Use `.agent/workflows/TDD_BDD.md` when you need the detailed outside-in workflow.

## Match the validation method to the risk

Use ordinary unit tests and boundary tests when the code is deterministic and the expected claim is limited to behaviour that can be checked through representative executions.

Strengthen the validation approach when failure would be expensive or dangerous, when concurrency or timing allows many possible execution orders, when external systems make behaviour harder to reproduce consistently, when the number of meaningful states is too large for a few example-based tests to cover credibly, or when the code must preserve an invariant in every case rather than only in typical cases.

When any of those facts is true, do not keep the same lightweight strategy by default. Add stronger review, broader examples, stronger invariants, or formal methods until the validation method matches the claim that must be supported.

## Use tests as a living explanation

Write tests so future readers can tell what behaviour matters. Use tests to show the important success cases, the important failure cases, and the assumptions the code is required to preserve.

Name each test so the intended behaviour is clear from the test name. Keep each test focused on one claim when that is possible. Read tests during review as explanations of behaviour, not only as executable checks.

If a reader cannot determine the intended behaviour by reading the specification and the tests together, the validation work is incomplete.

## Separate evidence from certainty

Describe test results as evidence, not as proof, unless the process used actually establishes proof.

Say that the tests show or cover a behaviour when that is the actual claim. Do not say that the tests prove correctness unless the work includes a stronger method that justifies that word.

Record residual risk when it affects the decision to accept the change. Name untested concurrency schedules, production-only integrations, load-related behaviour, or invariants that have not been proven when those gaps remain.

Keep the distinction between evidence and certainty explicit. Do not let the validation record imply a stronger conclusion than the method supports.

## Incomplete Validation Sections In Plans

Validation sections in plans become weak when they list files without named claims, speak about proof without naming the executed cases, use benchmarks without a decision rule, or rely on words such as "faster", "safer", or "better covered" without stating the evidence behind them. They are also weak when residual risk is left implicit or when existing coverage is treated as sufficient without saying which behavior it actually covers. A good validation section lets the reader see, at a glance, what has been checked and what still remains uncertain.

## Good Example of Applying Testing Principles

Use `MyApp.PageWindow` as a validation example.

Start by stating the exact observable behaviour at the public boundary. `new/1` must return `{:ok, window}` when `:page` and `:page_size` are integers greater than or equal to 1. `offset/1` must return the correct zero-based offset for a validated window. `new/1` must return `{:error, :invalid_page}` when `:page` is invalid. `new/1` must return `{:error, :invalid_page_size}` when `:page_size` is invalid.

Review that behaviour statement before relying on the implementation. Require the reviewer to confirm that the success case is part of the public contract, that each invalid-input case is part of the public contract, and that each stated claim has a corresponding test.

Then write ExUnit tests for those claims:

    defmodule MyApp.PageWindowTest do
      use ExUnit.Case, async: true

      test "builds a page window from valid input" do
        assert {:ok, window} = MyApp.PageWindow.new(page: 2, page_size: 25)
        assert MyApp.PageWindow.offset(window) == 25
      end

      test "returns an error when page is less than one" do
        assert MyApp.PageWindow.new(page: 0, page_size: 25) ==
                {:error, :invalid_page}
      end

      test "returns an error when page size is less than one" do
        assert MyApp.PageWindow.new(page: 1, page_size: 0) ==
                {:error, :invalid_page_size}
      end
    end

Run the tests and state the result precisely. These tests establish that the module behaved as expected for the executed valid case and the executed invalid cases. They do not establish correctness for inputs that were not executed, and they do not prove correctness for all future changes.

Use this validation approach only when the claim is limited to deterministic input and output behaviour at a small public boundary. Do not use only this validation approach when the code is concurrent, safety-critical, or requires a stronger claim than behaviour on selected executed cases. In those situations, add stronger review or stronger validation before making the stronger claim.
