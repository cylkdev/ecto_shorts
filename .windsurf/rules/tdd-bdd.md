---
trigger: always_on
---

## Why use TDD + BDD

Starting with a boundary test made of plain data removes ambiguity. There is nothing to misinterpret - the test is the specification. The outside-in approach (concentric circles) ensures you only build what the boundary test demands, and the red-green-refactor cycle at every level keeps the implementation growing from proven behaviour, not assumptions.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:
* Always use the TDD + BDD Workflow to drive the implementation of a feature or change.
* Before you start any feature or change, state that you are following the TDD + BDD workflow.

## What to do

Before implementing a feature or making a change, take a TDD + BDD approach as described in `.agent/TDD_BDD.md`. Write a failing boundary test first that shows the user-observable outcome in plain data - no implementation details, just inputs and expected outputs. This catches misinterpretations of the user's intent early, before any production code exists.

## TDD + BDD Workflow

1. Clarify the expected behaviour at the boundary. If multiple interpretations exist, use an `ExampleMapDoc` (`.agent/EXAMPLE_MAP_PLANS.md`) to reach agreement before writing code.

2. Write one failing boundary test that expresses the next observable outcome. The test should be pure data in, data out - no implementation details.

3. Show the test to the user and confirm it matches their intent before proceeding.

4. Run the test. Follow the errors one at a time, making the smallest change to fix each error.

5. When the error shifts from infrastructure (missing module, undefined function) to business logic (wrong return value), step into the inner circle: write a focused test that fails the same way, get it green with the smallest change, refactor while green, then step back out to the boundary test.

6. Repeat steps 4–5 until the boundary test passes.

7. Refactor the whole feature while keeping the boundary test green.

8. Run the full test suite. If anything broke, fix it.

9. Repeat from step 2 for the next slice of behaviour.