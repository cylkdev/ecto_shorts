# TDD + BDD Workflow

This document describes the workflow for delivering a feature or system change using a combination of Test-Driven Development (TDD) and Behaviour-Driven Development (BDD). Treat the reader as a complete beginner to this repository. They have only the current working tree and this document. There is no memory of prior work and no external context. A novice following this document will take any request, clarify what it means, prove the behaviour with a test before writing any code, and grow the implementation in small, safe steps.

## Why TDD + BDD

Test-driven development is not primarily about writing tests. It is an approach to problem-solving that begins with the end in mind. Writing a test first describes the desired behaviour before the implementation is decided. That single discipline change has three effects that compound over time.

First, it catches misunderstandings early. A test written before any code exists is pure data: inputs go in, expected outputs come out, and there are no implementation details to confuse the reader. If the test does not match what the user actually wanted, the misunderstanding surfaces immediately, before hours are invested building the wrong thing.

Second, it guides design. A test that is hard to write is a signal that the code is too complex or too coupled. Listening to that signal and adjusting leads to simpler designs naturally.

Third, it makes refactoring safe. A test that fails when behaviour changes means code can be restructured with confidence. If the tests pass after a change, the observable behaviour is preserved.

Outside-in BDD adds one more idea: start from what the user can observe (the boundary) and work inward. The workflow starts with a boundary test that describes the feature from the outside. When that test fails because some inner piece of logic is missing, step inward and write a focused test for that piece. Once the focused test passes, step back out to see if the boundary test has moved forward. This cycle of stepping in and stepping out continues until the boundary test passes, at which point the feature is done.

The combination of TDD and outside-in BDD provides a clear definition of "done" (the boundary test passes), a safe way to build incrementally (red-green-refactor at every level), and a living record of what the system does (the tests themselves).

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

1. Drive work from observable behaviour at a boundary. Grow the implementation in small proven slices. Keep a written record of what the system is supposed to do and how you proved it.

2. Do not start implementing if the expected behaviour is not fully defined. If multiple reasonable interpretations exist, use an ExampleMapDoc (as described in `.agent/EXAMPLE_MAP_PLANS.md`) to write concrete examples and rules and reach agreement fast. Behaviour is ready to implement when different readers would write the same boundary test for it.

3. Never write code without a failing test that demands it. If there is no failing test asking for a particular line of code, that line should not exist yet.

## Glossary

Every term used in this document is defined here. If you encounter a term elsewhere in the document that is not in this glossary, it is being used in its ordinary English sense.

`Boundary` means any place where behaviour can be observed from outside the implementation. Examples include a public function in a library module, an HTTP endpoint, a CLI command's output, a message handler's return value, a job's side effects, or a file written to disk. The boundary is the contract between your code and its callers.

`Boundary test` means an automated test that exercises the system through the boundary. It calls the public API, sends the HTTP request, or invokes the CLI command. It knows nothing about the internals. It checks only what went in and what came out.

`Focused test` means a narrower, unit-level test used after a boundary test reveals a missing behaviour that is easier to drive at a smaller seam. A focused test targets one module or one function rather than the whole feature.

`Seam` means the point in the code where it becomes easier to continue working with focused tests instead of only boundary tests. In a web application, the seam is often the boundary between the web layer (controllers, views, templates) and the business logic layer (contexts, domain modules). In a library, the seam might be the boundary between a public API module and an internal query builder.

`Outside-in testing` means starting from the outermost boundary (what the user observes) and working inward toward the implementation details. Write a boundary test first, then step in to write focused tests only when the boundary test failure points to missing business logic.

`Feature test` means the outermost boundary test for a feature. It describes the complete user-observable outcome. When this test passes, the feature is done.

`Red / Green / Refactor` is the three-step cycle at the heart of TDD. Red means a failing test that expresses one missing fact about the system. Green means the smallest code change has been written to make that test pass. Refactor means restructuring the code to improve its design while keeping the test green. If the test turns red during refactoring, behaviour has changed and the last change must be undone.

`Behaviour specification` means a written description of expected behaviour at a boundary. It states observable outcomes clearly using scenarios, acceptance tests, or plain-language descriptions with concrete inputs and outputs.

`Example document` means a short document of rules and concrete examples used to remove ambiguity before implementing. In this repository, it is produced by following the ExampleMapPlan process described in `.agent/EXAMPLE_MAP_PLANS.md`.

`Concentric circles` is a mental model for outside-in TDD. The outer circle is the boundary test. The inner circle is the focused test. Work starts in the outer circle, steps into the inner circle when needed, and steps back out when the inner work is done. The boundary test is always the final judge of whether the feature works.

## The Outside-In Model (Concentric Circles)

This section describes the core workflow. Every feature follows this pattern.

```
  THE TWO CIRCLES
  ═══════════════

  Think of your work as two circles, one inside the other.

  ┌─────────────────────────────────────────────────────────┐
  │                                                         │
  │  OUTER CIRCLE — the boundary test                       │
  │                                                         │
  │  This is a test for your public function.               │
  │  It calls the function, passes in data,                 │
  │  and checks what comes back.                            │
  │                                                         │
  │  You fix simple errors here:                            │
  │    - module does not exist yet                          │
  │    - function is not defined yet                        │
  │                                                         │
  │      ┌─────────────────────────────────────────┐        │
  │      │                                         │        │
  │      │  INNER CIRCLE — focused tests           │        │
  │      │                                         │        │
  │      │  When the function exists but returns   │        │
  │      │  the wrong answer, you step in here     │        │
  │      │  and write a small test for the         │        │
  │      │  specific logic that is missing         │        │
  │      │  (e.g. a query, a calculation).         │        │
  │      │                                         │        │
  │      └─────────────────────────────────────────┘        │
  │                                                         │
  └─────────────────────────────────────────────────────────┘

  You always start in the outer circle.
  You only step into the inner circle when needed.
  You always step back out to check the outer circle again.
```

Think of two concentric circles. The outer circle represents the boundary test. The inner circle represents focused tests for business logic.

Work begins in the outer circle with a boundary test that describes the feature from the user's point of view. This test will fail because the feature does not exist yet. That failure is the starting point.

Follow the errors. Each error message indicates what the system is missing. Fix errors one at a time, making the smallest change possible at each step. When the boundary test failure shifts from compile errors (`UndefinedFunctionError`, module not available) to assertion errors (the function returns the wrong value), step into the inner circle.

Inside the inner circle, write a focused test that fails in the same way the boundary test failed. This focused test is smaller and faster to run. Use the red-green-refactor cycle to make it pass: write the smallest implementation to get green, then refactor while keeping it green.

Once the focused test passes, step back out to the outer circle and run the boundary test again. One of two things will happen. Either the boundary test now fails with a new error, in which case progress has been made and the process repeats for the next error. Or the boundary test passes, in which case the feature is done at the code level.

After the boundary test passes, refactor the entire feature. This is the refactor step of the outer circle. Restructure code, improve naming, extract helpers, remove duplication, and clean up generated code that is not needed. The boundary test must stay green throughout. If it turns red, behaviour has changed and the last change must be undone.

When the refactor is complete, run the full test suite to confirm nothing else is broken. If everything passes, the feature is done.

This is the rhythm: outer test fails, follow errors, step in when business logic is reached, red-green-refactor inside, step back out, repeat until the outer test passes, refactor the whole feature, run the full suite.

```
  HOW A SINGLE BOUNDARY TEST GETS TO GREEN
  ═════════════════════════════════════════

  You start with one boundary test (the public API test).
  Follow the steps top to bottom. Arrows show where you loop back.


  1. Run the boundary test
     │
     ▼
  2. It fails. Read the error message. What kind of error is it?
     │
     ├─── A) Module or function does not exist yet?
     │       Examples you will see in the terminal:
     │         ** (UndefinedFunctionError) function Foo.bar/1 is undefined
     │         ** (UndefinedFunctionError) ... (module Foo is not available)
     │       │
     │       ▼
     │    A1. Create the missing module or define the missing function.
     │        Do not write any logic yet — just enough to stop this error.
     │       │
     │       ▼
     │    A2. Go back to step 1. ─────────────────────────────────┐
     │                                                            │
     ├─── B) The function exists but returns the wrong value?     │
     │       Examples you will see in the terminal:               │
     │         Assertion with == failed                           │
     │         left:  []                                          │
     │         right: [%Product{name: "Laptop", ...}]             │
     │       │                                                    │
     │       ▼                                                    │
     │    ┌──────────────────────────────────────┐                │
     │    │  STEP IN: write a focused test       │                │
     │    │  ════════════════════════════════    │                │
     │    │                                      │                │
     │    │  B1. Write a small test for just     │                │
     │    │      the piece of logic that is      │                │
     │    │      missing (e.g. a query helper).  │                │
     │    │            │                         │                │
     │    │            ▼                         │                │
     │    │  B2. Run the focused test.           │                │
     │    │      It should fail the same way.    │                │
     │    │            │                         │                │
     │    │            ▼                         │                │
     │    │  B3. Write the smallest amount of    │                │
     │    │      code to make it pass.           │                │
     │    │            │                         │                │
     │    │            ▼                         │                │
     │    │  B4. Clean up the code while the     │                │
     │    │      focused test still passes.      │                │
     │    │                                      │                │
     │    └──────────────────────────────────────┘                │
     │       │                                                    │
     │       ▼                                                    │
     │    STEP OUT: go back to step 1. ───────────────────────────┘
     │
     └─── C) The boundary test passes!
             │
             ▼
          3. Clean up the whole feature while the test stays green.
             │
             ▼
          4. Run `mix test` to make sure nothing else broke.
             │
             ▼
           DONE
```


## The Three Loops

```
  HOW THE THREE LOOPS FIT TOGETHER
  ═════════════════════════════════

  There are three loops, each one inside the next.
  You spend most of your time in the smallest loop.


  LOOP 1 — THE WHOLE FEATURE  (runs once)
  ────────────────────────────────────────
  A request comes in. You break it into small slices.
  Each slice is one testable outcome.
  You work through the slices one at a time.

    Example: "Add filtering by category" might have two slices:
      Slice 1: filter by one category returns matching products
      Slice 2: filter with no category returns all products

    For each slice, enter Loop 2.
         │
         ▼
  LOOP 2 — ONE SLICE  (runs once per slice)
  ──────────────────────────────────────────
  Write one boundary test for this slice.
  Run it. It will fail. Now enter Loop 3 to make it pass.
  When it passes, clean up the code, then run `mix test`.

    For each error, enter Loop 3.
         │
         ▼
  LOOP 3 — ONE ERROR AT A TIME  (runs many times per slice)
  ──────────────────────────────────────────────────────────
  This is where you spend most of your time.

    1. Read the error from `mix test`
    2. Make the smallest change to fix that one error
    3. Run the test again
         │
         ├── Still failing with a new error? Go back to step 1.
         │
         └── Test passes? Step back out to Loop 2.
```

Work is organized into three nested loops. Move between them as needed.

### Loop 1: Task Level (BDD)

The task-level loop goes from a request to a proven result. It is the outermost loop and runs once per feature or change.

When to enter: a request arrives to implement a feature, fix a bug, or make a system change.

What to do inside:

1. Read the request. Restate it in your own words as a one-sentence user story in the form "As a [role], I want [capability], so that [benefit]."

2. Identify the boundary. What is the outermost interface through which this behaviour can be observed? For a library, it is the public function. For a web app, it might be an HTTP endpoint or a page a user visits. For a CLI tool, it is the command's output.

3. Check for ambiguity. Could two reasonable people interpret this request differently? If yes, create an ExampleMapDoc (`.agent/EXAMPLE_MAP_PLANS.md`) to write concrete examples and rules until the behaviour is unambiguous. If no, continue.

4. Decide what kind of work this is. Is it a behaviour change (new feature or bug fix), a refactor (change structure without changing behaviour), or research (exploration with no code)? This determines which loop structure to follow. For behaviour changes, continue below. For refactors, see `.agent/REFACTOR_PLANS.md`.

5. Break the feature into small slices. Each slice is one observable outcome at the boundary. Order the slices from simplest to most complex. Each slice becomes one pass through the milestone-level loop.

6. Execute each slice through the milestone-level loop (see below).

7. After all slices are done, run the full test suite. If anything is broken, fix it.

When to exit: all slices pass, the full test suite passes, and the feature is verified at the boundary.

### Loop 2: Milestone Level (BDD)

The milestone-level loop delivers one small, provable slice of behaviour. It runs once per slice.

When to enter: one proof target has been identified from the task-level loop. A proof target is one scenario, one edge case, one contract detail, or one preserved behaviour during a refactor.

What to do inside:

1. Write one boundary test that expresses the proof target. The test must be pure data: set up the inputs, call the boundary, and assert on the outputs. No implementation details. No mocks unless absolutely necessary. Show the test to the user and confirm it matches their intent before proceeding.

2. Run the test. Confirm it fails. Read the error message carefully. The error should indicate that the feature does not exist yet (for example, "module not found" or "function undefined"), not that the test itself is broken.

3. Follow the errors through the test-level loop (see below) until the boundary test passes.

4. Refactor the whole slice while keeping the boundary test green.

5. Run the full test suite to check for regressions.

When to exit: the boundary test passes, the refactor is complete, and no other tests are broken.

### Loop 3: Test Level (TDD)

The test-level loop is the micro red-green-refactor cycle. It runs many times per milestone.

When to enter: a failing test (either a boundary test or a focused test) needs to pass.

What to do inside:

1. Read the error message. It indicates exactly what is missing.

2. If the error is a compile error (module does not exist, function is undefined, missing dependency), make the smallest change to fix that specific error. Do not jump ahead. Create the module with no functions. Define the function with no body. Add the dependency.

3. Run the test again. A new, different error should appear. If the same error appears, the change did not address it.

4. If the error is now about business logic (the function returned the wrong value, the data is missing, the assertion failed), and the current position is in the outer circle (boundary test), this is the point to step into the inner circle. Write a focused test for the specific piece of business logic that is failing. The focused test should fail with the same error as the boundary test.

5. Make the smallest code change to pass the focused test. "Smallest" means the least amount of code that makes the test green. It is acceptable to hard-code a return value, return an empty list, or stub a function if that is truly the smallest change. The next test will force replacement of the stub with real logic.

6. Run the focused test. If it passes, refactor while keeping it green. Then step back out to the boundary test.

7. If the focused test does not pass, repeat from step 1 within the inner circle.

When to exit: the test that triggered entry is now green and has been refactored.

```
  THE RED-GREEN-REFACTOR CYCLE
  ════════════════════════════

  This is the heartbeat of TDD. Every small change follows
  these three steps, in order, every time.


  RED — the test fails
  │
  │  You just wrote a test, or the last change revealed a new error.
  │  The test output is red. That is expected.
  │
  ▼
  GREEN — make it pass with the smallest change
  │
  │  Write the least amount of code that makes the test pass.
  │  It is fine to hard-code a value or return an empty list
  │  if that is truly the smallest change. The next test will
  │  force you to replace it with real logic.
  │
  ▼
  REFACTOR — clean up while the test stays green
  │
  │  Rename variables, extract helpers, remove duplication.
  │  Run the test after every change. If it turns red,
  │  undo the last change — you accidentally changed behaviour.
  │
  ▼
  Done. Pick up the next failing test and repeat.
```


## Step-by-Step Recipe

This section restates the three loops above as a single numbered procedure. Follow it mechanically.

1. Read the request. Restate it as a one-sentence user story.

2. Identify the boundary (public API, HTTP endpoint, CLI output, etc.).

3. Check: is the expected behaviour unambiguous? If no, create an ExampleMapDoc (`.agent/EXAMPLE_MAP_PLANS.md`). If yes, continue.

4. Write one boundary test. It must be pure data in, data out. No implementation details. Show it to the user and confirm it matches their intent.

5. Run the test. Confirm it fails for the right reason (the feature does not exist, not a test setup error).

6. Read the error message. Make the smallest change to fix that one error. Run the test again.

7. Repeat step 6 until the error shifts from compile errors (`UndefinedFunctionError`, module not available) to assertion errors (wrong return value, missing data).

8. When the error shifts to business logic, step in: write a focused test that fails the same way. Get it green with the smallest change. Refactor while green. Step back out to the boundary test.

9. Repeat steps 6 through 8 until the boundary test passes.

10. Refactor the whole feature while keeping the boundary test green.

11. Run the full test suite. If anything broke, fix it.

12. Move to the next slice of behaviour. Repeat from step 1.

```
  RECIPE SUMMARY
  ══════════════

  This is the same 12-step recipe above, shown as a picture
  so you can see where you loop back.


  SETUP (do this once per slice)
  ──────────────────────────────
  1. Restate the request in your own words
  2. Identify the public function you are testing
  3. Is the expected behaviour clear?
     │
     ├── No  → write an ExampleMapDoc first
     │
     └── Yes → continue
  4. Write one boundary test
  5. Run `mix test` — confirm it fails
     │
     ▼
  FIX ERRORS (repeat until the test passes)
  ──────────────────────────────────────────
  6. Read the error message from `mix test`
  7. What kind of error?
     │
     ├── Module or function missing?
     │     (UndefinedFunctionError)
     │     → Create the module or define the function
     │     → Run the test again
     │     → Go back to step 6
     │
     └── Wrong return value?
           (Assertion with == failed)
           → Write a focused test for the missing logic
  8. Make the focused test pass (red-green-refactor)
  9. Run the boundary test again
     │
     ├── Still failing? → Go back to step 6
     │
     └── Passes? → Continue below
         │
         ▼
  WRAP UP
  ───────
  10. Clean up the code while the boundary test stays green
  11. Run `mix test` — make sure nothing else broke
  12. More slices left? → Go back to step 1 for the next slice
```

## Worked Example

This example walks through the full workflow for a fictional library feature. The library provides product catalog functionality, and the request is to add a function that lists products filtered by category.

### The request

"Add a function to list products filtered by category."

### Step 1: Restate as a user story

"As a caller, I want to list products by category, so that I can display only the products relevant to a given section."

### Step 2: Identify the boundary

The boundary is the public function `Catalog.list_products/1`. Callers will pass a map of filter parameters and receive a list of product structs.

### Step 3: Check for ambiguity

The request is clear enough for a single boundary test. No ExampleMapDoc needed.

### Step 4: Write the boundary test

    defmodule CatalogTest do
      use MyApp.DataCase, async: true

      describe "list_products/1" do
        test "returns products matching the given category" do
          electronics = insert(:product, name: "Laptop", category: "electronics")
          _clothing = insert(:product, name: "T-Shirt", category: "clothing")

          result = Catalog.list_products(%{category: "electronics"})

          assert result == [electronics]
        end
      end
    end

The test is pure data. It sets up two products, calls the public API with a filter, and asserts the returned list contains only the matching product. There are no implementation details, no mocks, and no internal module references.

### Step 5: Run the test — confirm it fails

    $ mix test test/my_app/catalog_test.exs

    ** (UndefinedFunctionError) function Catalog.list_products/1 is undefined
       (module Catalog is not available)

The test fails because the module does not exist. This is the right kind of failure.

### Step 6: Follow the errors — fix compile errors

Create the module with no functions:

    defmodule Catalog do
    end

Run the test again:

    ** (UndefinedFunctionError) function Catalog.list_products/1 is undefined or private

The module exists now, but the function does not. Define the function with the simplest possible body:

    defmodule Catalog do
      def list_products(_params) do
        []
      end
    end

Run the test again:

    Assertion with == failed
    left:  []
    right: [%Product{name: "Laptop", category: "electronics", ...}]

The error has changed from a compile error (missing module, undefined function) to an assertion error (wrong return value). This is progress.

### Step 7: Step in — write a focused test

Since this is a library and the boundary is already a single function, the boundary test and the focused test are the same in this case. In a web application, this would be the point to step from a feature test (HTTP request) into a context test (function call). Here, the implementation continues at the same level.

### Step 8: Make the smallest change to pass

    defmodule Catalog do
      import Ecto.Query

      alias MyApp.{Product, Repo}

      def list_products(params) do
        Product
        |> where(^Enum.to_list(params))
        |> Repo.all()
      end
    end

Run the test:

    .

    1 test, 0 failures

The boundary test passes. The feature works for the basic case.

### Step 9: Refactor while green

The implementation is simple enough that there is not much to refactor. The questions to ask: is there generated or unused code? Is the naming clear? Is the function doing too much?

In this case, the function is clean. Move on.

### Step 10: Run the full test suite

    $ mix test

    All tests passed.

No regressions. The first slice is done.

### Next slices

If the feature needed more slices (for example, listing products with no filter returns all products, or handling an invalid category), repeat the entire process from step 1 for each slice. Each slice adds one boundary test, one proven behaviour, and one small increment of code.


## Refactoring Guidelines

Refactoring means changing the internal structure of code without changing its observable behaviour. It is the third step of every red-green-refactor cycle, and it is not optional.

When refactoring, always keep a test green. Which test to keep green depends on the scope of the refactor. When restructuring a single function, keep the focused test for that function green. When restructuring how two modules interact, keep the boundary test green because public function names or signatures may change along the way, requiring focused test updates.

Things to look for during the refactor step:

Naming. Do the module names, function names, and variable names clearly communicate what they do? Rename anything that requires reading the implementation to understand the name.

Duplication. Is there repeated logic that could be extracted into a helper? Extract it, run the tests, and confirm they still pass.

Unused code. Was code generated (for example, with a Phoenix generator) that is not actually used? Remove it. It can always be reintroduced later when a test demands it.

Readability. Can a reader who has never seen this code understand it in one pass? If not, simplify.

Test readability. Can a stakeholder read the test and understand what the feature does without reading the implementation? If not, extract private helper functions in the test module that use domain language instead of technical details. For example, replace `Query.css(".room", text: room.name)` with a private function called `room_name(room)`.

After every individual refactoring change (rename, extraction, deletion), run the test immediately. Do not batch multiple refactoring changes before running the test. Each change must be verified in isolation so that if a test fails, the cause is unambiguous.

## Common Mistakes

These are the pitfalls that beginners and coding agents hit most often. Knowing them in advance prevents wasted effort.

Writing the implementation first and tests after. A test written after the implementation becomes a rubber stamp for whatever the code already does. It does not catch misunderstandings because the test is shaped by the implementation rather than by the user's intent. Always write the test first.

Testing implementation details instead of observable outcomes. A test that checks whether a specific private function was called, or whether data was stored in a specific internal format, will break during refactoring even though the behaviour has not changed. Test what went in and what came out at the boundary. Ignore the internals.

Making the test pass with a large change instead of the smallest change. Writing a lot of code at once removes the ability to trace which line of code satisfies which test. If something breaks, there is no way to know where to look. Make the smallest change, run the test, and repeat.

Skipping the refactor step. It is tempting to move on to the next feature as soon as the test passes. But skipping the refactor step accumulates design debt. The refactor step is where naming improves, helpers are extracted, duplication is removed, and cleanup happens. It is what keeps the codebase healthy over time.

Not stepping in when the boundary test failure points to business logic. Trying to make the boundary test pass by writing all the business logic at once removes the granularity of the inner red-green-refactor cycle. Step in, write a focused test, and build the logic one small piece at a time.

Not stepping back out after the focused test passes. Staying in the inner circle and writing focused tests without checking the boundary test risks building logic that the boundary test does not actually need. Always step back out after each focused test passes to see where the boundary test stands.

Hard-coding values to pass a test and never replacing them. It is fine to hard-code a value as the smallest change to pass a test. But the next test must force replacement of the hard-coded value with real logic. Hard-coding repeatedly without writing the test that would expose the hard-coding is a sign the tests are not sliced finely enough.
