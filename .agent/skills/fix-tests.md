# Fix Tests

## What to do

Start by treating a failing test as a signal, not as the truth. Your goal is to prove what's wrong before you change anything.

1. Reproduce the failure exactly.

Run only the failing test first. Use the same command every time so you can compare results. Do not change code until you can reproduce the failure on demand.

2. Read the failure like a contract dispute.

Identify three things, and write them down in one sentence each:

What the test expected.

What actually happened.

Where the mismatch shows up (error message, assertion diff, stacktrace line).

If you can't state those three clearly, you are not ready to fix anything.

3. Find the user-facing boundary that the test represents.

Ask: "What is the test trying to protect?"
Usually this is a public function, an HTTP endpoint, a CLI command, or a business rule. Name that boundary explicitly.

4. Decide what is allowed to change: the code, the test, or the spec.
Pick one of these and commit to it before editing:

Code is wrong, test is right. Fix code to match the intended behavior.

Test is wrong, code is right. Fix test to match the intended behavior.

Both are unclear. Stop and define behavior with examples before changing either.

If you don't make this decision, you will flail and "patch" randomly.

5. Confirm intent with evidence.

Before editing, gather at least one piece of evidence that supports your decision:

A doc comment, README, ADR, or ticket description.

A similar test that already passes.

A naming pattern or existing module behavior.

Real usage (a caller, endpoint, or integration code path).

No evidence means you're guessing. If you're guessing, pause and ask clarifying questions or define examples.

6. Make the smallest possible change that proves the fix.
Change one thing. Run the single failing test again.
If it still fails, revert that change or adjust it—do not pile on more changes.

7. Expand the test run gradually.
Once the single test passes:

Run the whole test file.

Then run the whole test suite.

This catches "fixes" that break other contracts.

8. Add a guard against regressions when the failure revealed a missing case.

If the bug was "real," add a test that would have caught it earlier.
If the test was wrong, add a second assertion or a clearer test name so the intent is obvious next time.

9. When you change a test, prove it is still meaningful.
A changed test must still fail when the bug is reintroduced.
If a test would pass no matter what, it's not a test—it's noise.

10. Stop after three attempts.

If you've "fixed" the same failing test three times and it keeps coming back, assume misalignment:

- you misunderstood the intent,
- the spec is unclear,
- or the test is asserting the wrong contract.

At that point, stop editing and write concrete examples (inputs -> outputs) of what should happen, then resolve the behavior specification and type specs first.