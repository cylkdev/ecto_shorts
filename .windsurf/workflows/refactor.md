---
description: Run this when your involves refactoring code
auto_execution_mode: 3
---

## What to do

- Use a `RefactorDoc` (as described in `.agent/REFACTORS.md`) for each refactoring task.

- Read the documents in `.agent/refactors` to understand the refactoring techniques available and code smells to look out for.

### 1. Define what must stay the same

Before changing code, write down the behaviour that must not change.

Use one clear, observable boundary. This can be an API response, a function result, a CLI output, a message shape, or another result you can check directly.

Write down how you will verify it. Use an existing test, a new test, or a repeatable manual check.

If the preserved behaviour is unclear, clarify it first before refactoring.

### 2. Define the scope

Choose the exact code you will inspect and possibly change.

Name the file, module, function, or clause. Keep the scope small enough that you can verify changes quickly.

If the file has many problems, do not fix everything at once. Pick one problem first.

### 3. Choose one refactoring technique

Select a refactoring technique and apply it to the code.

Apply one refactoring technique at a time. If more cleanup is needed later, repeat the process.

If no refactoring technique fits, record the problem and stop until the approach is clear.

### 4. Record the refactor work

Update the `RefactorDoc` you used for the task. 

### 5. Make one small change

Make the smallest code change that applies the chosen technique and stays within scope.

Do not mix feature changes into the refactor. If you find a feature change is needed, record it as a separate follow-up task.

### 6. Verify immediately

Run the validation method after the change.

If the preserved behaviour fails, stop, fix the issue, and verify again before making more changes.

If the preserved behaviour passes, update the `RefactorDoc` you used for the task.

### 7. Repeat or finish

If another in-scope refactor problem remains, repeat the process from the beginning for the next problem.

Finish when the in-scope code is easier to change or verify, and the preserved behaviour still holds.