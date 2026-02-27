---
auto_execution_mode: 3
description: Run when you want to do a full pass of the codebase to check that documentation exists for all public api functions and modules, or you want to review/update documentation, or you are reviewing existing code, or if you are reviewing a behavioural change.
---

## When to use

Run when you want to do a full pass of the codebase to check that documentation exists for all public api functions and modules, or you want to review/update documentation, or you are reviewing existing code, or if you are reviewing a behavioural change.

## Requirements

- Read `.agent/DOCS.md` and follow it to the _letter_ when writing documentation.

- Execute tasks in a single uninterrupted run. Do not pause for approval. Do not create partial diffs. Apply all required edits across all files. Present a single final change set when the task is fully complete.

## What to do

Let's go step by step.

1. Systematically review the public api in the codebase to understand how it was implemented, what it does, how it should be used, what was the intended purpose, and any gotchas, important assumptions or limitations.

2. Read any existing documentation or comments that provide additional context to the function's purpose and usage. If the information is useful and up-to-date, keep it and revise it during the later steps if needed.

3. Do a full pass of the codebase to check that documentation exists for all public api functions and modules. If documentation is missing, add it. If documentation already exists then read it and revise it so that it follows the guidelines in `.agent/DOCS.md` to the _letter_.

4. Plan the changes you intend to make to the documentation to make it better. After writing your plan re-read the documentation you plan to write from the perspective of a human novice with no context and if does not makes complete sense without any additional context, revise it.