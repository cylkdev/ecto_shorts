---
trigger: always_on
---

- Do not repeat instructions or guidlines you are given. Apply them silently.

## Writing Guidelines

- Write in plain language and simple words.

- Explain steps fully. Do not assume the reader can fill in the blanks.

## Operating Guidelines

- Avoid scope creep.

- Do not act on an assumption. If the reason for your decision cannot be proven, stop and ask for clarification.

- When you make a change that results in an unexpected error, do not try to fix it. Ask for clarification on how to proceed.

- Before proposing a solution, review the codebase and all relevant context so your recommendation is grounded in evidence. For example, read the dependency’s documentation before suggesting a fix that depends on it.

- Use examples generously when providing an explanation. Your explanations should be easy for a complete beginner to understand at a glance.

- When describing the changes you intend to make to code you must copy snippets and explain the current state then explain from beginning to end with examples of the exact changes you intend to make. Include enough code to make all the boundaries and layers clear between every module and function.

- When explaining a problem start with a clear problem statement. If you cannot write a clear problem statement ask for clarifcation.

- When describing changes you've made show the before and after states.

- Keep your explanations clear, focused and concise.

## Coding Guidelines

- Before implementing a feature of making changes to code you must meet the following criteria:

- You can prove what code style or pattern to follow.
- You can clearly state exactly what is proven to be in scope and out of scope for the task.
- You can clearly state each relevant layer and boundary in the code, and how they interact with one another.

If you cannot meet these criteria, stop and ask for clarification.

- Before writing a function you must have a code style or pattern to follow. Prioritize searching the codebase and following existing coding styles. If you cannot find an existing code style for your task or there are conflicting styles, stop and ask for the user clarification.

- Look for existing functions that can be used to complete the task. Re-use existing functions unless there is a proven need not to.

- Make changes in small batches at a time. Stop and wait for feedback after making a batch of changes.