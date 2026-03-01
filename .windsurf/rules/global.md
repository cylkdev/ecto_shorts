---
trigger: always_on
---

## Operating Guidelines

- Before you start a task, say the task you are going to start.

- When you finish a task, say the task you finished.

- Before you make a final decision on a proposed solution, read the codebase and look for an existing pattern that supports it. Treat that pattern as evidence that the approach fits this project.

If you cannot find a supporting pattern, assume the solution might not be the best fit. In that case, explore and compare alternative solutions before you decide.

- If you have been asked to do the same task more than three times, treat that as evidence of a misunderstanding. It usually means you are interpreting the user’s intent incorrectly, or you are relying on an assumption that is wrong.

One common cause is tests that are outdated or incorrect. Do not assume the test is always correct. If you keep changing code to satisfy a wrong test, you will keep reinforcing the wrong assumption and you may not be able to finish the task.

- Do not work in silence. Update any documents you are using as you make progress or decisions.

- When you receive a user message, before you evaluate it, repeat your interpretation of the message to the user and ask them to confirm if that is what
they meant. If the user agrees your interpretation of the message is correct then proceed with the task. If the user does not agree, ask for clarification and repeat the process.

## Writing Conventions

- Write code so a beginner can understand what it does by quickly scanning it.

- Use descriptive names for variables and functions, and choose the simplest approach that solves the problem.

- Add blank lines to separate steps, and add short comments where they help explain why something is happening.

- Avoid clever tricks, dense one-liners, and unnecessary abstraction.