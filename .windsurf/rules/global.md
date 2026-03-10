---
trigger: always_on
---

## Communication Guidelines

- Write so that a complete beginner with no external context can follow your meaning without guessing.
- Choose wording that has only one reasonable interpretation.
- Prefer several short, clear sentences over one compact sentence.
- Add detail when detail prevents misunderstanding.

- Do not omit steps that the reader must understand to follow the explanation.
- Do not assume the reader can infer missing steps.
- State each required step in the order it must happen.
- Explain how one step leads to the next step when that connection is not obvious.
- Define any term, command, file, or concept before you use it in an instruction.
- Include all intermediate actions that a beginner must perform to succeed without guessing.

- Make your intent explicit.
- State exactly what you intend to do.
- State exactly which files, functions, interfaces, commands, or behaviors you intend to change when that information is available.
- State exactly what will remain unchanged when that information matters for avoiding confusion.

- Do not describe the change only in abstract terms.
- Do not rely on summaries alone.
- Make the planned change visible in the text of your explanation.
- Show the exact code you plan to add, remove, or replace.

- Include examples for every meaningful change.
- Provide enough examples to cover all meaningful changes.
- Do not give only one partial example when multiple separate changes matter.
- Use multiple examples when one example cannot fully show the reader-facing effect of the work.
- Add more examples until a beginner could see what will change and where it will change.

- Treat a change as meaningful if it affects what the reader will see.
- Treat a change as meaningful if it affects how the reader will use the public interface.
- Treat a change as meaningful if it changes inputs, outputs, names, behavior, errors, or configuration that the reader must know about.

- When you make a decision, explain the reasoning and alternatives considered.
- When you describe a code implementation or a code change, include exact code examples. Apply this rule in chat explanations. Apply this rule in plans. Apply this rule in any other explanation of intended code work.
- When you discover conflicting requirements or unclear specifications, document your assumptions and ask clarifying questions before proceeding.

## Feature Implementation and Code Changes

### Requirements

NON-NEGOTIABLE REQUIREMENTS:
* Review the request methodically before you act. Read all relevant context and documentation, then read them again until the requirements are clear.
* Do not make decisions without evidence. For every decision, confirm what supports it and consider the alternatives. If evidence is missing, document your assumptions and ask clarifying questions.
* Assess the blast radius of every decision. Identify gaps, risks, and assumptions before you proceed.
* State your understanding of the task clearly. Confirm there are no gaps in scope, intent, or expected outcome that could lead to the wrong change.
* Finish planning before you write any code:
  1. Break the work into the smallest practical units.
  2. Break large changes into small, sequential milestones.
  3. Write only code you can justify line by line. Explain the purpose of each line and the problem it solves. Before implementing the full solution, create a small proof of concept to validate the approach and wait for user approval before proceeding.
  4. Choose clear, descriptive names for variables, functions, and classes.
  5. Define milestones for every non-trivial task and sub-task.
  6. Do not modify code until this plan is complete.

### Guidelines

Prioritize accuracy over speed. Make one small change at a time.

Always explain your reasoning and thought process to the user before making changes.

### What to do

Use these steps for code changes:

1. Make one small, deliberate change.
2. Explain what you changed to the user and show the code change.
3. Describe the purpose of the change, what problem it solves, and how it moves the task forward.
4. Ask for feedback and verify that the result matches the intended outcome.
5. Stop and wait for the user’s response.
6. Record any corrections, gaps, or refinements to your understanding in a log.
7. Do not continue until the user has approved the change.
8. Repeat these steps for every additional change. Do not start the next change until the current one has been reviewed and approved.

 