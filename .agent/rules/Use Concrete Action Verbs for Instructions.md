# Use Concrete Action Verbs for Instructions

## Purpose

Use this rule when the reader must do something. This includes checklists, procedures, step-by-step guides, playbooks, runbooks, task lists, and acceptance steps.

Use concrete action verbs to make it explicitly clear what the reader is being asked to do. Apply this rule to task steps and checklist items, not to explanatory sections.

The goal is simple: a novice reader should be able to look at each instruction and know exactly what action to take without guessing.

## Core Rule

If the sentence tells the reader to do something, write it as a concrete action.

If the sentence explains, defines, or clarifies something, write it as plain description.

Match the sentence style to its job.

## Definitions

### Instruction

An instruction tells the reader to perform an action.

Examples:

    - Run `mix test`.
    - Open `lib/my_app/example.ex`.
    - Copy the failing output into your notes.

### Explanation

An explanation helps the reader understand something. It does not tell the reader to perform an action.

Examples:

    - This test verifies that the function returns the expected value for invalid input.
    - The repository root is the directory that contains the project's `mix.exs`.
    - This change matters because it keeps query construction in one visible shape.

### Concrete Action Verb

A concrete action verb names a specific action the reader can directly perform or observe.

Examples:

    - run
    - open
    - copy
    - paste
    - write
    - choose
    - select
    - change
    - edit
    - verify

### Abstract Process Verb

An abstract process verb names a general responsibility or outcome, but does not tell the reader exactly what to do next.

Examples:

    - handle
    - address
    - manage
    - ensure

### Context-Specific Capture Verb

A context-specific capture verb can be acceptable when the sentence names both what to capture and where it belongs.

Examples:

    - Record the decision in the Decision Log.
    - Document the reason in the plan note at the bottom of the file.
    - Log the failing command in your notes.

### Executor-Facing

Executor-facing writing is written for the person doing the task. It uses actions that person can actually take.

Good executor-facing instruction:

    - Copy the error message into your notes.

Not executor-facing enough:

    - Handle the error documentation.

## When to Use This Rule

- Use it for checklists.
- Use it for numbered procedures.
- Use it for setup steps.
- Use it for acceptance steps.
- Use it for any sentence that tells the reader to act.

## When Not to Use This Rule

Do not force concrete action verbs into sentences whose job is explanation.

Use plain descriptive language when the sentence:

- defines a term
- explains why something matters
- clarifies context
- describes expected behaviour

## How to Apply the Rule

- Use concrete executor-facing verbs when the sentence tells the reader to perform a task.
- Use plain descriptive language when the sentence explains, defines, or clarifies something.
- Match the sentence style to its job.

### Step 1. Identify the job of the sentence

Ask: is this sentence telling the reader to do something, or helping the reader understand something?

If it tells the reader to act, write an instruction.

If it explains meaning or context, write an explanation.

### Step 2. Start instructions with a concrete verb

Start each task step or checklist item with a verb that names the exact action.

- Use direct task verbs that tell the reader exactly what to do.
- Start each checklist item with a concrete action such as `run`, `copy`, `paste`, `write`, `choose`, `select`, `change`, `edit`, or `verify`.
- Use concrete executor-facing actions, not abstract process verbs.
- Use `record`, `document`, or `log` only when the step names both what to capture and where it belongs.
- Avoid vague process verbs such as `handle`, `address`, `manage`, or `ensure`.

Less clear:

    - Record the command.

Acceptable:

    - Record the decision in the Decision Log.

### Step 3. Name the exact object of the action

Do not stop at the verb. Also name what the reader should act on.

Less clear:

    - Verify the result.

More clear:

    - Verify that the command ends with `0 failures`.

### Step 4. Split vague or combined actions into separate steps

If one sentence hides multiple actions, break it into multiple instructions.

Less clear:

    - Record the exact command that reproduces the failure.

More clear:

    - Run the exact command that reproduces the failure.
    - Copy that command into your notes.

### Step 5. Preserve the original meaning

When you rewrite an instruction, keep the same task, order, and expected outcome.

Make the action clearer, but do not add new work, remove required work, or change what success means.

### Step 6. Keep explanations descriptive

Do not turn explanations into commands.

Write explanations so the reader can understand the task, the context, or the reason for a step.

## Self-Check

Before you keep an instruction, check it against these questions:

- Can a novice perform the step without guessing what action to take?
- Does the verb name a visible action?
- Does the step say what to act on?
- Can the reader tell when the step is complete?
- If the sentence uses `record`, `document`, or `log`, does it say what to capture and where to put it?
- If the sentence is explanation, does it avoid sounding like a command?
- Did the rewrite keep the original task and expected outcome?

If the answer to any relevant question is no, rewrite the sentence.

## Examples

### Instructions vs Explanations

Checklist or procedure:

    - Run `mix test`.
    - Copy the failing output into your notes.
    - Open `lib/my_app/example.ex`.
    - Change the function to return the expected value.

Explanation:

    - This test verifies that the function returns the expected value for invalid input.
    - The repository root is the directory that contains the project's `mix.exs`.
    - This change matters because it keeps query construction in one visible shape.

### Replace Abstract Verbs with Concrete Actions

Do not write:

    - Record the exact command that reproduces the failure.

Write:

    - Run the exact command that reproduces the failure.
    - Copy that command into your notes.

Do not write:

    - Record the failing output.

Write:

    - Copy the failing output into your notes.
    - Include the failing test name and the final failure count.

Acceptable when fully specified:

    - Record the decision in the Decision Log.

### More Rewrites

Do not write:

    - Ensure the file contains the new function.

Write:

    - Open the file.
    - Add the new function.
    - Save the file.

Do not write:

    - Handle the failing test output.

Write:

    - Copy the failing test output into your notes.
    - Highlight the first line that shows the mismatch.
