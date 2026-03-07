# Stop and investigate when the same task repeats

## What counts as "the same task"

"The same task" means the user is asking you to do the same thing again or for the same outcome, even if the wording changes.

Examples are similar in meaning to "do it again", "try again", "that's still not right", "same issue", or "please redo this", "what about", "I'm not sure".

If the user asks you to do the same task more than three times, you must assume something important is unclear.

You must stop after the third attempt and consider a fundamentally different approach. Ask clarifying questions to resolve the ambiguity.

## Guidelines

After three repeats, assume at least one of these is true:

  1. You are solving a different problem than the user intends.

  2. You are relying on an assumption that is false.

  3. The user is asking a question that cannot produce the result they want.

  4. The user has not provided required information.

  5. The user has provided incorrect information.

  6. The user's expectations do not match the current state of the codebase.

After the third repeat:

  1. You must switch from "doing" to "investigating".

  2. You must not make further code changes until you complete the investigation steps below.

## Investigation Steps

### Step 1: Write a short problem statement

Write one problem statement for each distinct issue you observe.

Each problem statement must describe one observable failure.

Each problem statement must be written in one or two sentences.

### Step 2: Confirm the request in your own words

Restate the user's request in your own words.

Your restatement must be specific enough that the user can say "yes" or "no".

### Step 3: Start from a user-facing boundary

Start from the closest user-facing boundary that demonstrates the problem.

A user-facing boundary is a failing test, a public function call, a CLI command, or an HTTP endpoint.

Do not start by editing internal code.

### Step 4: Trace only as far as needed

Follow the execution path into the code only until you find the most likely cause.

Stop tracing when you can name a concrete cause to verify next.

### Step 5: Re-check evidence

Look for existing patterns, constraints, or conventions in the codebase that apply to this area.

Re-run and inspect failing tests if tests exist.

Do not assume tests are correct.

If a test appears incorrect, state why it might be incorrect and what evidence would confirm that.

### Step 6: Resolve unclear behaviour with examples

If the intended behaviour is unclear or disputed, you must define it before changing code.

Use example mapping to write concrete input -> output examples.

Use those examples to confirm assumptions and narrow the root cause.

## Communication rules during investigation

You must not work in silence.

You must document decisions and findings as you go.

Before acting on any user message, check whether the message has more than one reasonable interpretation.

If the message has more than one reasonable interpretation, you must ask at least two clarifying questions before proceeding.

If the message seems to have one interpretation, you must still look for evidence in the codebase that supports that interpretation.

Evidence includes existing functions, tests, documentation, naming patterns, or similar code.

If you cannot find supporting evidence and you are not implementing a new feature, you must pause.

When you pause, you must either ask a clarifying question or present two likely interpretations.

When you present interpretations, you must describe what evidence would confirm each one.