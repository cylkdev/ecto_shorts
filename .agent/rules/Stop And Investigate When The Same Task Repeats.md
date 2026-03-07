# Stop And Investigate When The Same Task Repeats

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## What counts as "the same task"

"The same task" means the user is asking for the same outcome again, even if the wording changes.

Examples include requests similar in meaning to "do it again", "try again", "that's still not right", "same issue", "please redo this", or "what about".

If the user asks for the same task more than three times, assume something important is unclear.

After the third attempt, stop repeating the same execution path. Switch from doing to investigating.

## Guidelines

After three repeats, assume at least one of these is true:

1. You are solving a different problem than the user intends.
2. You are relying on an assumption that is false.
3. The user is asking a question that cannot produce the result they want.
4. The user has not provided required information.
5. The user has provided incorrect information.
6. The user's expectations do not match the current state of the codebase.

After the third repeat:

1. Stop making further code changes until the investigation produces a concrete next step.
2. Let one coordinator own the investigation question, the active user-observable boundary, and the next-step decision.
3. Fan out worker passes only after the investigation question is stable.
4. Collect worker-pass results before you decide whether to clarify behaviour, change code, or challenge the premise.

## Investigation Steps

### Step 1: Write a short problem statement

Write one problem statement for each distinct issue you observe.

Each problem statement must describe one observable failure.

Each problem statement must be written in one or two sentences.

### Step 2: Confirm the request in your own words

Restate the user's request in your own words.

Your restatement must be specific enough that the user can say "yes" or "no".

### Step 3: Start from a user-observable boundary

Start from the closest user-observable boundary that demonstrates the problem.

A user-observable boundary is a failing test, a public function call, a CLI command, or an HTTP endpoint.

Do not start by editing internal code.

### Step 4: Fan out bounded worker passes only after the boundary is fixed

A worker pass is one narrow evidence-gathering task such as reading one nearby test, tracing one caller path, checking one stacktrace branch, or comparing one existing pattern.

Use worker passes only after the coordinator has fixed the current problem statement and the current user-observable boundary.

### Step 5: Trace only as far as needed

Follow the execution path into the code only until you find the most likely cause.

Stop tracing when you can name a concrete cause to verify next.

### Step 6: Re-check evidence

Look for existing patterns, constraints, or conventions in the codebase that apply to this area.

Re-run and inspect failing tests if tests exist.

Do not assume tests are correct.

If a test appears incorrect, state why it might be incorrect and what evidence would confirm that.

### Step 7: Collect the evidence before deciding

Record worker-pass results in a mailbox such as an InvestigationLog, `Facts`, `Progress`, `Surprises & Discoveries`, or `Open Questions / Blockers`.

Collect that evidence before you decide whether the implementation is wrong, the expectation is wrong, or the intended behaviour is still unclear.

### Step 8: Resolve unclear behaviour with examples

If the intended behaviour is unclear or disputed, you must define it before changing code.

Use example mapping to write concrete input to output examples.

Use those examples to confirm assumptions and narrow the root cause.

## Communication Rules During Investigation

You must not work in silence.

You must document decisions and findings as you go.

Before acting on any user message, check whether the message has more than one reasonable interpretation.

If the message has more than one reasonable interpretation, you must ask clarifying questions or present the competing interpretations before proceeding.

If the message seems to have one interpretation, you must still look for evidence in the codebase that supports that interpretation.

Evidence includes existing functions, tests, documentation, naming patterns, or similar code.

If you cannot find supporting evidence and you are not implementing a new feature, you must pause.

When you pause, either ask a clarifying question or present the most likely interpretations together with the evidence that would confirm each one.
