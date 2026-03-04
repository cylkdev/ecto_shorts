# MADR Architecture Decision Records (ADRs)

This document describes the requirements for writing Architecture Decision Records (ADRs) using the MADR template.

Treat the reader as a complete beginner to this repository. They have only the current working tree and this single PLANS.md file. They have no memory of prior decisions and no external context.

## How to use this file

When authoring an ADR, follow this file to the letter. If it is not in your context, refresh your memory by reading the entire file.

When drafting an ADR, do not ask the user for "next steps". Proceed by writing the ADR, improving it until it is self-contained, and then treating it as ready for review.

When revising an ADR, do not silently change its meaning. If the decision changes, create a new ADR and mark the old one as superseded. Only do in-place edits for clarity, typos, formatting, or adding missing evidence that does not alter the decision.

ADRs are meant to answer "why did we choose this?" for future maintainers. They must be understandable without meetings, chat logs, or tribal knowledge.

## Non-negotiable requirements

Every ADR must be fully self-contained. Self-contained means that in its current form it contains all knowledge and instructions needed for a novice to understand the decision, evaluate its tradeoffs, and follow it.

Every ADR must record a single decision. If you are trying to decide two things, split it into two ADRs.

Every ADR must describe observable consequences. A consequence is something that becomes true in the system or in the team workflow after adopting the decision.

Every ADR must define every term of art in plain language or not use it.

Every ADR must be written so a future reader can answer these questions without looking anywhere else:
What problem were we solving?
What options did we seriously consider?
Why did we choose the option we chose?
What do we gain and what do we give up?
How do we know the decision was applied correctly?
When should we revisit the decision?

## Where ADRs live and how they are named

Store ADRs in a single folder that is easy to find from the repository root.

Use a predictable naming scheme so ADRs sort in a stable order and are easy to reference.

If this repository already has an ADR folder and naming convention, follow it.

If this repository does not, use:

    docs/adr/NNNN-short-decision-title.md

NNNN is a zero-padded sequence number (0001, 0002, …). The canonical output path and naming convention are governed by the `document-artifacts` rule in `.windsurf/rules/document-artifacts.md`.

## Status and lifecycle

Every ADR has a status. Status makes it clear whether the decision is only proposed, currently in force, or no longer in force.

Use one of:

  - proposed
  - accepted
  - rejected
  - deprecated
  - superseded by NNNN

If the decision is replaced, create a new ADR and set the old ADR to "superseded by NNNN". Link both directions in "More Information".

## How to compare options

Do not compare options at different abstraction levels. Compare like with like.

Do not list fake options. Only list options that a reasonable engineer could have chosen in this situation.

Put the chosen option first in "Considered Options" to make the outcome obvious.

Tie your justification to decision drivers. A decision driver is a constraint, quality goal, or risk that actually mattered here.

## Validation expectations

Validation is how a reader can confirm the decision was applied correctly.

Validation must be concrete. Prefer one of:
a repeatable command and the expected output
a test to run and what it proves
an observable runtime behaviour (logs, metrics, endpoints, UI behaviour)
a code review checklist with specific files and patterns to look for

If the decision is about architecture boundaries, name the boundaries and how violations will be detected.

## Formatting

Each ADR must be plain Markdown in a single file.

Keep the prose readable. Prefer sentences over lists.

Use lists only when they improve clarity. Use short lists. Avoid tables unless the decision truly needs a structured comparison.

If you include code, logs, commands, or transcripts, include them as indented blocks so they are easy to copy.

## Skeleton of a Good ADR (MADR)

    # <Short title of solved problem and chosen approach>

    ---
    Status: proposed
    Date: YYYY-MM-DD
    Deciders: []
    Consulted: []
    Informed: []
    ---

    ## Context and Problem Statement

    Write two to five sentences that explain the situation and the problem.
    State why this decision matters now.
    If helpful, phrase the problem as a question.

    ## Decision Drivers

    Write the constraints and quality goals that actually matter for this decision.
    Each driver must be something you can point to later and say "this influenced the choice".

    ## Considered Options

    List the options you seriously considered.
    Put the chosen option first.

    ## Decision Outcome

    Chosen option: "<option name>", because <justification that directly references the decision drivers>.

    ### Consequences

    Good, because <what improves and why>.
    Bad, because <what becomes harder or worse and why>.

    ## Validation

    Describe how a reader can confirm the decision is implemented and being followed.
    Include exact commands, tests, or observable behaviours.
    If the validation is ongoing, say how often it should be checked and by whom.

    ## Pros and Cons of the Options

    ### <Option 1>

    Describe the option in one or two sentences so the reader understands what it means in this repo.

    Good, because <reason>.
    Bad, because <reason>.
    Neutral (w.r.t. <concern>), because <reason>.

    ### <Option 2>

    Describe the option in one or two sentences.

    Good, because <reason>.
    Bad, because <reason>.
    Neutral (w.r.t. <concern>), because <reason>.

    ## More Information

    Link to related ADRs and key artifacts in this repository.
    Include any follow-up work that must happen for the decision to be successful.
    State when you would revisit the decision and what signal would trigger that revisit.