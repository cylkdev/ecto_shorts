---
trigger: always_on
---

<writing_conventions>

# Writing Conventions

Writing conventions are integral rules that must be integrated into your compilation process as you write. These writing conventions are strict rules that must be followed to the _letter_:

- Do not use emdash (—), Instead use a hyphen (-).

- Do not use special characters that a human cannot easily replicate on a standard keyboard.

- Write one sentence at a time. Prefer accuracy over speed. After writing a paragraph, do a thorough review and verify when you have written is following the writing conventions.

- Write from the reader’s point of view, using only things they can directly see or do. Start by stating the purpose, then state the intent (what they should do and why).

- Treat the reader as a complete beginner to both the technology stack and this project. Write as if they have no prior context, and include any definitions or pre-requisite information they need to succeed.

- Treat the reader as a complete beginner and write in a way that can be understood at a glance:

  - Prefer short paragraphs. Break up dense text with blank lines.

  - Start with the simplest concepts first. Introduce more advanced details only after the basics are clear.

  - Explain progressively: each step should build on the previous one.

  - Use examples, small diagrams, and illustrations throughout.

  - Describe things from the reader’s observable boundary (what they would see or do).

  - Use analogies when they make an idea easier to grasp, but keep them grounded and consistent.

- Use 4 spaces to indent code blocks.

- Write in plain language. Use clear, concise language and avoid jargon.

- Include examples where helpful.

- Use links to external resources where helpful.

- Use headings and subheadings to organize content.

- Use bullet points when order does not matter (a set of options, facts, or requirements).

- Use a list when you have 3 or more items. If you are naming three or more things (steps, rules, options, examples, requirements), format them as either bullet points or a numbered list.

- Use numbered lists for sequences; bullets for collections.

- Use numbered lists when order matters (steps someone must follow in a specific order).

- Limit list item length.

- Each list item must be 25 words or fewer.

- If an item needs more than 25 words, split it into a short parent bullet and indented sub-bullets.

- Keep lists scannable:
  - No more than 7 items in a single list.
  - Use consistent formatting and spacing.
  - If you have 8+ items, split into multiple lists with headings, or group into categories (each category becomes its own short list).

- Use a table when it helps the reader compare multiple items side-by-side using the same set of facts (the same “columns”). If a list is easy to scan, don’t use a table; instead use prose and code examples.

A table is for “compare these things side-by-side using the same columns”.

Example:

    | Plan       | Monthly limit  | Support response |
    |------------|----------------|------------------|
    | Basic      | 100 requests   | 24 hours         |
    | Pro        | 1,000 requests | 4 hours          |
    | Enterprise | Unlimited      | 1 hour           |

The benefit of this approach is that each row shares the same attributes, and the reader can scan across the columns to find the information they need.

A list is for “here are the things you can pass in” or “here are the rules”. Each item stands on its own.

Example of a list in prose:

    ## Short description

    - Install the tool.
    - Create a new project.
    - Add your configuration file.
    - Run the command.
    - Check the output and fix any errors.
    - Repeat until the task is complete.

Example of lists in function documentation:

    @doc """
    Short action-oriented description of what this function does.

    ## Parameters

      * `id` (`binary() | integer()`) - the ID of the record to update
      * `attrs` (`map()`) - the attributes to update

    ## Options

      * `:timeout` (`integer()`) - how long to wait, in milliseconds
      * `:retries` (`integer()`) - how many times to retry on failure
      * `:log`     (`boolean()`) - true to log the operation

    ## Examples

        iex> MyLib.update(id, %{name: "New Name"})
        {:ok, %MyLib.Record{name: "New Name"}}
    """

- Use descriptive names for variables and functions. Focus on purpose and then intent when naming.

If you follow the writing conventions a stateless coding agent or human novice that write
and consistently produce the same text. These writing conventions makes writing clear, concise, and easy to understand.

</writing_conventions>

<operating_guidelines>

# Operating Guidelines

- If the user asks you to repeat the same task more than three times, treat that as a sign you and the user are not aligned. It means one or more of these are true:

  - You are solving a different problem than the user intended.

  - You are relying on an assumption that is wrong.

  - The user is not asking the right question.

  - The user hasn't given you enough information to solve the problem.

  - The user hasn't given you the correct information to solve the problem.

  - There is a mismatch between the user's expectations and the actual state of the codebase.

After the third time you are asked to repeat the same task, stop making changes. Instead, assume everything might be wrong and there needs to be a fundamentally different approach. Write a problem statement for each problem to decompose it to small steps. Reason through each step systematically, considering different angles and documenting your findings.

  - Re-read the user’s request and restate it in your own words.

  - Re-scan the codebase for an existing pattern or constraint you may have missed.

  - Re-check any failing tests, because tests can be outdated or incorrect.

  - Do not assume tests are correct. If you keep changing code to satisfy a wrong test, you will reinforce the wrong behavior and can get stuck without finishing the real task.

Start your investigation at the user-facing boundary (for example: a test, a public function, or an HTTP endpoint). Then follow the execution path into the code only as far as you need to in order to find the real cause.

A common failure mode is treating the current implementation as the “source of truth” when the intended behavior was never clearly defined. When that happens, the code may appear to work while still being wrong, because effort went into implementation details instead of explicitly stating the behavior first.

If you suspect the behavior is unclear or disputed, use example mapping to restate the expected behavior as concrete examples, re-check your assumptions, and narrow down the root cause quickly.

- Do not work in silence. Update any documents you are using as you make progress or decisions.

- Before you act on a user message, confirm that you understand what the user is asking for:

  - If the message could reasonably be interpreted in more than one way, do not guess; you must ask at least two clarifying questions before proceeding.

  - If the message seems clear, confirm you can support it with evidence a human novice can observe before you start. Do this by scanning the codebase for the feature, module, or pattern the request is talking about. Your goal is to find concrete evidence (existing functions, tests, docs, naming patterns, or similar code) that supports your interpretation. If you cannot find supporting evidence and you are not implementing a new feature, treat that as a warning sign: pause and either ask a clarifying question or propose the two most likely interpretations and explain what evidence would confirm each one.

</operating_guidelines>
