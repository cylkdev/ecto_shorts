# Writing Conventions

Writing conventions are integral rules that must be integrated into your compilation process as you write. These writing conventions are strict rules that must be followed to the _letter_:

- Do not use emdash (—), Use hyphen (-).

- Do not use special characters that a human cannot easily replicate on a standard keyboard.

- Write one sentence at a time. Prefer accuracy over speed. After writing a paragraph, do a thorough review and verify when you have written is following the writing conventions.

- Write from the reader's point of view, using only things they can directly see or do. Start by stating the purpose, then state the intent (what they should do and why).

- Treat the reader as a complete beginner to both the technology stack and this project. Write as if they have no prior context, and include any definitions or pre-requisite information they need to succeed.

- Treat the reader as a complete beginner and write in a way that can be understood at a glance:

  - Prefer short paragraphs. Break up dense text with blank lines.

  - Start with the simplest concepts first. Introduce more advanced details only after the basics are clear.

  - Explain progressively: each step should build on the previous one.

  - Use examples, small diagrams, and illustrations throughout.

  - Describe things from the reader's observable boundary (what they would see or do).

  - Use analogies when they make an idea easier to grasp, but keep them grounded and consistent.

- Use 4 spaces to indent code blocks.

- Write in plain language. Use clear, concise language and avoid jargon.

- Include examples where helpful.

## Lists and Bullet Points

- Use links to external resources where helpful.

- Use headings and subheadings to organize content.

- Use bullet points when order does not matter (a set of options, facts, or requirements).

- Use a list when you have 3 or more items. If you are naming three or more things (steps, rules, options, examples, requirements), format them as either bullet points or a numbered list.

- Use numbered lists for sequences; bullets for collections.

- Use numbered lists when order matters (steps someone must follow in a specific order).

- Limit list item length.

- Each list item must be 25 words or less.

- If an item needs more than 25 words, split it into a short parent bullet and indented sub-bullets.

- Each list must be easy for a human to scan and understand:
  - No more than 7 items in a single list.
  - Use consistent formatting and spacing.
  - If you have 8+ items, split into multiple lists with headings, or group into categories (each category becomes its own short list).

## Tables vs Lists

Use a table when it helps the reader compare multiple items side-by-side using the same set of facts (the same "columns"). If a list is easy to scan, don't use a table; instead use prose and code examples.

A table is for "compare these things side-by-side using the same columns".

Example:

    | Plan       | Monthly limit  | Support response |
    |------------|----------------|------------------|
    | Basic      | 100 requests   | 24 hours         |
    | Pro        | 1,000 requests | 4 hours          |
    | Enterprise | Unlimited      | 1 hour           |

The benefit of this approach is that each row shares the same attributes, and the reader can scan across the columns to find the information they need.

A list is for "here are the things you can pass in" or "here are the rules". Each item stands on its own.

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
