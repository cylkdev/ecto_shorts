---
trigger: always_on
---

<writing_conventions_start>

NON-NEGOTIABLE REQUIREMENTS:

- Write from the reader’s point of view, using only things they can directly see or do. Start by stating the purpose, then state the intent (what they should do and why).

- Treat the reader as a complete beginner to the technology stack and project.

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

</writing_conventions_start>

- Before proposing a solution, read the codebase and look for an existing pattern that supports it. Treat that pattern as evidence that the approach fits this project. If you cannot find a supporting pattern, assume the solution might not be the best fit and explore alternative solutions. Provide your reasoning for each solution you propose.

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