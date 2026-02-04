## Verification

When you begin any task, output the following as your first line:

---! AGENTS.md LOADED !---

## Task guideline

Every task follows one loop: **Plan → Act → Reflect**. Do not skip steps. Do not start writing code before the plan and checklist exist. The checklist is your source of truth for the entire task — rewrite it if scope changes, reference it constantly, never let it go stale.

---

### Setup

- Install deps: `mix deps.get`
- Compile: `mix compile`
- Start dev server: `mix phx.server`
- REPL: `iex -S mix`

---

### PLAN — before you write anything

1. Restate the task in one sentence. If you can't, ask for clarification.
2. Identify which modules, files, and functions are affected. Read them before touching them.
3. Decide the OTP shape: stateless function pipeline, `GenServer`, `Task`, `Supervisor`, or a combination. Default to the simplest option.
4. Identify the context boundary. Is this change self-contained in one context, or does it cross boundaries? If it crosses, plan the public API surface explicitly.
5. **Generate a checklist.** Write it out before any code. Every item is one discrete, verifiable unit of work. This checklist is the single source of truth for the task. Every subsequent step in this file references it.

Example checklist format:

```
- [ ] Add `MyApp.Orders.cancel/1` to the Orders context public API
- [ ] Implement cancellation logic with guard: order status must be `:pending`
- [ ] Return `{:error, :already_cancelled}` if status is not `:pending`
- [ ] Write test for happy path
- [ ] Write test for already-cancelled guard
- [ ] Update supervision tree if a new process was added
- [ ] Run `mix format`, `mix credo`, `mix test`
```

---

### ACT — execute against the checklist

- Work one checklist item at a time. Complete it fully before moving to the next.
- After every file write, re-read what you wrote. Catch mistakes before moving on.
- If you hit something unexpected — a dependency you didn't plan for, an API that doesn't exist, a module that does something different than expected — **stop and update the checklist** before continuing. Do not improvise silently.
- Do not touch files that are not in your checklist. If something else needs changing, add it to the checklist first.

#### Code style rules (applied during ACT)

- One module per file. Name modules after what they do.
- `@moduledoc` on every module. `@doc` on every public function. No exceptions.
- Internal helpers are `defp`. Unexported modules are `@moduledoc false`.
- Public functions return `{:ok, result}` or `{:error, reason}`. No bare exceptions for control flow.
- Use `with` for chaining. Always handle the `else` clause explicitly.
- Prefer pattern matching over conditionals.

#### Structure rules (applied during ACT)

- Business logic lives in context modules under `lib/`. Public API is at the context boundary only.
- Do not call across context boundaries except through the public API.
- Ecto schemas are data shapes. Validation and business rules live in the context.
- Use changesets for all mutations. Raw SQL only with a documented performance reason.

#### OTP rules (applied during ACT)

- Default to stateless functions. Add a process only for state, fault isolation, or distribution.
- `GenServer` for long-lived mutable state.
- `Task` for short-lived async work.
- Every process must have a home in a supervision tree. Update the tree when you add one.

---

### REFLECT — after each checklist item and at the end

After completing each checklist item, answer these three questions before moving on:

1. **Does it compile?** Run `mix compile`. If not, fix it now.
2. **Does it do what the checklist item said?** Re-read the item. Re-read the code. If there is a mismatch, fix it now.
3. **Did this change anything outside its scope?** If yes, add the new scope to the checklist.

At the end of the entire task, run the full reflection pass:

- Run `mix format`. Fix any issues.
- Run `mix credo`. Fix any issues.
- Run `mix test`. All tests must pass. Fix failures before finishing.
- Walk through every checklist item. Confirm each one is done. If any item is unchecked, do it now.
- Confirm no cross-context boundary violations were introduced.
- Confirm every new or changed public function has `@doc`.
- Confirm every new process has a supervision tree entry.

---

### Testing rules

- Every public function needs at least one test.
- Test the contract (inputs → outputs), not the implementation details.
- Mock external I/O at the boundary. Core logic must be pure and testable.
- Tag integration tests: `@tag :integration`.
- Add or update tests for every checklist item that touches code. This is not optional.
- Tests are part of the checklist. If they are not on it, add them before you start.

### Clear Communication Guidelines

#### 1. Be Explicit in Your System Prompt or Instructions

- "Write in short, direct sentences. Use simple commands."
- "Be brutally clear. No flowery language or pleasantries."
- "Give step-by-step instructions using imperative verbs: Do X. Then do Y."
- "Avoid qualifiers like 'you might want to' or 'consider'. Just say what to do."

#### 2. Provide Concrete Examples (Few-Shot Prompting)

**Bad:** "You might want to consider opening the file before proceeding."  
**Good:** "Open the file."

**Bad:** "It would be helpful to check the database connection."  
**Good:** "Check the database connection. If it fails, restart the service."

#### 3. Use Negative Examples

- "Don't use phrases like 'perhaps', 'maybe', 'you could try'"
- "Don't apologize or add context unless asked"
- "Don't use passive voice. Use active voice."

#### 4. Specify Format Constraints

- "Use numbered steps"
- "Start each instruction with an action verb"
- "Maximum 10 words per sentence"
- "No adjectives or adverbs unless technically necessary"

#### 5. Request a Specific Style

- "Write like a military drill sergeant"
- "Write like assembly instructions"
- "Write like emergency evacuation procedures"

### Example Prompt Template

```
You are a technical writer who creates brutally clear instructions.

Rules:
- Use imperative mood
- Keep sentences under 15 words
- No hedging words (maybe, perhaps, consider)
- Start each step with an action verb
- Be direct and literal
```

---

## Documentation Guideline

Generate module and function documentation for Elixir APIs following HexDocs conventions.

### Core Rules

Write documentation that is:

- Direct and instructional
- Progressive (simple to complex)
- Concrete with runnable examples
- Self-contained per section
- The vocabulary used in the documentation should be understood by a 18 year old high school student who is starting programming
- Do not write vague statements, always be clear.

Avoid:

- Hedging language ("might", "could", "perhaps")
- Apologetic tone
- Excessive adjectives
- Passive voice

### Module Documentation Structure

#### 1. Opening Summary (Required)

Write a one-line summary explaining what the module does. Start with an active verb describing the module's primary purpose.

**Pattern**: `[Module] [verb]s [what] [how/why]`

**Example**:

```elixir
# Good
Changesets allow filtering, type casting, validation, and constraints when manipulating structs.

# Bad
This module might help you work with changesets for various purposes.
```

#### 2. Conceptual Overview (If Complex)

Break down the module into 2-4 core concepts using bullet lists. Use parallel structure. Keep each item to one clear statement.

**Pattern**:

```markdown
Module does X by providing:

* Component A - what it does
* Component B - what it does  
* Component C - what it does
```

#### 3. How It Works (Required)

Explain the fundamental operation in 2-3 paragraphs. Use concrete examples immediately. Connect to real use cases.

Structure:

- State the problem
- Show the solution with code
- Explain key behavior

#### 4. Common Workflows (When Applicable)

Provide step-by-step patterns for typical tasks. Use imperative verbs. Show complete, runnable code

**Pattern**:

````markdown
### [Task Name]

1. Start with [action]
2. Apply [transformation]
3. Execute [operation]
   
```elixir
# Complete example
step_1_code
|> step_2_code
|> step_3_code
```
````

#### 5. Special Topics

Cover edge cases, advanced usage, or important caveats. Use clear section headers. Provide examples for each topic.

**Headers should be**:
- Action-oriented ("Working with X", "Handling Y")
- Not question-based
- Specific, not generic

#### 6. Important Notes (When Critical)

- Highlight critical information using blockquotes.
- State facts directly.
- Explain consequences.

**Pattern**:
```markdown
> NOTE: [Direct statement of fact]. [Consequence]. [Alternative if applicable].
````


---

### Function Documentation Structure

#### 1. Function Summary (Required)

- One sentence describing what the function does.
- Use active voice.
- Include key parameters in context.

**Pattern**: `[Verb]s [object] [context/constraint]`

**Examples**:

```elixir
# Good
Applies the given params as changes on the data according to the set of permitted keys.

# Bad  
This function might be used to apply some parameters to data.
```

#### 2. Behavior Description (Required)

Explain:

- What inputs are valid
- What the function returns
- When it executes (conditions)
- What it modifies

Use short paragraphs (2-3 sentences each). Lead with the most important behavior.

#### 3. Options (When Parameters Accept Them)

Document each option with:

- Name as code (`:option_name`)
- Type/valid values
- Default value
- Effect on behaviour

**Pattern**:

```markdown
### Options

* `:option_name` - what it controls, defaults to [value]/[description]
* `:another_option` - what it does. May be [values]. When [condition], [effect]
```

#### 4. Examples (Required)

Provide 2-5 examples showing:

- Basic usage (always first)
- Common variations
- Edge cases
- Error cases (when relevant)

Rules:

- Use `iex>` for `doctest` examples. 
- Use complete code blocks for full examples.
- Show actual output at the bottom of the test
- `iex>` can only appear on the first line of a test. Lines that should be executed 

**Pattern**:

````markdown
### Examples
```elixir
iex> MyApp.MyModule.basic_case()
expected_result

iex> MyApp.MyModule.variation_case()
different_result

# More complex example
code
|> MyApp.MyModule.first_call()
|> MyApp.MyModule.chained_calls()
#=> final_result
```
````

#### 5. Notes/Warnings (When Critical)

State important caveats directly.
Explain why the limitation exists.
Provide alternatives when available.

**Pattern**:

```markdown
Note [statement]. [Why]. [Alternative].
```

### Code Example Guidelines

#### Structure

- Always runnable without modification
- Include all necessary imports/aliases
- Use realistic data
- Show complete pipelines

#### Formatting

- One operation per line in pipelines
- Align pipeline operators
- Use meaningful variable names
- Add comments for complex logic only

#### Output

- Show expected results with `#=>`
- Use `%StructName{...}` for partial output
- Include type information when not obvious

**Example**:

```elixir
# Good
post = %Post{title: "Hello", author: "Jane"}
changeset = 
  post
  |> cast(params, [:title])
  |> validate_required([:title])
  
changeset.valid? #=> true

# Bad (not runnable)
cast(changeset, [:title])
# result depends on previous state
```

---

### Type Specification Documentation

When documenting types:

- Use proper `typespec` syntax
- Explain each complex type
- Link to related types
- Show usage in context

**Pattern**:

````markdown
### Types

# type_name()

```elixir
@type type_name() :: type_definition
```

[Description of what the type represents and when to use it]
````
### Cross-References

Link to related content using:
- Function references:  `ModuleName.function_name/arity` 
- Module references: `ModuleName` 
- External links: `[text](url)` for guides
- Section links: `[text](#section-header)` for same document

**When to link**:
- First mention of related function
- Alternative approaches
- Prerequisite knowledge
- More detailed information

**Example**:
```markdown
See `cast/4` for casting external data.
Check the [Associations cheatsheet](associations.html) for examples.
````

### Organizing Complex Information

#### Use Lists For

- Multiple related items
- Step-by-step processes
- Options/parameters
- Parallel concepts

#### Use Tables For

- Type mappings
- Comparisons
- Option references

#### Use Sections For

- Different use cases
- Conceptual divisions
- Progressive complexity

---
### Error and Edge Case Documentation

#### Document When

- Function can fail
- Input must meet constraints
- Behavior differs under conditions
- Race conditions possible

#### How to Document

State the condition directly. Show the error with example. Explain how to handle it.

**Pattern**:

````markdown
If [condition], [what happens]:

```elixir
# Code that triggers condition
result #=> {:error, reason}
```

To handle this, [solution].
````

### Elixir Doctest Rules

1. Format doctest examples as indented text. Never use code fences.
2. Indent all lines with exactly 4 spaces
3. Start first line with `iex>` prompt
4. Use `...>` for continuation lines
5. Show output without any prompt
6. Never wrap examples in ` ```elixir ` blocks
7. Include the full module name as well as the function for example `MyApp.MyModule.function_name()`

#### Rule: Do not use codefences

For code fences do not do this:

````
### Examples

```elixir
iex> add(1, 2)
3
```

````

This is bad because:
- No indentation
- Missing full module name

Instead you should tab-indent twice like this:

````
### Examples

		iex> MyApp.MyModule.add(1, 2)
		3

````

### Complete Example

```elixir
defmodule MyAPI.Users do
  @moduledoc """
  Creates and manages user accounts.
  
  This module provides functions for user registration, authentication,
  and profile management. It handles password hashing, email validation,
  and session management.
  
  ### Basic Usage
  
      iex> params = %{email: "user@example.com", password: "secure123"}
      ...> {:ok, user} = MyAPI.Users.register(params)
      %User{email: "user@example.com", ...}
  
  ### Authentication
  
  Authenticate users with email and password:
  
      case MyAPI.Users.authenticate(email, password) do
        {:ok, user} -> 
          # Login successful
        {:error, :invalid_credentials} ->
          # Show error
      end
  
  > NOTE: Passwords are hashed using Bcrypt. Never store plain passwords.
  """
  
  @doc """
  Registers a new user with the given parameters.
  
  Creates a user account after validating email format and password strength.
  Passwords are automatically hashed before storage.
  
  Returns `{:ok, user}` if successful or `{:error, changeset}` if validation fails.
  
  ### Parameters
  
  * `params` - a map with string or atom keys containing:
    * `email` - valid email address (required)
    * `password` - minimum 8 characters (required)
    * `name` - display name (optional)
  
  ### Examples
  
      iex> params = %{email: "user@example.com", password: "secure123"}
      ...> {:ok, user} = MyAPI.Users.register(params)
      %User{email: "user@example.com"}
      
      iex> params = %{email: "invalid", password: "short"}
      ...> {:error, changeset} = MyAPI.Users.register(params)
      ...> changeset.errors
      [email: {"has invalid format", []}, password: {"should be at least 8 characters", []}]
  """
  @spec register(map()) :: {:ok, User.t()} | {:error, Ecto.Changeset.t()}
  def register(params) do
    # Implementation
  end
end
````

### Validation Checklist

Before finalizing documentation:

- Functions that chain calls together pipe the first argument
- Module summary starts with active verb
- Every function has one-line summary
- All examples are runnable
- Options documented with defaults
- Edge cases covered
- No hedging language
- Cross-references present
- Error cases shown
- Code follows conventions
- Progressive complexity
- Examples in doctests for functions are not wrapped in "```elixir" or "```" blocks. Tests should be tab intended.