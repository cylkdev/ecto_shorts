# Dead Code

## Category

Dispensables

## Description

Code that is never executed: unused functions, unreachable branches, commented-out code, or modules that nothing references. Dead code clutters the codebase, confuses developers, and may cause false confidence in test coverage.

## Signs and Symptoms

- Functions with no callers (check with `mix xref graph --sink Module.function/arity`).
- Commented-out code blocks.
- Conditional branches that can never be reached.
- Modules that are never referenced.
- Variables that are assigned but never used.
- Pattern match clauses that can never match.
- Private functions that are never called.
- Configuration for features that were removed.

## Causes

- Fear of deleting code ("we might need it later").
- Incomplete refactoring that left orphaned code.
- Feature flags for features that were never enabled or were removed.
- Copy-pasted code where only part was needed.
- Defensive programming that handles impossible cases.

## Example

```elixir
defmodule MyApp.UserService do
  # Old implementation - keeping just in case
  # def create_user_v1(params) do
  #   %User{}
  #   |> User.changeset(params)
  #   |> Repo.insert()
  # end

  def create_user(params) do
    %User{}
    |> User.changeset(params)
    |> Repo.insert()
  end

  # This function is never called anywhere
  def legacy_migrate_user(old_user) do
    # Migration logic from 2019
    %User{
      name: old_user["name"],
      email: old_user["email"]
    }
  end

  def get_user(id) do
    case Repo.get(User, id) do
      nil -> {:error, :not_found}
      user -> {:ok, user}
    end
  end

  # Unreachable clause - previous clause matches all users
  def format_name(%User{name: name}), do: name
  def format_name(%User{first_name: f, last_name: l}), do: "#{f} #{l}"

  # Private function never called
  defp send_welcome_email(_user) do
    # TODO: implement
    :ok
  end

  def process(user, opts) do
    # Variable assigned but never used
    _debug_mode = Keyword.get(opts, :debug, false)

    # This branch is impossible - we already validated
    if is_nil(user.id) do
      raise "This should never happen"
    end

    do_process(user)
  end
end
```

## Refactored

```elixir
defmodule MyApp.UserService do
  def create_user(params) do
    %User{}
    |> User.changeset(params)
    |> Repo.insert()
  end

  def get_user(id) do
    case Repo.get(User, id) do
      nil -> {:error, :not_found}
      user -> {:ok, user}
    end
  end

  def format_name(%User{name: name}), do: name

  def process(user, _opts) do
    do_process(user)
  end
end
```

## Detection Tools

### Compiler Warnings
Elixir warns about unused variables and functions:

```bash
mix compile --warnings-as-errors
```

### Cross-Reference Analysis
Find unused functions:

```bash
# Find all callers of a function
mix xref graph --sink MyApp.UserService.legacy_migrate_user/1

# Find unreferenced modules
mix xref graph --format stats
```

### Credo
Detects various forms of dead code:

```bash
mix credo --strict
```

### Coverage Analysis
Uncovered code may be dead:

```bash
mix coveralls.html
```

## Treatment

- **Delete It**: Remove the dead code. Version control preserves history.
- **Remove Commented Code**: If it's commented out, delete it.
- **Simplify Conditionals**: Remove impossible branches.
- **Clean Up Unused Variables**: Remove or use them.

## Why Refactor

- Reduces cognitive load when reading code.
- Improves compilation time (slightly).
- Eliminates false test coverage.
- Makes the codebase easier to navigate.
- Removes potential confusion about what code is active.

## Common Objections and Responses

| Objection | Response |
|-----------|----------|
| "We might need it later" | Git preserves history; you can always recover it |
| "It documents how things used to work" | Write actual documentation instead |
| "It's harmless" | It confuses readers and may mask real issues |
| "Tests cover it" | Tests for dead code waste CI time |

## Prevention

- Delete code immediately when it becomes unused.
- Use feature flags that are cleaned up after rollout.
- Review PRs for orphaned code after refactoring.
- Run `mix credo` and `mix xref` in CI.
- Treat compiler warnings as errors.

## Related Smells

- `Comments`
- `Speculative Generality`
- `Lazy Module`

## Related Refactoring Techniques

- `Remove Dead Code`
- `Collapse Module Hierarchy`
- `Inline Function`
