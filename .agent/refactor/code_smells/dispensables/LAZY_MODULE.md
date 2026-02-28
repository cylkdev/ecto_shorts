# Lazy Module

## Category

Dispensables

## Description

A module that does too little to justify its existence. It may have been created with good intentions but never grew to fulfill its purpose, or it may have shrunk after refactoring removed most of its functionality.

## Signs and Symptoms

- A module with only 1-2 small functions.
- A module that simply delegates to another module without adding value.
- A module created "for future expansion" that never expanded.
- A wrapper module that adds no behaviour or abstraction.
- A module whose functions could easily live in a related module.

## Causes

- Over-engineering and premature abstraction.
- Refactoring that moved functionality elsewhere but left the shell.
- Speculative design for features that never materialized.
- Misunderstanding of when to create a new module.
- Copy-pasting module structure from other projects.

## Example

```elixir
defmodule MyApp.Users.NameFormatter do
  def format(user) do
    "#{user.first_name} #{user.last_name}"
  end
end

defmodule MyApp.Users.EmailValidator do
  def valid?(email) do
    String.contains?(email, "@")
  end
end

defmodule MyApp.DateUtils do
  def today do
    Date.utc_today()
  end
end

defmodule MyApp.StringWrapper do
  defdelegate downcase(string), to: String
  defdelegate upcase(string), to: String
  defdelegate trim(string), to: String
end
```

## Refactored

```elixir
defmodule MyApp.Users.User do
  defstruct [:first_name, :last_name, :email]

  # Inline the simple formatting
  def full_name(%__MODULE__{first_name: first, last_name: last}) do
    "#{first} #{last}"
  end

  # Inline the simple validation
  def valid_email?(%__MODULE__{email: email}) do
    String.contains?(email, "@")
  end
end

# DateUtils and StringWrapper deleted entirely
# - Use Date.utc_today() directly
# - Use String functions directly
```

## Treatment

- **Inline Module**: Move the module's functions into a related module.
- **Collapse Module Hierarchy**: Merge with parent or sibling module.
- **Delete Module**: If it only wraps standard library functions, remove it.

## Why Refactor

- Fewer modules means less navigation overhead.
- Related functionality is co-located.
- Reduces indirection and cognitive load.
- Eliminates maintenance burden of near-empty modules.

## When to Keep Small Modules

Not every small module is lazy. Keep modules that:

- **Define a clear boundary**: Even if small, it represents a distinct concept.
- **Will grow**: You have concrete plans to add functionality.
- **Implement a behaviour**: The module satisfies a contract.
- **Provide a stable API**: External code depends on this interface.

```elixir
# This small module is justified - it's a behaviour implementation
defmodule MyApp.Cache.NullCache do
  @behaviour MyApp.Cache

  @impl true
  def get(_key), do: nil

  @impl true
  def put(_key, _value), do: :ok

  @impl true
  def delete(_key), do: :ok
end
```

## Decision Guide

Ask these questions:

1. Does this module have a single, clear responsibility? → Keep if yes
2. Will this module grow with planned features? → Keep if yes
3. Does it implement a behaviour or protocol? → Keep if yes
4. Is it just wrapping another module? → Consider inlining
5. Could its functions live naturally in a related module? → Consider merging

## Related Smells

- `Speculative Generality`
- `Dead Code`
- `Middle Man`

## Related Refactoring Techniques

- `Inline Module`
- `Collapse Module Hierarchy`
- `Move Function`
