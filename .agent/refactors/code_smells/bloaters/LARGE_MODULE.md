# Large Module

## Category

Bloaters

## Description

A module contains too many functions, too many lines, or too many unrelated responsibilities. In Elixir, modules should have a single, cohesive purpose. A large module often indicates that multiple concerns have been bundled together.

## Signs and Symptoms

- The module exceeds 300-500 lines of code.
- The module contains functions that operate on different data types or domains.
- Difficulty describing the module's purpose in a single sentence.
- Functions in the module rarely call each other.
- The module has many public functions with unrelated purposes.
- Developers frequently need to scroll or search to find relevant functions.
- The module name is vague (e.g., `Utils`, `Helpers`, `Common`).

## Causes

- Treating modules as namespaces rather than cohesive units.
- Adding "just one more function" without considering module boundaries.
- Fear of creating "too many" modules.
- Lack of clear domain boundaries in the application.
- Migrating from languages where large classes are common.

## Example

```elixir
defmodule MyApp.Utils do
  # User-related functions
  def format_user_name(user), do: "#{user.first_name} #{user.last_name}"
  def validate_email(email), do: String.contains?(email, "@")
  def hash_password(password), do: Bcrypt.hash_pwd_salt(password)

  # Order-related functions
  def calculate_order_total(items), do: Enum.sum(Enum.map(items, & &1.price))
  def format_order_number(id), do: "ORD-#{String.pad_leading(to_string(id), 6, "0")}"

  # Date utilities
  def format_date(date), do: Calendar.strftime(date, "%Y-%m-%d")
  def days_between(date1, date2), do: Date.diff(date2, date1)

  # String utilities
  def slugify(string), do: string |> String.downcase() |> String.replace(~r/\s+/, "-")
  def truncate(string, length), do: String.slice(string, 0, length)

  # ... 50 more unrelated functions
end
```

## Refactored

```elixir
defmodule MyApp.Users.Formatter do
  def format_name(user), do: "#{user.first_name} #{user.last_name}"
end

defmodule MyApp.Users.Validator do
  def valid_email?(email), do: String.contains?(email, "@")
end

defmodule MyApp.Users.Password do
  def hash(password), do: Bcrypt.hash_pwd_salt(password)
end

defmodule MyApp.Orders.Calculator do
  def total(items), do: Enum.sum(Enum.map(items, & &1.price))
end

defmodule MyApp.Orders.Formatter do
  def order_number(id), do: "ORD-#{String.pad_leading(to_string(id), 6, "0")}"
end

defmodule MyApp.DateHelpers do
  def format(date), do: Calendar.strftime(date, "%Y-%m-%d")
  def days_between(date1, date2), do: Date.diff(date2, date1)
end

defmodule MyApp.StringHelpers do
  def slugify(string), do: string |> String.downcase() |> String.replace(~r/\s+/, "-")
  def truncate(string, length), do: String.slice(string, 0, length)
end
```

## Treatment

- **Extract Module**: Move related functions into dedicated modules.
- **Move Function**: Relocate functions to modules where they belong.
- **Extract Behavior**: If the module implements multiple interfaces, separate them.
- **Group by Domain**: Organize modules by business domain, not technical layer.

## Why Refactor

- Smaller modules are easier to understand and maintain.
- Clear module boundaries improve code navigation.
- Related functions are easier to find when grouped cohesively.
- Testing becomes simpler with focused modules.
- Compilation is faster with smaller modules (Elixir recompiles changed modules).

## When to Ignore

- Facade modules that intentionally aggregate related functionality for external consumers.
- Protocol implementations that must live in a single module.
- Modules that are large but highly cohesive (all functions work on the same data type).

## Related Smells

- `Long Function`
- `Divergent Change`
- `Shotgun Surgery`

## Related Refactoring Techniques

- `Extract Module`
- `Move Function`
- `Extract Behavior`
