# Shotgun Surgery

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Category

Change Preventers

## Description

A single change requires modifications to many different modules scattered across the codebase. This is the opposite of Divergent Change-instead of one module changing for many reasons, one reason causes changes in many modules.

## Signs and Symptoms

- A simple feature change requires editing 5+ files.
- The same type of change (e.g., adding a field) must be made in multiple places.
- Forgetting to update one location causes bugs.
- Related logic is scattered across many modules.
- Changes to a concept require a mental checklist of "all the places to update."

## Causes

- Over-decomposition of related functionality.
- Lack of centralization for cross-cutting concerns.
- Copy-paste of similar logic instead of extraction.
- Missing abstractions that would consolidate related code.

## Example

```elixir
# Adding a new user field requires changes in ALL these modules:

defmodule MyApp.Users.Schema do
  schema "users" do
    field :name, :string
    field :email, :string
    # Must add: field :phone, :string
  end
end

defmodule MyApp.Users.Changeset do
  def create_changeset(user, attrs) do
    user
    |> cast(attrs, [:name, :email])  # Must add :phone
    |> validate_required([:name, :email])  # Must add :phone
  end
end

defmodule MyApp.Users.Query do
  def search(term) do
    from u in User,
      where: ilike(u.name, ^"%#{term}%") or ilike(u.email, ^"%#{term}%")
      # Must add: or ilike(u.phone, ^"%#{term}%")
  end
end

defmodule MyApp.Users.Serializer do
  def to_json(user) do
    %{
      name: user.name,
      email: user.email
      # Must add: phone: user.phone
    }
  end
end

defmodule MyApp.Users.CSV do
  def headers, do: ["name", "email"]  # Must add "phone"

  def row(user), do: [user.name, user.email]  # Must add user.phone
end

defmodule MyApp.Users.Form do
  def fields, do: [:name, :email]  # Must add :phone
end

defmodule MyAppWeb.UserController do
  def user_params(params) do
    Map.take(params, ["name", "email"])  # Must add "phone"
  end
end
```

## Refactored

```elixir
defmodule MyApp.Users.Schema do
  @fields [:name, :email, :phone]
  @required_fields [:name, :email]
  @searchable_fields [:name, :email, :phone]

  def fields, do: @fields
  def required_fields, do: @required_fields
  def searchable_fields, do: @searchable_fields

  schema "users" do
    field :name, :string
    field :email, :string
    field :phone, :string
  end
end

defmodule MyApp.Users.Changeset do
  alias MyApp.Users.Schema

  def create_changeset(user, attrs) do
    user
    |> cast(attrs, Schema.fields())
    |> validate_required(Schema.required_fields())
  end
end

defmodule MyApp.Users.Query do
  alias MyApp.Users.Schema

  def search(term) do
    conditions = Enum.reduce(Schema.searchable_fields(), false, fn field, acc ->
      dynamic([u], ilike(field(u, ^field), ^"%#{term}%") or ^acc)
    end)

    from u in User, where: ^conditions
  end
end

defmodule MyApp.Users.Serializer do
  alias MyApp.Users.Schema

  def to_json(user) do
    Map.take(user, Schema.fields())
  end
end

defmodule MyApp.Users.CSV do
  alias MyApp.Users.Schema

  def headers, do: Enum.map(Schema.fields(), &to_string/1)

  def row(user) do
    Enum.map(Schema.fields(), &Map.get(user, &1))
  end
end

defmodule MyAppWeb.UserController do
  alias MyApp.Users.Schema

  def user_params(params) do
    allowed = Enum.map(Schema.fields(), &to_string/1)
    Map.take(params, allowed)
  end
end
```

## Treatment

- **Move Function**: Consolidate scattered logic into one module.
- **Inline Module**: If modules are too granular, combine them.
- Centralize repeated field lists, constants, and metadata in one place.
- Use module attributes when one shared source of truth reduces the number of edits.

## Why Refactor

- Single point of change for related modifications.
- Reduced risk of forgetting to update a location.
- Easier to understand the full scope of a concept.
- Faster development with fewer files to modify.

## Centralization Strategies

| Scattered Data | Centralization Approach |
|----------------|------------------------|
| Field lists | Module attributes in schema |
| Validation rules | Changeset module with shared rules |
| Serialization fields | Derive from schema fields |
| Configuration | Application config or dedicated config module |
| Error messages | Dedicated errors module |

## Related Smells

- `Divergent Change` (opposite problem)
- `Duplicate Code`
- `Feature Envy`

## Related Refactoring Techniques

- `Move Function`
- `Inline Module`
- `Extract Module`
- `Introduce Parameter Struct`
