---
trigger: model_decision
description: Code is Ecto schema/context shaped: use Ecto.Schema with schema/field/belongs_to/has_many and changeset/2; schema modules define composable query helpers (import Ecto.Query). Contexts call EctoShorts.Actions instead of Repo.* directly and avoid importing Ecto.Query.
---

# Schema and Context Patterns

## Schema Structure
All schemas use `LearnElixirPG.SchemaTemplate` behaviour where applicable:

```elixir
defmodule LearnElixirPG.ContextName.SchemaName do
  use Ecto.Schema
  import Ecto.Changeset

  @required_fields [:field1, :field2]
  @available_fields [:optional_field | @required_fields]

  schema "table_name" do
    field :field1, :string
    field :optional_field, :string
    belongs_to :organization, LearnElixirPG.Accounts.Organization
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, @available_fields)
    |> validate_required(@required_fields)
    |> foreign_key_constraint(:organization_id)
  end
end
```

## UUID Primary Keys
For high-throughput schemas needing distributed ID generation:
```elixir
@primary_key {:id, :binary_id, autogenerate: true}
```

## Reusable Query Pattern
All `Ecto.Query` calls **must** live inside schema modules, never in contexts. Contexts compose schema query functions and pass the result to `EctoShorts.Actions`:

```elixir
defmodule LearnElixirPG.Education.Lesson do
  import Ecto.Query

  def by_course(query \\ __MODULE__, course_id) do
    where(query, [l], l.course_id == ^course_id)
  end

  def paginated(query \\ __MODULE__, limit, offset) do
    query
    |> order_by([l], desc: l.inserted_at)
    |> limit(^limit)
    |> offset(^offset)
  end
end
```

Reference: https://learn-elixir.dev/blogs/creating-reusable-ecto-code

## Context Modules
- **Never** `import Ecto.Query` in a context module
- **Never** call `Repo.all`, `Repo.get_by`, or similar `Repo` functions directly - use `EctoShorts.Actions` instead
- The only exception for direct `Repo` usage is `Repo.transaction` for `Ecto.Multi` operations
- Schemas should have a single `changeset/2` that handles all validations (including password hashing) so `Actions.create`/`Actions.update` work without needing direct `Repo.insert`/`Repo.update`
- Use `EctoShorts.Actions` for all CRUD:

```elixir
alias EctoShorts.Actions
def find(params), do: Actions.find(Schema, params)
def all(params \\ %{}), do: Actions.all(Schema, params)
def create(params), do: Actions.create(Schema, params)
```

Compose schema queries with `Actions.all/1`:
```elixir
def list_messages(organization_id, opts \\ []) do
  Message
  |> Message.by_organization(organization_id)
  |> Message.paginated(limit, offset)
  |> Actions.all()
end
```

## Unified Changeset Pattern
Schemas must have a **single** `changeset/2` that handles all validations including password hashing. Do not create separate changesets like `registration_changeset` or `password_changeset`:

```elixir
def changeset(user, attrs \\ %{}) do
  user
  |> cast(attrs, @available_fields)
  |> validate_required(@required_fields)
  |> validate_email()
  |> validate_password()
  |> validate_confirmation(:password, message: "does not match password")
  |> maybe_hash_password()
end

defp maybe_hash_password(changeset) do
  password = get_change(changeset, :password)

  if password && changeset.valid? do
    changeset
    |> put_change(:password_hash, Bcrypt.hash_pwd_salt(password))
    |> delete_change(:password)
  else
    changeset
  end
end
```

This allows `EctoShorts.Actions.create/2` and `Actions.update/3` to work without needing direct `Repo.insert`/`Repo.update`.

## Type Specs
- Define `@type t :: %__MODULE__{}` in schemas that need it for typespecs

## Timestamps
Always use `:utc_datetime_usec` for timestamp precision.

## Indexes
- Add indexes for all foreign keys
- Add indexes for frequently queried fields
- Use composite indexes for common multi-column queries

## Factory Pattern
Create factories in `test/support/factory/<context>/`:
```elixir
defmodule LearnElixirPG.Support.Factory.ContextName.SchemaName do
  @behaviour FactoryEx

  @impl FactoryEx
  def schema, do: LearnElixirPG.ContextName.SchemaName

  @impl FactoryEx
  def repo, do: LearnElixirPG.Repo

  @impl FactoryEx
  def build(params \\ %{}) do
    Map.merge(%{field1: "default_#{System.unique_integer([:positive])}"}, params)
  end
end
```

## Checklist for New Schemas
1. Create schema in `lib/learn_elixir_pg/<context>/<schema>.ex`
2. Add CRUD functions to context module
3. Create migration in `priv/repo/migrations/`
4. Create factory in `test/support/factory/<context>/`
5. Add indexes for foreign keys and queried fields
