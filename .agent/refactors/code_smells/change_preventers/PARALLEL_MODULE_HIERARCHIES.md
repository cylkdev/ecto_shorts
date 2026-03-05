# Parallel Module Hierarchies

## Category

Change Preventers

## Description

Every time you create a module in one hierarchy, you must create a corresponding module in another hierarchy. This creates a maintenance burden where adding a new concept requires changes in multiple parallel structures.

## Signs and Symptoms

- Module namespaces mirror each other (e.g., `Models.User`, `Serializers.User`, `Validators.User`).
- Adding a new entity requires creating modules in 3+ different directories.
- Parallel directories with matching file names.
- One-to-one correspondence between modules in different namespaces.
- Boilerplate code that follows the same pattern across parallel modules.

## Causes

- Over-application of "separation of concerns" at the module level.
- Framework conventions that encourage parallel structures.
- Copying patterns from other languages without adaptation.
- Lack of consolidation strategies for related functionality.

## Example

```
lib/
├── models/
│   ├── user.ex
│   ├── order.ex
│   └── product.ex
├── validators/
│   ├── user_validator.ex
│   ├── order_validator.ex
│   └── product_validator.ex
├── serializers/
│   ├── user_serializer.ex
│   ├── order_serializer.ex
│   └── product_serializer.ex
├── queries/
│   ├── user_query.ex
│   ├── order_query.ex
│   └── product_query.ex
└── policies/
    ├── user_policy.ex
    ├── order_policy.ex
    └── product_policy.ex
```

```elixir
# Adding a new entity "Invoice" requires creating 5 new files:
# - models/invoice.ex
# - validators/invoice_validator.ex
# - serializers/invoice_serializer.ex
# - queries/invoice_query.ex
# - policies/invoice_policy.ex
```

## Refactored

```
lib/
├── my_app/
│   ├── users/
│   │   ├── user.ex          # Schema + changeset
│   │   ├── query.ex          # User-specific queries
│   │   └── policy.ex         # User authorization
│   ├── orders/
│   │   ├── order.ex
│   │   ├── query.ex
│   │   └── policy.ex
│   └── products/
│       ├── product.ex
│       ├── query.ex
│       └── policy.ex
```

```elixir
# Consolidate related functionality into the schema module
defmodule MyApp.Users.User do
  use Ecto.Schema
  import Ecto.Changeset

  schema "users" do
    field :name, :string
    field :email, :string
    timestamps()
  end

  # Changeset/validation lives with the schema
  def changeset(user, attrs) do
    user
    |> cast(attrs, [:name, :email])
    |> validate_required([:name, :email])
    |> validate_format(:email, ~r/@/)
    |> unique_constraint(:email)
  end

  # Serialization can use protocols or derive
  defimpl Jason.Encoder do
    def encode(user, opts) do
      user
      |> Map.take([:id, :name, :email])
      |> Jason.Encode.map(opts)
    end
  end
end

defmodule MyApp.Users.Query do
  import Ecto.Query
  alias MyApp.Users.User

  def base, do: from(u in User)

  def by_email(query \\ base(), email) do
    where(query, [u], u.email == ^email)
  end

  def active(query \\ base()) do
    where(query, [u], is_nil(u.deactivated_at))
  end
end

defmodule MyApp.Users.Policy do
  alias MyApp.Users.User

  def can?(%User{role: :admin}, :delete, %User{}), do: true
  def can?(%User{id: id}, :update, %User{id: id}), do: true
  def can?(_, _, _), do: false
end
```

## Alternative: Use Behaviors for Cross-Cutting Concerns

```elixir
defmodule MyApp.Serializable do
  @callback to_map(struct()) :: map()
end

defmodule MyApp.Users.User do
  @behaviour MyApp.Serializable

  # ... schema definition ...

  @impl true
  def to_map(user) do
    Map.take(user, [:id, :name, :email])
  end
end
```

## Treatment

- **Move Function**: Consolidate parallel modules into domain-focused modules.
- **Collapse Module Hierarchy**: Merge thin parallel modules.
- **Use Protocols**: Replace parallel serializers with protocol implementations.
- **Use Behaviors**: Define contracts that modules implement inline.
- **Organize by Domain**: Group by business concept, not technical layer.

## Why Refactor

- Adding new entities requires fewer files.
- Related code is co-located and easier to understand.
- Reduced boilerplate and ceremony.
- Domain concepts are self-contained.

## When Parallel Hierarchies Are Acceptable

- Framework requirements (e.g., Phoenix controllers/views).
- Clear separation needed for team organization.
- Parallel structures have genuinely different lifecycles.
- The hierarchy is shallow (2 levels, not 5).

## Elixir-Specific Patterns

| Instead of | Consider |
|------------|----------|
| `Serializers.User` | `Jason.Encoder` protocol impl |
| `Validators.User` | Changeset in schema module |
| `Queries.User` | `MyApp.Users.Query` in same context |
| `Policies.User` | `MyApp.Users.Policy` or inline |

## Related Smells

- `Shotgun Surgery`
- `Duplicate Code`
- `Speculative Generality`

## Related Refactoring Techniques

- `Move Function`
- `Collapse Module Hierarchy`
- `Extract Behavior`
- `Inline Module`
