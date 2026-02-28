# Middle Man

## Category

Couplers

## Description

A module that does little more than delegate to another module. If most of a module's functions simply call functions on another module without adding value, the middle man should be removed or the delegation should be made explicit.

## Signs and Symptoms

- A module where most functions are one-liners that call another module.
- Functions that pass all parameters unchanged to another function.
- A wrapper module that adds no logic, validation, or transformation.
- Callers could easily call the underlying module directly.
- The module exists only for "organizational" purposes.

## Causes

- Over-application of encapsulation.
- Refactoring that moved logic elsewhere but left the shell.
- Misguided attempts to hide dependencies.
- Creating abstraction layers "just in case."
- Following patterns from other languages without adaptation.

## Example

```elixir
defmodule MyApp.UserService do
  alias MyApp.Users.Repository

  # Pure delegation - adds no value
  def get(id), do: Repository.get(id)

  def get_by_email(email), do: Repository.get_by_email(email)

  def create(attrs), do: Repository.create(attrs)

  def update(user, attrs), do: Repository.update(user, attrs)

  def delete(user), do: Repository.delete(user)

  def list_all, do: Repository.list_all()

  def list_active, do: Repository.list_active()

  # Only function with actual logic
  def deactivate(user) do
    update(user, %{active: false, deactivated_at: DateTime.utc_now()})
  end
end

# Callers use UserService when they could use Repository directly
defmodule MyApp.UserController do
  alias MyApp.UserService

  def show(conn, %{"id" => id}) do
    user = UserService.get(id)  # Why not Repository.get(id)?
    render(conn, "show.html", user: user)
  end
end
```

## Refactored

### Option 1: Remove the Middle Man

```elixir
# Delete UserService, use Repository directly
defmodule MyApp.UserController do
  alias MyApp.Users.Repository

  def show(conn, %{"id" => id}) do
    user = Repository.get(id)
    render(conn, "show.html", user: user)
  end
end

# Keep only the function with actual logic in a focused module
defmodule MyApp.Users.Deactivation do
  alias MyApp.Users.Repository

  def deactivate(user) do
    Repository.update(user, %{
      active: false,
      deactivated_at: DateTime.utc_now()
    })
  end
end
```

### Option 2: Add Value to Justify the Middle Man

```elixir
defmodule MyApp.UserService do
  alias MyApp.Users.Repository
  alias MyApp.Events

  # Now adds value: logging, events, caching
  def get(id) do
    case Repository.get(id) do
      nil ->
        Logger.debug("User not found: #{id}")
        nil

      user ->
        Events.emit(:user_accessed, %{user_id: id})
        user
    end
  end

  def create(attrs) do
    with {:ok, user} <- Repository.create(attrs) do
      Events.emit(:user_created, %{user_id: user.id})
      send_welcome_email(user)
      {:ok, user}
    end
  end

  def deactivate(user) do
    with {:ok, updated} <- Repository.update(user, %{
           active: false,
           deactivated_at: DateTime.utc_now()
         }) do
      Events.emit(:user_deactivated, %{user_id: user.id})
      send_deactivation_email(updated)
      {:ok, updated}
    end
  end

  defp send_welcome_email(user), do: # ...
  defp send_deactivation_email(user), do: # ...
end
```

### Option 3: Use defdelegate Explicitly

```elixir
defmodule MyApp.UserService do
  alias MyApp.Users.Repository

  # Explicit delegation - clear that these are pass-through
  defdelegate get(id), to: Repository
  defdelegate get_by_email(email), to: Repository
  defdelegate list_all(), to: Repository

  # Actual logic lives here
  def create(attrs) do
    with {:ok, user} <- Repository.create(attrs) do
      send_welcome_email(user)
      {:ok, user}
    end
  end

  def deactivate(user) do
    Repository.update(user, %{
      active: false,
      deactivated_at: DateTime.utc_now()
    })
  end
end
```

## Treatment

- **Remove Middle Man**: Let callers use the underlying module directly.
- **Inline Module**: Merge the middle man into its caller or delegate.
- **Add Value**: If the layer should exist, add logging, validation, events, or caching.
- **Use defdelegate**: Make pure delegation explicit and intentional.

## Why Refactor

- Reduces indirection and cognitive load.
- Eliminates unnecessary abstraction layers.
- Makes dependencies clearer.
- Reduces code to maintain.

## When Middle Man Is Justified

Keep a middle man when it:

- **Adds cross-cutting concerns**: Logging, metrics, caching, authorization.
- **Provides a stable API**: Shields callers from underlying changes.
- **Aggregates multiple modules**: Coordinates calls to several dependencies.
- **Enforces business rules**: Validates or transforms data.
- **Manages transactions**: Wraps operations in database transactions.

```elixir
# Justified: adds authorization and transactions
defmodule MyApp.UserService do
  alias MyApp.{Repo, Users.Repository, Authorization}

  def update(actor, user, attrs) do
    with :ok <- Authorization.can?(actor, :update, user) do
      Repo.transaction(fn ->
        user
        |> Repository.update(attrs)
        |> log_change(actor)
      end)
    end
  end
end
```

## Related Smells

- `Lazy Module`
- `Message Chains` (opposite problem)
- `Speculative Generality`

## Related Refactoring Techniques

- `Remove Middle Man`
- `Inline Module`
- `Move Function`
