# Duplicate Code

## Category

Dispensables

## Description

Identical or very similar code exists in multiple places. When the same logic appears more than once, changes must be made in multiple locations, increasing the risk of inconsistencies and bugs.

## Signs and Symptoms

- Copy-pasted code blocks across functions or modules.
- Similar functions that differ only in small details.
- The same pattern of operations repeated with different data.
- Bug fixes that must be applied in multiple places.
- "I've seen this code somewhere before" feeling when reading.

## Causes

- Copy-paste programming under time pressure.
- Lack of awareness of existing functionality.
- Fear of modifying shared code.
- Insufficient abstraction of common patterns.
- Different developers implementing similar features independently.

## Example

```elixir
defmodule UserController do
  def create(conn, params) do
    case Repo.insert(User.changeset(%User{}, params)) do
      {:ok, user} ->
        conn
        |> put_status(:created)
        |> put_resp_header("location", "/users/#{user.id}")
        |> json(%{data: %{id: user.id, name: user.name, email: user.email}})

      {:error, changeset} ->
        errors = Enum.map(changeset.errors, fn {field, {msg, _}} ->
          %{field: field, message: msg}
        end)

        conn
        |> put_status(:unprocessable_entity)
        |> json(%{errors: errors})
    end
  end
end

defmodule PostController do
  def create(conn, params) do
    case Repo.insert(Post.changeset(%Post{}, params)) do
      {:ok, post} ->
        conn
        |> put_status(:created)
        |> put_resp_header("location", "/posts/#{post.id}")
        |> json(%{data: %{id: post.id, title: post.title, body: post.body}})

      {:error, changeset} ->
        # Exact same error handling duplicated
        errors = Enum.map(changeset.errors, fn {field, {msg, _}} ->
          %{field: field, message: msg}
        end)

        conn
        |> put_status(:unprocessable_entity)
        |> json(%{errors: errors})
    end
  end
end

defmodule CommentController do
  def create(conn, params) do
    case Repo.insert(Comment.changeset(%Comment{}, params)) do
      {:ok, comment} ->
        conn
        |> put_status(:created)
        |> put_resp_header("location", "/comments/#{comment.id}")
        |> json(%{data: %{id: comment.id, body: comment.body}})

      {:error, changeset} ->
        # Same error handling again
        errors = Enum.map(changeset.errors, fn {field, {msg, _}} ->
          %{field: field, message: msg}
        end)

        conn
        |> put_status(:unprocessable_entity)
        |> json(%{errors: errors})
    end
  end
end
```

## Refactored

```elixir
defmodule MyAppWeb.ControllerHelpers do
  import Plug.Conn
  import Phoenix.Controller

  def render_created(conn, resource, location, data) do
    conn
    |> put_status(:created)
    |> put_resp_header("location", location)
    |> json(%{data: data})
  end

  def render_changeset_errors(conn, changeset) do
    errors = format_errors(changeset)

    conn
    |> put_status(:unprocessable_entity)
    |> json(%{errors: errors})
  end

  defp format_errors(changeset) do
    Enum.map(changeset.errors, fn {field, {msg, _}} ->
      %{field: field, message: msg}
    end)
  end
end

defmodule UserController do
  import MyAppWeb.ControllerHelpers

  def create(conn, params) do
    case Repo.insert(User.changeset(%User{}, params)) do
      {:ok, user} ->
        render_created(conn, user, "/users/#{user.id}", serialize_user(user))

      {:error, changeset} ->
        render_changeset_errors(conn, changeset)
    end
  end

  defp serialize_user(user) do
    %{id: user.id, name: user.name, email: user.email}
  end
end

defmodule PostController do
  import MyAppWeb.ControllerHelpers

  def create(conn, params) do
    case Repo.insert(Post.changeset(%Post{}, params)) do
      {:ok, post} ->
        render_created(conn, post, "/posts/#{post.id}", serialize_post(post))

      {:error, changeset} ->
        render_changeset_errors(conn, changeset)
    end
  end

  defp serialize_post(post) do
    %{id: post.id, title: post.title, body: post.body}
  end
end
```

## Types of Duplication

### 1. Exact Duplication
Identical code copied verbatim. Extract to a shared function.

### 2. Structural Duplication
Same structure with different values. Use parameterization.

```elixir
# Before: structural duplication
def fetch_user(id), do: Repo.get(User, id)
def fetch_post(id), do: Repo.get(Post, id)
def fetch_comment(id), do: Repo.get(Comment, id)

# After: parameterized
def fetch(schema, id), do: Repo.get(schema, id)
```

### 3. Algorithmic Duplication
Same algorithm with different types. Use protocols or behaviours.

```elixir
# Before: same algorithm, different types
def serialize_user(user), do: Map.take(user, [:id, :name])
def serialize_post(post), do: Map.take(post, [:id, :title])

# After: protocol
defprotocol Serializable do
  def serialize(data)
end

defimpl Serializable, for: User do
  def serialize(user), do: Map.take(user, [:id, :name])
end
```

## Treatment

- **Extract Function**: Move duplicated code to a shared function.
- **Extract Module**: Create a module for shared functionality.
- **Pull Up Function**: Move common code to a shared module.
- **Form Template Function**: Extract the common algorithm, parameterize differences.
- **Use Protocols**: For type-specific duplication with common interface.

## Why Refactor

- Single point of change for shared logic.
- Bug fixes apply everywhere automatically.
- Reduced code size and cognitive load.
- Easier testing of shared functionality.
- Consistent behaviour across the codebase.

## Related Smells

- `Long Function`
- `Shotgun Surgery`
- `Large Module`

## Related Refactoring Techniques

- `Extract Function`
- `Extract Module`
- `Pull Up Function`
- `Form Template Function`
- `Parameterize Function`
