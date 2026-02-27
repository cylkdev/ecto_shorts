---
trigger: model_decision
description: Code makes outbound HTTP requests (SharedUtils.HTTP.* wrappers or direct Req/Finch/HTTPoison/Tesla usage). Needs an app-specific HTTP wrapper module with child_spec and supervision entry. Tests stub HTTP via HTTPSandbox-style response hooks.
---

# HTTP Client Pattern

## Rule: Never Use Raw HTTP Libraries
**Never** use `:req`, `:httpoison`, `:tesla`, `:httpc`, or `Finch` directly. Always go through `SharedUtils.HTTP`.

## Creating App-Specific HTTP Clients
Every app needing HTTP must create its own wrapper module:

```elixir
defmodule MyApp.HTTP do
  @app_name :my_app_http

  @default_opts [
    name: @app_name,
    atomize_keys?: false,
    pools: [default: [size: 10, count: 5]]
  ]

  def child_spec(opts \\ []), do: SharedUtils.HTTP.child_spec({@app_name, opts})

  def get(url, headers \\ [], opts \\ []) do
    SharedUtils.HTTP.get(url, headers, Keyword.merge(@default_opts, opts))
  end

  def post(url, body, headers \\ [], opts \\ []) do
    SharedUtils.HTTP.post(url, body, headers, Keyword.merge(@default_opts, opts))
  end

  def put(url, body, headers \\ [], opts \\ []) do
    SharedUtils.HTTP.put(url, body, headers, Keyword.merge(@default_opts, opts))
  end

  def delete(url, headers \\ [], opts \\ []) do
    SharedUtils.HTTP.delete(url, headers, Keyword.merge(@default_opts, opts))
  end
end
```

## Supervision Tree
Add the HTTP module as a child:
```elixir
children = [MyApp.HTTP]
```

## Test Isolation
Use `SharedUtils.Support.HTTPSandbox` for mock responses:
```elixir
SharedUtils.Support.HTTPSandbox.set_get_responses([
  {~r/api\.example\.com/, fn _url, _headers, _opts ->
    {:ok, %SharedUtils.HTTP.Response{status: 200, body: Jason.encode!(%{ok: true})}}
  end}
])
```

## Response Pattern
```elixir
case MyApp.HTTP.post(url, payload, headers) do
  {:ok, {body, %SharedUtils.HTTP.Response{status: status}}} when status in 200..299 ->
    {:ok, body}
  {:ok, {body, %SharedUtils.HTTP.Response{status: status}}} ->
    {:error, ErrorMessage.bad_gateway("API error", %{status: status, body: body})}
  {:error, %ErrorMessage{} = error} ->
    {:error, error}
end
```
