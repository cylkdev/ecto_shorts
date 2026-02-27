---
trigger: model_decision
description: Functions return {:ok, _} / {:error, _} tuples (often ErrorMessage.*) and are composed with with. Code uses raise/try/rescue for expected control flow or mixes return shapes. Errors are logged/propagated via SharedUtils.Logger and consistent tuple contracts.
---

# Error Handling Patterns

## ErrorMessage Library
Use the `ErrorMessage` library (from `error_message` hex package) for all structured errors. Do **not** alias it as `MyApp.ErrorMessage` - use it directly.

## Return Types
Context functions must return:
- `{:ok, result}` for success
- `{:error, ErrorMessage.t()}` for errors
- `:ok` for void success operations

## Common Error Constructors
```elixir
ErrorMessage.not_found("resource not found", %{id: id})
ErrorMessage.bad_request("invalid input", %{field: "email"})
ErrorMessage.unauthorized("not authenticated")
ErrorMessage.forbidden("not allowed")
ErrorMessage.service_unavailable("service down", %{service: name})
ErrorMessage.bad_gateway("provider error", %{status: 502})
ErrorMessage.request_timeout("operation timed out")
ErrorMessage.internal_server_error("unexpected error", %{error: reason})
```

## Error Chaining with `with`
```elixir
def process(params) do
  with {:ok, validated} <- validate(params),
       {:ok, result} <- execute(validated) do
    {:ok, result}
  end
end
```

The `with` block automatically propagates `{:error, _}` tuples - do not add redundant `else` clauses unless transforming errors.

## Logging Errors
Use `SharedUtils.Logger` which natively formats `ErrorMessage` structs:
```elixir
SharedUtils.Logger.error("MyModule", error_message)
```

## Bug Fixing
- Always fix the root cause, not symptoms
- Never apply downstream patches for upstream issues
- Never mix atom and string keys to work around data shape issues
