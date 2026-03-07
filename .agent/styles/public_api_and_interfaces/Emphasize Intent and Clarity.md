# Emphasize Intent and Clarity

Prefer explicit function arguments over pulling required values from maps. Function signatures should clearly communicate what inputs are required.

Don't put required arguments into open-ended containers like maps or keyword lists.

When you put required arguments into open-ended containers and then pull values out inside the function, you're hiding what the function actually needs. This is especially bad in Elixir because the language and its tools treat the function signature as the contract.

In Elixir, you can ask the console for help on a function and immediately see its arguments. If the function is defined as `create_user(id, email, opts \\ [])`, the console shows you exactly what it needs. You can understand how to call it without opening the source code.

But if it's defined as `create_user(params, opts \\ [])`, the console only shows `params`.
It doesn't tell you that `params` must contain `:id` and `:email`. You have to read the implementation to figure that out. That makes the function harder to discover and understand.

The same problem shows up in your editor. Elixir tooling and IDEs use the function signature to provide argument hints and autocomplete. If the required values are explicit arguments, your editor can guide you. If everything is bundled into a map, the editor has no idea what keys are required. You've taken something the tools could understand and turned it into a runtime convention.

You also lose one of the small but important guarantees Elixir gives you: arity checking.
If a function requires two arguments, the compiler enforces that. If you hide required data inside a single map argument, the compiler can't help you. Missing keys become runtime
errors instead of clearer call-site mistakes.

Elixir is built around explicit function heads and pattern matching. The function head is meant to describe what the function needs. When required values are pulled out of a map inside the body, the real contract is hidden. In Elixir, that goes against how the language is designed to be used.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

### Bad Example (DO NOT COPY) - Hiding required values inside a map

```elixir
# What does this function actually require?
def create_user(params, opts) do
  id    = params[:id]
  email = params[:email]
  role  = opts[:role]

  # ...
end
```

This example doesn't tell you what's required.

You must read the body to discover it needs:

  - `:id`
  - `:email`
  - `:role`

If any of those keys are missing, it may fail at runtime.
The API is ambiguous: are all keys optional? Which ones matter?
The signature communicates nothing about required inputs.

### Good Example - Explicit required arguments

```elixir
def create_user(id, email, opts) do
  role = Keyword.get(opts, :role, :user)

  # ...
end
```

This example clearly communicates:

- `id` is required
- `email` is required
- `opts` contains optional configuration

You can understand usage without reading the implementation.

The contract is obvious at the call site:

```elixir
create_user(user_id, user_email, role: :admin)
```

The required inputs are explicit and visible.

### Rule of thumb

- If it's required then make it a positional argument.
- If it's optional then put it in opts.

The function signature should tell the story without reading the body.
That's what "prefer explicit function arguments" really means in practice.
