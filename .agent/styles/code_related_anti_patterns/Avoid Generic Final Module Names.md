# Avoid Generic Final Module Names

For any modules whose last segment is a generic role name like "Supervisor", "Registry", "Manager", or "Policy", ensure that name is unique across the whole codebase. Rename the modules to be explicit by adding a descriptive suffix, even if it feels repetitive. Treat module names as if they all live in one flat namespace, even if the files are in different folders.

Example:

Don't do this:

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

```elixir
# BAD EXAMPLE, DO NOT DO THIS!
defmodule MyApp.Chat.Supervisor do
end

defmodule MyApp.Billing.Supervisor do
end
```

Do this:

```elixir
# GOOD EXAMPLE, DO THIS!
defmodule MyApp.Chat.ChatSupervisor do
end

defmodule MyApp.Billing.BillingSupervisor do
end
```
