---
trigger: model_decision
description: Module is an RPC service wrapper (often in a *_service app) where every public function delegates via LearnElixirRPC.call_on_random_node/4. Service module contains no business logic, just thin forwarding to implementation modules with the same function name/arity.
---

# RPC Service Wrapper Pattern

## Purpose
Service apps (`*_service`) are thin RPC wrappers that provide distributed access to implementation modules via `LearnElixirRPC.call_on_random_node/4`.

## Pattern
```elixir
defmodule TeachingPlatformService do
  @service_name "teaching_platform_service"

  def my_function(arg1, arg2) do
    LearnElixirRPC.call_on_random_node(
      @service_name,
      TeachingPlatformService.MyImplementation,
      :my_function,
      [arg1, arg2]
    )
  end
end
```

## Rules
- `@service_name` must match a substring of the target node name
- Every public function delegates to exactly one implementation function
- Use the same function name and conceptual arity as the implementation
- Service modules have **no business logic** - only RPC delegation
- No OTP application module needed (no supervision tree)

## Testing
In test, `call_directly?: true` is configured so RPC calls execute locally:
```elixir
# config/config.exs
config :learn_elixir_rpc, call_directly?: Mix.env() !== :prod
```

Tests should verify both the delegation layer and the underlying database effects.

## Adding New Service Functions
1. Implement the function in the target app (e.g., `TeachingPlatformService.MyImplementation`)
2. Add the RPC delegation in the service app
3. Write tests that verify end-to-end behaviour
