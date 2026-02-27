---
trigger: model_decision
description: Umbrella contains a paired *_service app and code must call the service boundary (TeachingPlatformService/PaymentsService) instead of calling implementation apps directly. Direct calls bypass LearnElixirRPC routing and break distributed deployments. Look for cross-app calls that skip the *_service wrapper.
---

# Service App Pattern

Apps that have a corresponding `_service` app must **never** be accessed directly by other apps in the cluster. Always go through the `_service` wrapper.

## Current Service Apps

| Service App | Wraps |
|-------------|-------|
| `TeachingPlatformService` | `LearnElixirPG`, `TeachingPlatformAuth`, and various `TeachingPlatformService.*` implementation modules |
| `PaymentsService` | Stripe payments via `stripity_stripe` |

## Rules

- The `TeachingPlatformService` module is the main RPC entry point - web apps should call it rather than directly accessing `LearnElixirPG` contexts
- The only apps that may reference implementation modules directly are:
  1. The implementation app itself
  2. The corresponding `_service` app (e.g., `teaching_platform_service` calling `LearnElixirPG.Accounts`)
- If a new domain app needs distributed access, create a `_service` app following this pattern

## Why

Service apps use `LearnElixirRPC.call_on_random_node/4` to route calls to the correct node in the cluster. Calling the implementation directly bypasses node routing and breaks in distributed deployments.

## Testing Stripe Interactions

Do **not** mock Stripe at the module level (e.g., swapping `@billing_module` at compile time). Instead:

- Use [stripe-mock](https://github.com/stripe/stripe-mock) - a local HTTP server that implements the Stripe API
- Point `stripity_stripe` at the local stripe-mock server in test config
- This gives realistic end-to-end testing of Stripe interactions without hitting the real API
