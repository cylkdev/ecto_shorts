---
trigger: model_decision
description: Module is a Phoenix LiveView/component (use ... :live_view, mount/3, handle_event/3, handle_info/2). Uses socket.assigns/assign, connected?/1, streams (stream/3, stream_insert/3), push_event/3, and HEEx features like phx-hook and ~p routes.
---

# LiveView Patterns

## Module Setup
```elixir
defmodule TeachingPlatformWeb.MyLive do
  use TeachingPlatformWeb, :live_view
end
```

## PubSub Subscriptions
Only subscribe when the socket is connected:
```elixir
def mount(_params, _session, socket) do
  if connected?(socket) do
    TeachingPlatformPubSub.subscribe_to_topic(socket.assigns.id)
  end

  {:ok, socket}
end
```

Always unsubscribe when switching contexts or on unmount.

## Handling PubSub Messages
```elixir
def handle_info(%TeachingPlatformPubSub.Message{event: event, payload: payload}, socket) do
  {:noreply, socket}
end
```

## Streams vs Assigns
- Use `stream/3` and `stream_insert/3` for large lists to avoid keeping all items in memory
- Use `temporary_assigns` for one-time render data with no possible updates

## Hooks
Phoenix hooks require an `id` attribute on the DOM element:
```heex
<div id="my-hook" phx-hook="MyHook">
```

## JS Commands
Use `push_event/3` to communicate with JavaScript hooks:
```elixir
{:noreply, push_event(socket, "event_name", %{data: value})}
```

## Verified Routes
Use `~p` sigil for type-safe routes:
```elixir
~p"/users/#{user.id}"
```
