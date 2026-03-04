# Sending Unnecessary Data

**Problem**

Message passing copies data between processes. Large messages can be CPU- and memory-expensive. This includes:

- explicit messages (`send/2`, `GenServer.call/3`, `GenServer.cast/2`, `GenServer.start_link/3` init args)
- implicit messages via closures (`spawn/1`, `Task.async/1`, `Task.async_stream/3`) where captured variables are copied into the new process

This commonly bites when you "just pass the whole struct" (for example, a `%Plug.Conn{}`) when you only need one or two fields.

**Example**

Capturing a large variable inside a spawned function:

```elixir
spawn(fn -> report_ip(conn.remote_ip) end)
```

Even though you only use `conn.remote_ip`, the closure still captures `conn` and the whole term gets copied.

**Refactoring**

Extract the minimal data first, then spawn with only that value:

```elixir
ip_address = conn.remote_ip
spawn(fn -> report_ip(ip_address) end)
```

Other options when appropriate:

- make the receiving process fetch the data it needs (instead of being sent a huge payload)
- for infrequently changing shared data, consider mechanisms designed for sharing (for example, `:persistent_term`) instead of repeatedly copying large terms
