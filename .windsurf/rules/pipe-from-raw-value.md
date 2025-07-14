---
trigger: always_on
description: Apply whenever forming a |> pipeline: start it with a raw value, struct, or literal, not a function call, so the data flow is evident (value |> func1 |> func2), avoiding hidden arguments like func(value) |> next.
---

Rule: Pipe from a Raw Value

---

Avoid starting a pipe chain with a function call. Always begin a pipe chain with a raw value or literal. This improves readability and intent by clearly showing what data is being transformed.

Valid

```elixir

a |> b |> c

```

Invalid

```elixir

b(a) |> c

```