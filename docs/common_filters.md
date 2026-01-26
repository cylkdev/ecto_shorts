## How it works

The routing layer handles:

- top‑level `:where, :or_where` container maps (tests with where: %{...} / or_where: %{...})
- top‑level `:as, :at` containers (tests for binding selectors)
- pagination keys `:first, :last, :limit, :offset, :order_by, :preload`
- repeated/keyword list expansion

The builder boundary is reached when the shape is:

- a concrete field comparison (implicit or explicit op)
- a boolean operator list
- a query‑builder op (:select/:join/:select_merge)

The max nesting depth of the parameters is effectively the point where the value is no longer a map/keyword list of further routing keys, but a terminal operator/value or a query‑builder op. Routing keeps peeling layers only while the value is still a container (map/keyword list) that holds more routing keys (:where/:or_where, :as/:at, boolean ops, field names). Once the value stops being that kind of container and becomes a terminal expression (operator/value pair or a query‑builder op), routing stops and the builder is invoked.

So “max depth” = the deepest point in the nested params where you can still make routing decisions. Past that point, you just pass the terminal expression to the builder.