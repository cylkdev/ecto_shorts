# Namespace trespassing

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Problem

This anti-pattern manifests when a package author or a library defines modules outside of its "namespace". A library should use its name as a "prefix" for all of its modules. For example, a package named `:my_lib` should define all of its modules within the `MyLib` namespace, such as `MyLib.User`, `MyLib.SubModule`, `MyLib.Application`, and `MyLib` itself.

This is important because the Erlang VM can only load one instance of a module at a time. So if there are multiple libraries that define the same module, then they are incompatible with each other due to this limitation. By always using the library name as a prefix, it avoids module name clashes due to the unique prefix.

### Example

This problem commonly manifests when writing an extension of another library. For example, imagine you are writing a package that adds authentication to Plug called `:plug_auth`. You must avoid defining modules within the `Plug` namespace:

```elixir
defmodule Plug.Auth do
  # ...
end
```

Even if `Plug` does not currently define a `Plug.Auth` module, it may add such a module in the future, which would ultimately conflict with `plug_auth`'s definition.

### Solution

Given the package is named `:plug_auth`, it must define modules inside the `PlugAuth` namespace:

```elixir
defmodule PlugAuth do
  # ...
end
```

There are few known exceptions to this anti-pattern:

Protocol implementations are, by design, defined under the `protocol` namespace

In some scenarios, the namespace owner may allow exceptions to this rule. For example, in Elixir itself, you defined custom Mix tasks by placing them under the Mix.Tasks namespace, such as Mix.Tasks.PlugAuth

If you are the maintainer for both `plug` and `plug_auth`, then you may allow `plug_auth` to define modules with the `Plug` namespace, such as `Plug.Auth`. However, you are responsible for avoiding or managing any conflicts that may arise in the future.
