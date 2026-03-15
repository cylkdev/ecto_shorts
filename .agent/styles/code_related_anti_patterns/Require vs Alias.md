# Require vs Alias

In Elixir, `alias` and `require` solve different problems, but they are often used together for the same module.

Use `alias` when you want a shorter name for a module. An alias only changes how you write the module name in this file. It does not load code and it does not enable macros.

Use `require` when you want to call macros from a module. A macro is code that runs at compile time. Elixir requires you to `require` the module before you can use its macros (unless you `import` them).

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Guidelines

- Put all `alias` lines together in one block.

- Put all `require` lines together in one block.

- Always place the `require` block after the `alias` block.

- If you both `alias` and `require` the same module, write the `alias` first, then the `require`.

- This makes the file easy to scan. First you see the names the file will use (`alias`). Then you see which modules provide macros (`require`).

## Examples

Don't do this:

```elixir
# BAD EXAMPLE, DO NOT COPY!
require EctoShorts.DynamicBuilders.Postgres.ExprHelpers
alias EctoShorts.DynamicBuilders.Postgres.ExprHelpers
```

Do this:

```elixir
# GOOD EXAMPLE, COPY THIS!
alias EctoShorts.DynamicBuilders.Postgres.ExprHelpers

require EctoShorts.DynamicBuilders.Postgres.ExprHelpers
```

Don't do this:

```elixir
# BAD EXAMPLE, DO NOT COPY!
alias EctoShorts.Generator.AST
alias EctoShorts.Generator.Blueprint
require EctoShorts.DynamicBuilders.Postgres.ExprHelpers
alias EctoShorts.DynamicBuilders.Postgres.ExprHelpers
```

Do this:

```elixir
# GOOD EXAMPLE, COPY THIS!
alias EctoShorts.Generator.AST
alias EctoShorts.Generator.Blueprint
alias EctoShorts.DynamicBuilders.Postgres.ExprHelpers

require EctoShorts.DynamicBuilders.Postgres.ExprHelpers
```

## Why do this

It prevents "hunt the require" when you see a macro call and want to confirm the module was required.

It avoids confusing files where `alias` and `require` are interleaved.

It gives you one consistent place to add or remove module references as the file changes.
