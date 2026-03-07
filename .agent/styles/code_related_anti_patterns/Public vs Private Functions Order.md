# Public vs Private Functions Order

Put your public functions first, and your private helper functions after them.

A public function is one other modules can call. In Elixir it starts with `def`.
A private function is only for code inside the same module. In Elixir it starts with `defp`.

This order matters because it helps a reader understand the module quickly.

When someone opens a module, they usually want to know:

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

1. What can I call from the outside?
2. How does it work on the inside?

## Rule

1. Write all `def` functions before any `defp` functions.
2. If a private helper is shared by a related set of public functions, put the helper right after that set (and still below the public functions it supports). Otherwise, keep all private helpers (`defp`) together at the bottom of the module.

## Why we do this

- The top of the file reads like a list of what the module offers.
- A beginner can scan the public functions first without getting lost in helpers.
- Helpers are easy to find because they are all in one place (the bottom), unless they are clearly attached to one group in a large module.

### Examples

Do NOT place private helpers at the top of the module:

    # BAD EXAMPLE, DO NOT COPY!
    defmodule MyModule do
      defp helper do
        ...
      end

      def some_work(arg) do
        arg + helper()
      end
    end

Place all public functions first, then all private helpers at the bottom:

    # GOOD EXAMPLE
    defmodule MyModule do
      def some_work(arg) do
        arg + helper()
      end

      defp helper do
        ...
      end
    end

In a large module, you should place a shared helper after the group that uses it:

    # GOOD EXAMPLE
    defmodule MyModule do
      def some_func_a(arg) do
        arg + shared()
      end

      def some_func_b(arg) do
        arg + shared()
      end

      defp shared do
        ...
      end

      def another_func do
        ...
      end
    end

## Quick checklist

- The first functions I see are public (`def`).
- I do not see any `defp` before the public functions it supports.
- Private helpers are grouped at the bottom, unless they are shared by a clear group in a large module.
