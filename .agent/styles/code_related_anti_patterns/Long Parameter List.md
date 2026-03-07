# Long parameter list

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Problem

In a functional language like Elixir, functions tend to explicitly receive all inputs and return all relevant outputs, instead of relying on mutations or side-effects. As functions grow in complexity, the amount of arguments (parameters) they need to work with may grow, to a point where the function's interface becomes confusing and prone to errors during use.

#### Example

In the following example, the `loan/6` function takes too many arguments, causing its interface to be confusing and potentially leading developers to introduce errors during calls to this function.

```elixir
defmodule Library do
  # Too many parameters that can be grouped!
  def loan(user_name, email, password, user_alias, book_title, book_ed) do
    ...
  end
end
```

#### Solution

To address this anti-pattern, related arguments can be grouped using key-value data structures, such as maps, structs, or even keyword lists in the case of optional arguments. This effectively reduces the number of arguments and the key-value data structures adds clarity to the caller.

For this particular example, the arguments to `loan/6` can be grouped into two different maps, thereby reducing its arity to `loan/2`:

```elixir
defmodule Library do
  def loan(%{name: name, email: email, password: password, alias: alias} = user, %{title: title, ed: ed} = book) do
    ...
  end
end
```

In some cases, the function with too many arguments may be a private function, which gives us more flexibility over how to separate the function arguments. One possible suggestion for such scenarios is to split the arguments in two maps (or tuples): one map keeps the data that may change, and the other keeps the data that won't change (read-only). This gives us a mechanical option to refactor the code.

Other times, a function may legitimately take half a dozen or more completely unrelated arguments. This may suggest the function is trying to do too much and would be better broken into multiple functions, each responsible for a smaller piece of the overall responsibility.
