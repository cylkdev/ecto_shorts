# Complex extractions in clauses

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Problem

When we use multi-clause functions, it is possible to extract values in the clauses for further usage and for pattern matching/guard checking. This extraction itself does not represent an anti-pattern, but when you have extractions made across several clauses and several arguments of the same function, it becomes hard to know which extracted parts are used for pattern/guards and what is used only inside the function body. This anti-pattern is related to Unrelated multi-clause function, but with implications of its own. It impairs the code readability in a different way.

### Example

The multi-clause function `drive/1` is extracting fields of an `%User{}` struct for usage in the clause expression (age) and for usage in the function body (name):

```elixir
def drive(%User{name: name, age: age}) when age >= 18 do
  "#{name} can drive"
end

def drive(%User{name: name, age: age}) when age < 18 do
  "#{name} cannot drive"
end
```

While the example above is small and does not constitute an anti-pattern, it is an example of mixed extraction and pattern matching. A situation where drive/1 was more complex, having many more clauses, arguments, and extractions, would make it hard to know at a glance which variables are used for pattern/guards and which ones are not.

### Solution

As shown below, a possible solution to this anti-pattern is to extract only pattern/guard related variables in the signature once you have many arguments or multiple clauses:

```elixir  
def drive(%User{age: age} = user) when age >= 18 do
  %User{name: name} = user
  "#{name} can drive"
end

def drive(%User{age: age} = user) when age < 18 do
  %User{name: name} = user
  "#{name} cannot drive"
end
```
