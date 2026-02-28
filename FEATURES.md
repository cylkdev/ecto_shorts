- Add support for selecting the first binding with `:first`
- Add support for selecting the last binding with `:last`
- The following behaviour must be true:
    1. When a join is applied it applies it to the binding specified by the selector or it defaults to the first binding.
    2. After a join is applied the filters are applied to the same binding that the join was applied to.

- What is common filters? whats the purpose/backstory? what problem is it solving?
How does the language work? What's the structure? is there a pattern?