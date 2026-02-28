---
trigger: always_on
---

Write code as if the repo’s style rules are part of the compiler.

Before you write or change any code, read every code style document that applies. Treat them as the source of truth, even if your default habits disagree.

While you work, apply the rules as you make each change. Do not “fix style later”. If you are unsure how a rule applies, look for an existing example in the codebase and copy the pattern.

When you finish, run the repo’s formatting and linting commands and adjust the code until it matches the rules with no exceptions. If you cannot run commands, review the code for rule compliance by comparing it to the style docs and nearby code, and then explain which rules you followed and where they are defined.

## Code Style Documents

Read the descriptions to decide which documents apply. Do not read documents that do not apply. 

- `.agent/code-styles/ABSINTHE.md`: Covers conventions specific to Absinthe GraphQL schemas. Use when writing or reviewing any Absinthe schema, mutation, query, or subscription code. Covers: snake_case `<resource>_<verb>` naming for mutations, noun-only naming for queries, schema module structure (import order, middleware placement), consistent verb usage, and input/payload type alignment.

- `.agent/code-styles/CODE_RELATED_ANTI_PATTERNS.md`: Covers Elixir language-level anti-patterns to avoid in any Elixir code. Use when writing or reviewing Elixir modules and functions. Covers: generic module name collisions, comment overuse, complex `with/else` blocks, mixed extraction in multi-clause functions, dynamic atom creation, long parameter lists, namespace trespassing, non-assertive map access (`map[:key]` vs `map.key`), non-assertive pattern matching, truthiness misuse (`&&` vs `and`), structs with 32+ fields, and `alias ... as:` as a naming workaround.

- `.agent/code-styles/DESIGN_RELATED_ANTI_PATTERNS.md`: Covers API and data model design anti-patterns. Use when designing module interfaces, data structures, and return types. Covers: alternative return types (one function, multiple shapes), boolean obsession (use atoms/enums instead), exceptions used for control flow, primitive obsession (use domain structs), unrelated multi-clause functions, and using `Application.fetch_env!` inside library logic.

- `.agent/code-styles/META_PROGRAMMING_ANTI_PATTERNS.md`: Covers macro and compile-time anti-patterns. Use when writing or reviewing code that uses `defmacro`, `use`, or dynamic module references. Covers: accidental compile-time dependencies from macro expansion, large code generation inside `quote`, unnecessary macros (use a function instead), `use` when `import/alias` suffices, and untracked compile-time dependencies from programmatically generated module names.

- `.agent/code-styles/NAMING_CONVENTIONS.md`: Covers Elixir naming conventions as a reference. Use when naming variables, functions, modules, files, and atoms. Covers: snake_case vs CamelCase rules, underscore prefix semantics (`_foo`, `__foo__`), trailing `!` for raising variants, trailing `?` for boolean functions, `is_` prefix for guard-safe predicates, and the semantic distinction between `size` (O(1)) and `length` (O(n)), plus `get`, `fetch`, and `fetch!` conventions.

- `.agent/code-styles/PROCESS_RELATED_ANTI_PATTERNS.md`: Covers OTP process anti-patterns. Use when writing or reviewing GenServers, Agents, Tasks, or any code that spawns processes. Covers: using processes for code organization instead of runtime need, scattered process interfaces (callers reaching directly into Agent/GenServer state), sending unnecessarily large messages across processes, and starting long-running processes outside a supervision tree.

- `.agent/code-styles/PUBLIC_API_AND_INTERFACES.md`: Covers public function API design. Use when defining any public or private function interface. Single rule: required arguments must be explicit positional parameters, not hidden inside maps or keyword lists. Optional arguments go in `opts`. The function signature should fully communicate the contract without reading the body.

- `.agent/code-styles/STRUCT_ANTI_PATTERNS.md`: Covers struct definition conventions. Use when defining any `defstruct`. Single rule: `defstruct` lists field names only (no default values inline), and a `new/1` constructor applies defaults from a `@default_options` module attribute or a `defp default_options/0` function when any default must be computed at runtime.

- `.agent/code-styles/TESTING.md`: Covers test and doctest style. Use when writing or reviewing tests and doctests. Covers: asserting on full returned values instead of individual fields, when per-field assertions are appropriate, and doctest formatting with `iex>` and `...>` prefixes showing the complete expected output.