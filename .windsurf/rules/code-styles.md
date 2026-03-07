---
trigger: always_on
---

Use this rule before you write or revise code.

Read only the style documents that apply to the current change. Do not read unrelated style documents by default.

Use `.agent/styles/AGENTS.md` to identify the correct style area. Then read the specific rule files that own the current unresolved question. Treat those files as the source of truth.

Keep the code aligned with the selected rules as you make each change. Do not defer style corrections until later.

If the rule application is unclear, inspect the current codebase for an existing example and follow that pattern.

## Style Areas

- `.agent/styles/code_related_anti_patterns/`: Use when writing or reviewing general Elixir modules and functions.
- `.agent/styles/design_related_anti_patterns/`: Use when designing module interfaces, data structures, and return shapes.
- `.agent/styles/documentation/`: Use when writing module docs, function docs, or doctests.
- `.agent/styles/ecto/`: Use when writing or reviewing Ecto queries, schemas, or changesets.
- `.agent/styles/meta_programming_anti_patterns/`: Use when writing or reviewing macros, `use`, or compile-time code.
- `.agent/styles/naming_conventions/`: Use when naming modules, functions, variables, files, or atoms.
- `.agent/styles/process_related_anti_patterns/`: Use when writing or reviewing GenServers, Agents, Tasks, or other process code.
- `.agent/styles/public_api_and_interfaces/`: Use when defining public or private function interfaces.
- `.agent/styles/struct_anti_patterns/`: Use when defining or revising `defstruct`.
- `.agent/styles/testing/`: Use when writing or reviewing tests and doctests.
