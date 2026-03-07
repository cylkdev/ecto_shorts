---
trigger: always_on
---

Use this rule after you change Elixir code, tests, or project configuration.

Do not improvise the verification path. Use `.windsurf/skills/run-checks/SKILL.md` unless the user explicitly asked for only one specific check.

Use this routing:

- If the user explicitly asked for only Credo, use `.windsurf/skills/run-credo/SKILL.md`.
- If the user explicitly asked for only Dialyzer, use `.windsurf/skills/run-dialyzer/SKILL.md`.
- If the user explicitly asked for only tests, use `.windsurf/skills/run-tests/SKILL.md`.
- Otherwise, use `.windsurf/skills/run-checks/SKILL.md`.

Treat a change as `rename-only` only when it renamed things without changing implementation logic, control flow, function inputs or outputs, return shapes, types, specs, callbacks, runtime behaviour, query behaviour, changeset behaviour, or test meaning.

Treat any change to function inputs or outputs, return shapes, types, specs, callbacks, structs, behaviours, queries, changesets, config-driven runtime behaviour, or user-observable results as a broader change. Use the full verification path.

Do not use this rule for documentation-only changes or other changes that did not affect Elixir code, tests, or project configuration.
