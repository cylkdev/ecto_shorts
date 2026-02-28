---
trigger: always_on
---

## Requirements

NON-NEGOTIABLE REQUIREMENTS:
* Do not modify any of these files without explicit user approval: `.windsurf/rules/project-guidelines.md`, `.credo.exs`, `mix.exs`

## Umbrella Applications Guidelines

- Child apps can depend on other child apps using `in_umbrella: true` in `mix.exs`.
- All apps share `config/` at the umbrella root.

## Configuration Guidelines

- Use `Application.compile_env/3` for compile-time config.
- Do not use `Mix.env()` in runtime code. Mix is not available at runtime.

## Testing Guidelines

- All `test_helper.exs` files must include `Code.put_compiler_option(:warnings_as_errors, true)` as the first line.
- Use `FactoryEx` (`{:factory_ex, "~> 0.3.4"}`) for generating test data. Add factories to `test/support/factory/`.
- Use `test/support/` within each app for test helpers, factories, and setup modules.
- Use a `DataCase` module for apps needing database access (e.g. `<AppName>.DataCase`). Define it in `test/support/`. In umbrella apps, use the `_pg` child app (e.g. `MyAppPG.DataCase`).
- Use a `ConnCase` module for apps needing web access (e.g. `<AppName>.ConnCase`). Define it in `test/support/`. In umbrella apps, use the `_web` child app (e.g. `MyAppWeb.ConnCase`).
- Use `<AppName>.DataCase.setup_sandbox/1` for SQL sandbox setup in `ConnCase`.
- Do not use `Application.put_env/3` in tests. Pass the varying value into the code under test so each test controls its behavior without affecting others.