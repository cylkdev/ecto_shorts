---
trigger: always_on
---

Use this rule when project-level constraints affect the current change.

## Protected Files

Do not modify any of these files without explicit user approval:

- `.windsurf/rules/project-guidelines.md`
- `.credo.exs`
- `mix.exs`

If you need to modify one of those files, ask for approval, then stop and wait for the response.

## Project Rules

- Child apps may depend on other child apps using `in_umbrella: true` in `mix.exs`. All apps share `config/` at the umbrella root.
- Use `Application.compile_env/3` for compile-time config.
- Do not use `Mix.env()` in runtime code. Mix is not available at runtime.
- Keep `Code.put_compiler_option(:warnings_as_errors, true)` as the first line of every `test_helper.exs`.
- Use `FactoryEx` (`{:factory_ex, "~> 0.3.4"}`) to generate test data. Add factories to `test/support/factory/`.
- Use `test/support/` within each app for test helpers, factories, and setup modules.
- Use a `DataCase` module for apps that need database access. Define it in `test/support/`. In umbrella apps, use the `_pg` child app, for example `MyAppPG.DataCase`.
- Use a `ConnCase` module for apps that need web access. Define it in `test/support/`. In umbrella apps, use the `_web` child app, for example `MyAppWeb.ConnCase`.
- Use `<AppName>.DataCase.setup_sandbox/1` for SQL sandbox setup in `ConnCase`.
- Do not use `Application.put_env/3` in tests. Pass the varying value into the code under test so each test controls its behaviour without affecting others.
