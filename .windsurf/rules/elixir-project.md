---
trigger: always_on
---

## App Organization

This is an Elixir umbrella application. All apps live in `apps/`.

## Testing
- All apps needing database access in tests **must** use `LearnElixirPG.DataCase` - do **not** create app-local `DataCase` modules
- Web app's `ConnCase` uses `LearnElixirPG.DataCase.setup_sandbox/1` for SQL sandbox setup

## Dependencies
- Each app has its own `mix.exs` with `in_umbrella: true` dependencies
- All apps share `config/` at the umbrella root
- Use `test/support/` within each app for test helpers, factories, and setup modules

## Configuration
- Use `Application.compile_env/3` for compile-time config
- **Never** use `Mix.env()` at runtime in production - use compile-time conditionals
- **Never** use `Application.put_env/3` in tests - use dependency injection
- All apps share the same `config/config.exs`, `dev.exs`, `test.exs`, `prod.exs`, `runtime.exs`

## Adding New Apps
1. Create with `mix new apps/my_app --sup` from umbrella root
2. Update `mix.exs` to use umbrella paths (`build_path`, `config_path`, etc.)
3. Add `test_coverage: [tool: ExCoveralls]`
4. Add `elixirc_paths` for `:test` to include `test/support`
5. Create `AGENTS.md` in the app root
