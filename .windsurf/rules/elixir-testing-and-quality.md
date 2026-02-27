---
trigger: always_on
description: Use after writing or modifying code to ensure no breaking changes were introduced.
---

## When to use

Use after writing or modifying code to ensure no breaking changes were introduced.

## What to do

### 1. Write Tests

- Write comprehensive tests for all new functionality before moving to the next section

- Tests should cover happy paths, edge cases, and error conditions

- Use `FactoryEx` for test data generation

- Run tests from within the specific app directory, never from the umbrella root

- All `test_helper.exs` files must include `Code.put_compiler_option(:warnings_as_errors, true)` as the first line.

- All apps that need database access in tests must use `LearnElixirPG.DataCase` - do not create app-local `DataCase` modules

### 2. Run Tests

After writing tests, execute them:

```bash
cd apps/<app_name> && mix test
```

If tests fail, fix the issues before proceeding.

### 3. Run Credo

Check code style and consistency:

```bash
mix credo --strict
```

Address any warnings or errors before proceeding.

### 4. Run Dialyzer

Perform static analysis:

```bash
mix dialyzer
```

Fix all errors or warnings emitted from this app before proceeding.
