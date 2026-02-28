---
name: write-changelog
description: Compares old and new code versions and writes a concise changelog entry describing only user-facing changes to the public interface — new features, removed or renamed functions/parameters, behavior changes, and breaking changes — ignoring internal refactors or implementation details. Use this skill after completing a set of changes that affect the public API.
---

## When to use

Use this skill after you complete a set of changes that affect what a consumer of the library sees: new public functions, removed or renamed functions or parameters, changed return values, new configuration options, or breaking changes.

## When not to use

Do **not** use this skill for purely internal refactors, code style fixes, or implementation changes that leave the public interface identical.

## What to do

### 1. Identify the old and new code

Determine the comparison baseline. Use one of:

- The current `main` or `master` branch versus the working tree.
- A specific tag or commit the user provides.
- The state before and after the current editing session if no VCS reference is given.

### 2. Diff the public interface

Compare only the **public surface area** between old and new:

- Exported functions and macros (those with `@doc` or without `@doc false`).
- Public typespecs (`@type`, `@opaque`).
- Behaviour callbacks (`@callback`).
- Module-level configuration options documented in `@moduledoc`.
- Mix task commands and their flags.

Ignore changes that are purely internal: private functions, internal module splits, variable renames, comment edits, test changes, and refactors that preserve the same inputs/outputs.

### 3. Classify each change

Sort every public-interface difference into exactly one category:

- **Added** — new function, parameter, option, type, or behaviour callback.
- **Changed** — altered return value, parameter meaning, default value, or observable behaviour of an existing function.
- **Deprecated** — function or option still works but is scheduled for removal.
- **Removed** — function, parameter, option, or type no longer exists.
- **Fixed** — bug fix that changes user-observable output to match documented intent.
- **Breaking** — any change that can cause existing consumer code to fail at compile time or runtime. Always call this out explicitly, even if it also fits another category.

### 4. Write the changelog entry

Open `CHANGELOG.md` at the project root. Add a new version section at the top, below the `## Changelog` heading, following the existing format:

```
#### VNEXT
- <category>: <concise description>
```

Rules for each line:

- One line per change.
- Start with a lowercase category prefix only for **breaking** and **fix** entries (e.g. `breaking:`, `fix:`). Other entries need no prefix.
- Describe the change from the caller's perspective, not the implementer's.
- Mention the function or module name so consumers can search for it.
- Keep each line to one sentence.

### 5. Omit noise

Do **not** include:

- Internal module renames or splits.
- Private function changes.
- Test-only changes.
- Documentation-only changes (unless a public option or parameter was added/removed in the docs).
- Dependency bumps that do not change the public API.

### 6. Review

Re-read the entry. Verify every line maps to a real public-interface difference found in step 2. Remove any line that describes an implementation detail rather than a user-facing change.