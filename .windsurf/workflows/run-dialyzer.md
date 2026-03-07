---
description: Runs Dialyzer for the relevant Elixir Mix project, fixes static analysis warnings, and repeats until the command passes cleanly.
auto_execution_mode: 3
---

// turbo-all

## When to use

Use this workflow after changing Elixir code, types, specs, behaviours, or function return shapes and you need to verify static analysis with Dialyzer.

## When not to use

Do **not** use this workflow to run Credo or tests.

## Definitions

- A `Mix project` is any directory that contains `mix.exs`.
- An `umbrella project` is a Mix project whose child applications live under `apps/`.
- `Dialyzer` is the static analysis tool that checks whether the compiled code and type information agree.

## What to do

1. Choose the correct directory before you run anything.
   - If the change is limited to one child application inside an umbrella project, run Dialyzer from that child application's directory.
   - If the change touches the umbrella root, shared configuration, or more than one child application, run Dialyzer from the umbrella root.
   - If this is not an umbrella project, run Dialyzer from the project root that contains `mix.exs`.

2. Make sure the project can compile.
   - If dependencies are missing, run `mix deps.get`.
   - If compilation fails, fix the compile error first. Dialyzer results are not useful until the project builds.

3. Run the analyzer:

       mix dialyzer

4. Fix the reported warnings.
   - Start with the first warning in the output.
   - Prefer fixing the real mismatch between the code and its types, specs, or control flow.
   - Do not silence a warning unless the repository already uses an ignore file or the task explicitly allows that approach.
   - Re-run `mix dialyzer` after each coherent batch of fixes.

5. Handle existing warnings explicitly.
   - If the command already reports unrelated warnings that were present before your change, record that fact before making more edits.
   - Do not claim success unless the selected command is clean or the user explicitly accepts the remaining baseline.

6. Widen the scope only if the work widened.
   - If you started in one child application and later changed shared or umbrella-level files, re-run `mix dialyzer` from the umbrella root.

7. Stop only when the chosen scope passes with exit code `0` and no unexpected Dialyzer warnings.

## Report back

State the directory you used, the exact command you ran, and whether the final run was clean.
