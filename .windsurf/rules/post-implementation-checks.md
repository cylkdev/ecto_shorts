---
trigger: always_on
---

After implementing a feature or making a change to the codebase, run these
steps in a loop until all steps pass. Do not skip any steps.

1. Run the project's standard quality checks (`/run-checks` workflow).
2. Run the code style review (`/code-style-review` workflow).
3. If either step reported issues and applied fixes, go back to step 1.
4. When both steps pass with no issues, report success and stop.
